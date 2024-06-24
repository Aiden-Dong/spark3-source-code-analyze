/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import org.apache.spark.{broadcast, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rdd.{RDD, RDDOperationScope}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.trees.{BinaryLike, LeafLike, TreeNodeTag, UnaryLike}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.NextIterator

object SparkPlan {
  /** 此 [[SparkPlan]] 转换自的原始 [[LogicalPlan]]。 */
  val LOGICAL_PLAN_TAG = TreeNodeTag[LogicalPlan]("logical_plan")

  /** 继承自其祖先的 [[LogicalPlan]]。 */
  val LOGICAL_PLAN_INHERITED_TAG = TreeNodeTag[LogicalPlan]("logical_plan_inherited")

  private val nextPlanId = new AtomicInteger(0)

  /** 注册一个新的 SparkPlan，返回其 SparkPlan ID */
  private[execution] def newPlanId(): Int = nextPlanId.getAndIncrement()
}

/**
 * 物理操作符的基类。
 * 命名约定是物理操作符以 "Exec" 后缀结尾，例如 [[ProjectExec]]。
 */
abstract class SparkPlan extends QueryPlan[SparkPlan] with Logging with Serializable {
  @transient final val session = SparkSession.getActiveSession.orNull

  protected def sparkContext = session.sparkContext

  override def conf: SQLConf = {
    if (session != null) {
      session.sessionState.conf
    } else {
      super.conf
    }
  }

  val id: Int = SparkPlan.newPlanId()

  /**
   * 如果计划的这个阶段支持基于行的执行，则返回 true。
   * 一个计划也可以支持基于列的执行（参见 supportsColumnar）。Spark 在查询规划期间将决定调用哪种执行方式。
   */
  def supportsRowBased: Boolean = !supportsColumnar

  /**
   * 如果计划的这个阶段支持基于列的执行，则返回 true。
   * 一个计划也可以支持基于行的执行（参见 supportsRowBased）。
   * Spark 在查询规划期间将决定调用哪种执行方式。
   */
  def supportsColumnar: Boolean = false

  /**
   * 在列处理模式下输出的列的确切 Java 类型。
   * 这是一个用于代码生成的性能优化，并且是可选的。
   */
  def vectorTypes: Option[Seq[String]] = None

  /** 重写的复制方法还将 SQL 上下文传播到复制的计划。 */
  override def makeCopy(newArgs: Array[AnyRef]): SparkPlan = {
    if (session != null) {
      session.withActive(super.makeCopy(newArgs))
    } else {
      super.makeCopy(newArgs)
    }
  }

  /**
   * @return 连接到此计划的逻辑计划。
   */
  def logicalLink: Option[LogicalPlan] =
    getTagValue(SparkPlan.LOGICAL_PLAN_TAG)
      .orElse(getTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG))

  /**
   * 如果未设置，则递归设置逻辑计划链接。
   */
  def setLogicalLink(logicalPlan: LogicalPlan): Unit = {
    setLogicalLink(logicalPlan, false)
  }

  private def setLogicalLink(logicalPlan: LogicalPlan, inherited: Boolean = false): Unit = {
    // Stop at a descendant which is the root of a sub-tree transformed from another logical node.
    if (inherited && getTagValue(SparkPlan.LOGICAL_PLAN_TAG).isDefined) {
      return
    }

    val tag = if (inherited) {
      SparkPlan.LOGICAL_PLAN_INHERITED_TAG
    } else {
      SparkPlan.LOGICAL_PLAN_TAG
    }
    setTagValue(tag, logicalPlan)
    children.foreach(_.setLogicalLink(logicalPlan, true))
  }

  /**
   * @return 包含此 SparkPlan 的指标的所有指标。
   */
  def metrics: Map[String, SQLMetric] = Map.empty

  /**
   * 重置所有指标。
   */
  def resetMetrics(): Unit = {
    metrics.valuesIterator.foreach(_.reset())
    children.foreach(_.resetMetrics())
  }

  /**
   * @return [[SQLMetric]] for the `name`.
   */
  def longMetric(name: String): SQLMetric = metrics(name)

  // TODO: Move to `DistributedPlan`
  /**
   * 指定数据在集群中不同节点之间的分区方式。
   * 请注意，如果在应用 EnsureRequirements 之前调用此方法可能会失败，
   * 因为 PartitioningCollection 要求所有的分区都具有相同数量的分区。
   */
  def outputPartitioning: Partitioning = UnknownPartitioning(0) // TODO: WRONG WIDTH!

  /**
   * 指定此运算符的所有子项的数据分发要求。
   * 默认情况下，每个子项的分发要求均为 [[UnspecifiedDistribution]]，这意味着每个子项都可以具有任何分发。
   *
   * 如果运算符重写了此方法，并为多个子项指定了分发要求（不包括 [[UnspecifiedDistribution]] 和 [[BroadcastDistribution]]），
   * 则 Spark 保证这些子项的输出将具有相同数量的分区，以便运算符可以安全地对这些子项的结果 RDD 的分区进行压缩。
   *
   * 一些运算符可以利用此保证来满足一些有趣的需求，
   * 例如，非广播连接可以为其左子项指定 HashClusteredDistribution(a,b)，
   * 并为其右子项指定 HashClusteredDistribution(c,d)，那么可以保证左右子项由 a、b/c、d 共同分区，
   * 这意味着相同值的元组位于相同索引的分区中，例如，(a=1,b=2) 和 (c=1,d=2) 都位于左右子项的第二个分区中。
   */
  def requiredChildDistribution: Seq[Distribution] =
    Seq.fill(children.size)(UnspecifiedDistribution)

  /** 指定每个分区中的数据排序方式. */
  def outputOrdering: Seq[SortOrder] = Nil

  /** 为此运算符的输入数据指定每个分区的排序要求. */
  def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq.fill(children.size)(Nil)

  /**
   * 在准备工作完成后，通过委托给doExecute返回此查询的结果作为一个RDD[InternalRow]。
   * SparkPlan的具体实现应该重写doExecute。
   */
  final def execute(): RDD[InternalRow] = executeQuery {
    if (isCanonicalizedPlan) {
      throw new IllegalStateException("A canonicalized plan is not supposed to be executed.")
    }
    doExecute()
  }

  /**
   * 在准备工作完成后，通过委托给doExecuteBroadcast返回此查询的结果作为广播变量。
   *
   * SparkPlan的具体实现应该重写doExecuteBroadcast。
   */
  final def executeBroadcast[T](): broadcast.Broadcast[T] = executeQuery {
    if (isCanonicalizedPlan) {
      throw new IllegalStateException("A canonicalized plan is not supposed to be executed.")
    }
    doExecuteBroadcast()
  }

  /**
   * 在准备工作完成后，通过委托给doColumnarExecute返回此查询的结果作为一个RDD[ColumnarBatch]。
   * 如果supportsColumnar返回true，则SparkPlan的具体实现应该重写doColumnarExecute。
   */
  final def executeColumnar(): RDD[ColumnarBatch] = executeQuery {
    if (isCanonicalizedPlan) {
      throw new IllegalStateException("A canonicalized plan is not supposed to be executed.")
    }
    doExecuteColumnar()
  }

  /**
   * 在准备查询并为可视化创建的RDD添加查询计划信息后，执行查询。.
   */
  protected final def executeQuery[T](query: => T): T = {
    RDDOperationScope.withScope(sparkContext, nodeName, false, true) {
      prepare()
      waitForSubqueries()
      query
    }
  }

  /**
   * 此计划节点的未相关标量子查询列表，其中包含保存子查询结果的 future。
   * 此列表由 prepareSubqueries 填充，在 prepare 中调用。
   */
  @transient
  private val runningSubqueries = new ArrayBuffer[ExecSubqueryExpression]

  /**
   * 查找此计划节点中的标量子查询表达式，并开始评估它们。
   */
  protected def prepareSubqueries(): Unit = {
    expressions.foreach {
      _.collect {
        case e: ExecSubqueryExpression =>
          e.plan.prepare()
          runningSubqueries += e
      }
    }
  }

  /**
   * 阻塞线程直到所有子查询完成评估并更新结果。
   */
  protected def waitForSubqueries(): Unit = synchronized {
    // fill in the result of subqueries
    runningSubqueries.foreach { sub =>
      sub.updateResult()
    }
    runningSubqueries.clear()
  }

  /**
   * 是否调用了 "prepare" 方法。
   */
  private var prepared = false

  /**
   * 为执行准备此 SparkPlan。它是幂等的。.
   */
  final def prepare(): Unit = {
    // doPrepare() may depend on it's children, we should call prepare() on all the children first.
    children.foreach(_.prepare())
    synchronized {
      if (!prepared) {
        prepareSubqueries()
        doPrepare()
        prepared = true
      }
    }
  }

  /**
   * 由 SparkPlan 的具体实现覆盖。确保在 SparkPlan 的任何execute之前运行。
   * 如果我们想在执行查询之前设置一些状态，这将非常有用，例如，BroadcastHashJoin 使用它来异步广播。
   * 注意，prepare方法已经遍历了整个树，因此实现不必调用子节点的prepare方法。
   * 这个方法只会被调用一次，并受到this的保护。
   */
  protected def doPrepare(): Unit = {}

  /**
   * 由 SparkPlan 的具体实现覆盖。
   */
  protected def doExecute(): RDD[InternalRow]

  /**
   * 由 SparkPlan 的具体实现覆盖。
   */
  protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    throw QueryExecutionErrors.doExecuteBroadcastNotImplementedError(nodeName)
  }

  /**
   * 由支持Columnar的SparkPlan的具体实现覆盖。
   * 按照约定，创建ColumnarBatch的执行器负责在不再需要时关闭它。
   * 这允许输入格式在需要时能够重用批次。
   */
  protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(s"Internal Error ${this.getClass} has column support" +
      s" mismatch:\n${this}")
  }

  /**
   * 将该计划的输出转换为基于行的形式，如果它是列式计划的话。
   */
  def toRowBased: SparkPlan = if (supportsColumnar) ColumnarToRowExec(this) else this

  /**
   * 将 UnsafeRow 打包成字节数组以加快序列化速度。
   * 字节数组的格式如下：
   * [size] [UnsafeRow 的字节] [size] [UnsafeRow 的字节] ... [-1]
   * UnsafeRow 是高度可压缩的（对于任何列至少为 8 个字节），字节数组也被压缩了。
   */
  private def getByteArrayRdd(n: Int = -1,
                              takeFromEnd: Boolean = false): RDD[(Long, Array[Byte])] = {
    execute().mapPartitionsInternal { iter =>
      var count = 0
      val buffer = new Array[Byte](4 << 10)  // 4K
      val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      val bos = new ByteArrayOutputStream()
      val out = new DataOutputStream(codec.compressedOutputStream(bos))

      if (takeFromEnd && n > 0) {
        // To collect n from the last, we should anyway read everything with keeping the n.
        // Otherwise, we don't know where is the last from the iterator.
        var last: Seq[UnsafeRow] = Seq.empty[UnsafeRow]
        val slidingIter = iter.map(_.copy()).sliding(n)
        while (slidingIter.hasNext) { last = slidingIter.next().asInstanceOf[Seq[UnsafeRow]] }
        var i = 0
        count = last.length
        while (i < count) {
          val row = last(i)
          out.writeInt(row.getSizeInBytes)
          row.writeToStream(out, buffer)
          i += 1
        }
      } else {
        // `iter.hasNext` may produce one row and buffer it, we should only call it when the
        // limit is not hit.
        while ((n < 0 || count < n) && iter.hasNext) {
          val row = iter.next().asInstanceOf[UnsafeRow]
          out.writeInt(row.getSizeInBytes)
          row.writeToStream(out, buffer)
          count += 1
        }
      }
      out.writeInt(-1)
      out.flush()
      out.close()
      Iterator((count, bos.toByteArray))
    }
  }

  /**
   * 解码字节数组回到 UnsafeRows 并将它们放入缓冲区中。
   */
  private def decodeUnsafeRows(bytes: Array[Byte]): Iterator[InternalRow] = {
    val nFields = schema.length

    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val bis = new ByteArrayInputStream(bytes)
    val ins = new DataInputStream(codec.compressedInputStream(bis))

    new NextIterator[InternalRow] {
      private var sizeOfNextRow = ins.readInt()
      private def _next(): InternalRow = {
        val bs = new Array[Byte](sizeOfNextRow)
        ins.readFully(bs)
        val row = new UnsafeRow(nFields)
        row.pointTo(bs, sizeOfNextRow)
        sizeOfNextRow = ins.readInt()
        row
      }

      override def getNext(): InternalRow = {
        if (sizeOfNextRow >= 0) {
          try {
            _next()
          } catch {
            case t: Throwable if ins != null =>
              ins.close()
              throw t
          }
        } else {
          finished = true
          null
        }
      }
      override def close(): Unit = ins.close()
    }
  }

  /**
   * Runs this query returning the result as an array.
   */
  def executeCollect(): Array[InternalRow] = {
    val byteArrayRdd = getByteArrayRdd()

    val results = ArrayBuffer[InternalRow]()
    byteArrayRdd.collect().foreach { countAndBytes =>
      decodeUnsafeRows(countAndBytes._2).foreach(results.+=)
    }
    results.toArray
  }

  private[spark] def executeCollectIterator(): (Long, Iterator[InternalRow]) = {
    val countsAndBytes = getByteArrayRdd().collect()
    val total = countsAndBytes.map(_._1).sum
    val rows = countsAndBytes.iterator.flatMap(countAndBytes => decodeUnsafeRows(countAndBytes._2))
    (total, rows)
  }

  /**
   * Runs this query returning the result as an iterator of InternalRow.
   *
   * @note Triggers multiple jobs (one for each partition).
   */
  def executeToIterator(): Iterator[InternalRow] = {
    getByteArrayRdd().map(_._2).toLocalIterator.flatMap(decodeUnsafeRows)
  }

  /**
   * Runs this query returning the result as an array, using external Row format.
   */
  def executeCollectPublic(): Array[Row] = {
    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    executeCollect().map(converter(_).asInstanceOf[Row])
  }

  /**
   * Runs this query returning the first `n` rows as an array.
   *
   * This is modeled after `RDD.take` but never runs any job locally on the driver.
   */
  def executeTake(n: Int): Array[InternalRow] = executeTake(n, takeFromEnd = false)

  /**
   * Runs this query returning the last `n` rows as an array.
   *
   * This is modeled after `RDD.take` but never runs any job locally on the driver.
   */
  def executeTail(n: Int): Array[InternalRow] = executeTake(n, takeFromEnd = true)

  private def executeTake(n: Int, takeFromEnd: Boolean): Array[InternalRow] = {
    if (n == 0) {
      return new Array[InternalRow](0)
    }

    val childRDD = getByteArrayRdd(n, takeFromEnd)

    val buf = if (takeFromEnd) new ListBuffer[InternalRow] else new ArrayBuffer[InternalRow]
    val totalParts = childRDD.partitions.length
    var partsScanned = 0
    while (buf.length < n && partsScanned < totalParts) {
      // The number of partitions to try in this iteration. It is ok for this number to be
      // greater than totalParts because we actually cap it at totalParts in runJob.
      var numPartsToTry = 1L
      if (partsScanned > 0) {
        // If we didn't find any rows after the previous iteration, quadruple and retry.
        // Otherwise, interpolate the number of partitions we need to try, but overestimate
        // it by 50%. We also cap the estimation in the end.
        val limitScaleUpFactor = Math.max(conf.limitScaleUpFactor, 2)
        if (buf.isEmpty) {
          numPartsToTry = partsScanned * limitScaleUpFactor
        } else {
          val left = n - buf.length
          // As left > 0, numPartsToTry is always >= 1
          numPartsToTry = Math.ceil(1.5 * left * partsScanned / buf.length).toInt
          numPartsToTry = Math.min(numPartsToTry, partsScanned * limitScaleUpFactor)
        }
      }

      val parts = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts).toInt)
      val partsToScan = if (takeFromEnd) {
        // Reverse partitions to scan. So, if parts was [1, 2, 3] in 200 partitions (0 to 199),
        // it becomes [198, 197, 196].
        parts.map(p => (totalParts - 1) - p)
      } else {
        parts
      }
      val sc = sparkContext
      val res = sc.runJob(childRDD, (it: Iterator[(Long, Array[Byte])]) =>
        if (it.hasNext) it.next() else (0L, Array.emptyByteArray), partsToScan)

      var i = 0

      if (takeFromEnd) {
        while (buf.length < n && i < res.length) {
          val rows = decodeUnsafeRows(res(i)._2)
          if (n - buf.length >= res(i)._1) {
            buf.prepend(rows.toArray[InternalRow]: _*)
          } else {
            val dropUntil = res(i)._1 - (n - buf.length)
            // Same as Iterator.drop but this only takes a long.
            var j: Long = 0L
            while (j < dropUntil) { rows.next(); j += 1L}
            buf.prepend(rows.toArray[InternalRow]: _*)
          }
          i += 1
        }
      } else {
        while (buf.length < n && i < res.length) {
          val rows = decodeUnsafeRows(res(i)._2)
          if (n - buf.length >= res(i)._1) {
            buf ++= rows.toArray[InternalRow]
          } else {
            buf ++= rows.take(n - buf.length).toArray[InternalRow]
          }
          i += 1
        }
      }
      partsScanned += partsToScan.size
    }
    buf.toArray
  }

  /**
   * Cleans up the resources used by the physical operator (if any). In general, all the resources
   * should be cleaned up when the task finishes but operators like SortMergeJoinExec and LimitExec
   * may want eager cleanup to free up tight resources (e.g., memory).
   */
  protected[sql] def cleanupResources(): Unit = {
    children.foreach(_.cleanupResources())
  }
}

trait LeafExecNode extends SparkPlan with LeafLike[SparkPlan] {

  override def producedAttributes: AttributeSet = outputSet
  override def verboseStringWithOperatorId(): String = {
    val argumentString = argString(conf.maxToStringFields)
    val outputStr = s"${ExplainUtils.generateFieldString("Output", output)}"

    if (argumentString.nonEmpty) {
      s"""
         |$formattedNodeName
         |$outputStr
         |Arguments: $argumentString
         |""".stripMargin
    } else {
      s"""
         |$formattedNodeName
         |$outputStr
         |""".stripMargin
    }
  }
}

object UnaryExecNode {
  def unapply(a: Any): Option[(SparkPlan, SparkPlan)] = a match {
    case s: SparkPlan if s.children.size == 1 => Some((s, s.children.head))
    case _ => None
  }
}

trait UnaryExecNode extends SparkPlan with UnaryLike[SparkPlan] {

  override def verboseStringWithOperatorId(): String = {
    val argumentString = argString(conf.maxToStringFields)
    val inputStr = s"${ExplainUtils.generateFieldString("Input", child.output)}"

    if (argumentString.nonEmpty) {
      s"""
         |$formattedNodeName
         |$inputStr
         |Arguments: $argumentString
         |""".stripMargin
    } else {
      s"""
         |$formattedNodeName
         |$inputStr
         |""".stripMargin
    }
  }
}

trait BinaryExecNode extends SparkPlan with BinaryLike[SparkPlan] {

  override def verboseStringWithOperatorId(): String = {
    val argumentString = argString(conf.maxToStringFields)
    val leftOutputStr = s"${ExplainUtils.generateFieldString("Left output", left.output)}"
    val rightOutputStr = s"${ExplainUtils.generateFieldString("Right output", right.output)}"

    if (argumentString.nonEmpty) {
      s"""
         |$formattedNodeName
         |$leftOutputStr
         |$rightOutputStr
         |Arguments: $argumentString
         |""".stripMargin
    } else {
      s"""
         |$formattedNodeName
         |$leftOutputStr
         |$rightOutputStr
         |""".stripMargin
    }
  }
}
