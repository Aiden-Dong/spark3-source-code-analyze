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

package org.apache.spark.sql.execution.exchange

import java.util.function.Supplier

import scala.concurrent.Future

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriteProcessor}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.MutablePair
import org.apache.spark.util.collection.unsafe.sort.{PrefixComparators, RecordComparator}
import org.apache.spark.util.random.XORShiftRandom

/**
 * 为所有Shuffle Exchange实现引入一个共同的Trait，以便模式匹配。
 */
trait ShuffleExchangeLike extends Exchange {
  // 此shuffle的map数量。
  def numMappers: Int

  // shuffle 的分区数量.
  def numPartitions: Int

  // shuffle 来源
  def shuffleOrigin: ShuffleOrigin

  // 负责实现shuffle并执行准备工作，例如等待子查询的异步作业。
  final def submitShuffleJob: Future[MapOutputStatistics] = executeQuery {
    mapOutputStatisticsFuture
  }

  protected def mapOutputStatisticsFuture: Future[MapOutputStatistics]

  // 返回具有指定分区规格的shuffle RDD。
  def getShuffleRDD(partitionSpecs: Array[ShufflePartitionSpec]): RDD[_]

  // 返回shuffle执行完成后的运行时统计信息。
  def runtimeStatistics: Statistics
}

// 描述洗牌操作符的来源。
sealed trait ShuffleOrigin

//  表示洗牌操作符是由内部的 EnsureRequirements 规则添加的。
//  这意味着洗牌操作符用于确保内部数据分区的要求，Spark 可以自由地对其进行优化，只要仍然确保要求。
case object ENSURE_REQUIREMENTS extends ShuffleOrigin

// 表示洗牌操作符是由用户指定的重新分区操作符添加的。
// Spark 仍然可以通过更改洗牌分区数量来优化它，因为数据分区不会改变。
case object REPARTITION_BY_COL extends ShuffleOrigin

// 表示洗牌操作符是由用户指定的具有特定分区数量的重新分区操作符添加的。
// Spark 无法对其进行优化。
case object REPARTITION_BY_NUM extends ShuffleOrigin

// 表示洗牌操作符是由用户指定的rebalance操作符添加的。
// Spark 将尝试重新平衡分区，使每个分区的大小既不太小也不太大。
// 如果可能的话，将使用本地洗牌读取以减少网络流量。
case object REBALANCE_PARTITIONS_BY_NONE extends ShuffleOrigin

// 表示洗牌操作符是由用户指定的具有列的重新平衡操作符添加的。
// Spark 将尝试重新平衡分区，使每个分区的大小既不太小也不太大。
// 与 REBALANCE_PARTITIONS_BY_NONE 不同，这种情况下不能使用本地洗牌读取，因为输出需要按照给定的列进行分区。
case object REBALANCE_PARTITIONS_BY_COL extends ShuffleOrigin

/**
 * 执行一个洗牌操作，将会得到所需的分区。
 */
case class ShuffleExchangeExec(
    override val outputPartitioning: Partitioning,
    child: SparkPlan,
    shuffleOrigin: ShuffleOrigin = ENSURE_REQUIREMENTS)
  extends ShuffleExchangeLike {

  // 写指标
  private lazy val writeMetrics = SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  // 读指标
  private[sql] lazy val readMetrics = SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)

  // 输出指标
  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions")
  ) ++ readMetrics ++ writeMetrics

  override def nodeName: String = "Exchange"

  private lazy val serializer: Serializer = new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))

  @transient lazy val inputRDD: RDD[InternalRow] = child.execute()

  // 如果启用了自适应查询执行（AQE），则需要 `mapOutputStatisticsFuture`。
  @transient
  override lazy val mapOutputStatisticsFuture: Future[MapOutputStatistics] = {
    if (inputRDD.getNumPartitions == 0) {
      Future.successful(null)
    } else {
      sparkContext.submitMapStage(shuffleDependency)  // 提交任务执行
    }
  }

  // 获取上游RDD的分区数
  override def numMappers: Int = shuffleDependency.rdd.getNumPartitions
  // 获取下游分区数
  override def numPartitions: Int = shuffleDependency.partitioner.numPartitions

  // 返回目标ShuffleRDD
  override def getShuffleRDD(partitionSpecs: Array[ShufflePartitionSpec]): RDD[InternalRow] = {
    new ShuffledRowRDD(shuffleDependency, readMetrics, partitionSpecs)
  }

  // 目标指标数据
  override def runtimeStatistics: Statistics = {
    val dataSize = metrics("dataSize").value
    val rowCount = metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN).value
    Statistics(dataSize, Some(rowCount))
  }

  /**
   *一个[[ShuffleDependency]]，它将基于newPartitioning中定义的分区方案对其子节点的行进行分区。
   * 返回的ShuffleDependency的这些分区将成为洗牌的输入。
   */
  @transient
  lazy val shuffleDependency : ShuffleDependency[Int, InternalRow, InternalRow] = {
    val dep = ShuffleExchangeExec.prepareShuffleDependency(
      inputRDD,
      child.output,
      outputPartitioning,
      serializer,
      writeMetrics)
    metrics("numPartitions").set(dep.partitioner.numPartitions)
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(
      sparkContext, executionId, metrics("numPartitions") :: Nil)
    dep
  }

  /**
   * 缓存已创建的ShuffleRowRDD，以便我们可以重用它。
   */
  private var cachedShuffleRDD: ShuffledRowRDD = null

  protected override def doExecute(): RDD[InternalRow] = {
    // Returns the same ShuffleRowRDD if this plan is used by multiple plans.
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = new ShuffledRowRDD(shuffleDependency, readMetrics)
    }
    cachedShuffleRDD
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ShuffleExchangeExec =
    copy(child = newChild)
}

object ShuffleExchangeExec {

  /**
   * 确定在发送到洗牌之前是否必须对记录进行防御性复制。
   * Spark 的几个洗牌组件将在内存中缓冲反序列化的 Java 对象。
   * 洗牌代码假设对象是不可变的，因此不会执行自己的防御性复制。
   * 然而，在 Spark SQL 中，操作符的迭代器返回相同的可变的 Row 对象。
   * 为了正确地洗牌这些操作符的输出，我们需要在将记录发送到洗牌之前执行我们自己的复制。
   * 这种复制是昂贵的，所以我们尽量避免在可能的情况下进行。
   * 此方法封装了选择何时复制的逻辑。
   * 长远来看，我们可能希望将这种逻辑推入核心的洗牌 API 中，以便在 SQL 中不必依赖核心内部的知识。
   * 有关此问题的更多讨论，请参见 SPARK-2967、SPARK-4479 和 SPARK-7375。
   *
   * @param partitioner 洗牌的分区器
   * @return 如果在洗牌之前应该复制行，则为 true，否则为 false
   */
  private def needToCopyObjectsBeforeShuffle(partitioner: Partitioner): Boolean = {
    // Note: even though we only use the partitioner's `numPartitions` field, we require it to be
    // passed instead of directly passing the number of partitions in order to guard against
    // corner-cases where a partitioner constructed with `numPartitions` partitions may output
    // fewer partitions (like RangePartitioner, for example).
    val conf = SparkEnv.get.conf
    val shuffleManager = SparkEnv.get.shuffleManager
    val sortBasedShuffleOn = shuffleManager.isInstanceOf[SortShuffleManager]
    val bypassMergeThreshold = conf.get(config.SHUFFLE_SORT_BYPASS_MERGE_THRESHOLD)
    val numParts = partitioner.numPartitions
    if (sortBasedShuffleOn) {
      if (numParts <= bypassMergeThreshold) {
        // 如果我们使用的是原始的 SortShuffleManager，并且输出分区的数量足够小，则 Spark 将退回到基于哈希的洗牌写入路径，该路径不会缓冲反序列化的记录。
        // 请注意，如果我们修复了 SPARK-6026 并删除了此绕过，则必须删除此情况。
        false
      } else if (numParts <= SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE) {
        // SPARK-4550 和 SPARK-7081 扩展了基于排序的洗牌，将在对其进行排序之前对单个记录进行序列化。
        // 仅在洗牌依赖项未指定聚合器或排序，并且记录序列化器具有某些属性且分区数量未超过限制的情况下，才会应用此优化。
        // 如果启用了此优化，我们可以安全地避免复制。
        //
        // Exchange 永远不会为其 ShuffledRDD 配置聚合器或键排序，而 Spark SQL 中的序列化器始终满足这些属性，因此我们只需要检查分区数量是否超过限制。
        false
      } else {
        //Spark的SortShuffleManager使用ExternalSorter在内存中缓冲记录，因此我们必须复制。
        true
      }
    } else {
      // Catch-all case to safely handle any future ShuffleManager implementations.
      true
    }
  }

  /**
   * 返回一个 [[ShuffleDependency]]，它将根据 newPartitioning 中定义的分区方案对其子节点的行进行分区。
   * 返回的 ShuffleDependency 的这些分区将成为洗牌的输入。
   */
  def prepareShuffleDependency(
      rdd: RDD[InternalRow],               // 上游 RDD
      outputAttributes: Seq[Attribute],    // 输出属性
      newPartitioning: Partitioning,       // 分区计划
      serializer: Serializer,              // 序列化器
      writeMetrics: Map[String, SQLMetric])
    : ShuffleDependency[Int, InternalRow, InternalRow] = {

    // 获取分区实例
    val part: Partitioner = newPartitioning match {
      // 随机分区模式
      case RoundRobinPartitioning(numPartitions) => new HashPartitioner(numPartitions)

      case HashPartitioning(_, n) =>    // hash 分区方式
        new Partitioner {
          override def numPartitions: Int = n
          // 对于哈希分区，分区键已经是有效的分区 ID
          // 因为我们使用 HashPartitioning.partitionIdExpression 生成分区键。
          override def getPartition(key: Any): Int = key.asInstanceOf[Int]
        }

      case RangePartitioning(sortingExpressions, numPartitions) =>   // 范围分区
        // Extract only fields used for sorting to avoid collecting large fields that does not
        // affect sorting result when deciding partition bounds in RangePartitioner
        val rddForSampling = rdd.mapPartitionsInternal { iter =>
          val projection =
            UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
          val mutablePair = new MutablePair[InternalRow, Null]()
          // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
          // partition bounds. To get accurate samples, we need to copy the mutable keys.
          iter.map(row => mutablePair.update(projection(row).copy(), null))
        }
        // Construct ordering on extracted sort key.
        val orderingAttributes = sortingExpressions.zipWithIndex.map { case (ord, i) =>
          ord.copy(child = BoundReference(i, ord.dataType, ord.nullable))
        }
        implicit val ordering = new LazilyGeneratedOrdering(orderingAttributes)
        new RangePartitioner(
          numPartitions,
          rddForSampling,
          ascending = true,
          samplePointsPerPartitionHint = SQLConf.get.rangeExchangeSampleSizePerPartition)

      case SinglePartition =>
        new Partitioner {
          override def numPartitions: Int = 1
          override def getPartition(key: Any): Int = 0
        }
      case _ => throw new IllegalStateException(s"Exchange not implemented for $newPartitioning")
      // TODO: Handle BroadcastPartitioning.
    }

    // 获取分区key
    def getPartitionKeyExtractor(): InternalRow => Any = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) =>
        // Distributes elements evenly across output partitions, starting from a random partition.
        // nextInt(numPartitions) implementation has a special case when bound is a power of 2,
        // which is basically taking several highest bits from the initial seed, with only a
        // minimal scrambling. Due to deterministic seed, using the generator only once,
        // and lack of scrambling, the position values for power-of-two numPartitions always
        // end up being almost the same regardless of the index. substantially scrambling the
        // seed by hashing will help. Refer to SPARK-21782 for more details.
        val partitionId = TaskContext.get().partitionId()
        var position = new XORShiftRandom(partitionId).nextInt(numPartitions)
        (row: InternalRow) => {
          // The HashPartitioner will handle the `mod` by the number of partitions
          position += 1
          position
        }
      case h: HashPartitioning =>
        val projection = UnsafeProjection.create(h.partitionIdExpression :: Nil, outputAttributes)
        row => projection(row).getInt(0)
      case RangePartitioning(sortingExpressions, _) =>
        val projection = UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
        row => projection(row)
      case SinglePartition => identity
      case _ => throw new IllegalStateException(s"Exchange not implemented for $newPartitioning")
    }

    // 是否是随机分区
    val isRoundRobin = newPartitioning.isInstanceOf[RoundRobinPartitioning] &&
      newPartitioning.numPartitions > 1

    // 将RDD转为 tuple RDD
    val rddWithPartitionIds: RDD[Product2[Int, InternalRow]] = {
      // 数据进行循环分区 (RoundRobinPartitioning) 的重要性。
      // 为了避免数据丢失，Spark 需要确保生成的RoundRobinPartitioning方案是确定性的。
      // 换句话说，每次执行任务 (包括重试) 都应该将相同的数据分配到相同的分区。
      //
      // 注释还提到目前使用一种简单的方法，即在分区之前对数据进行本地排序。
      // 这样可以确保分区结果的确定性。
      // 不过，如果分区数目为 1，则不需要进行排序，因为所有数据都会分配到同一个分区。
      val newRdd = if (isRoundRobin && SQLConf.get.sortBeforeRepartition) {
        rdd.mapPartitionsInternal { iter =>
          val recordComparatorSupplier = new Supplier[RecordComparator] {
            override def get: RecordComparator = new RecordBinaryComparator()
          }
          // The comparator for comparing row hashcode, which should always be Integer.
          val prefixComparator = PrefixComparators.LONG

          // The prefix computer generates row hashcode as the prefix, so we may decrease the
          // probability that the prefixes are equal when input rows choose column values from a
          // limited range.
          val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
            private val result = new UnsafeExternalRowSorter.PrefixComputer.Prefix
            override def computePrefix(row: InternalRow):
            UnsafeExternalRowSorter.PrefixComputer.Prefix = {
              // The hashcode generated from the binary form of a [[UnsafeRow]] should not be null.
              result.isNull = false
              result.value = row.hashCode()
              result
            }
          }
          val pageSize = SparkEnv.get.memoryManager.pageSizeBytes

          val sorter = UnsafeExternalRowSorter.createWithRecordComparator(
            StructType.fromAttributes(outputAttributes),
            recordComparatorSupplier,
            prefixComparator,
            prefixComputer,
            pageSize,
            // We are comparing binary here, which does not support radix sort.
            // See more details in SPARK-28699.
            false)
          sorter.sort(iter.asInstanceOf[Iterator[UnsafeRow]])
        }
      } else {
        rdd
      }

      // round-robin function is order sensitive if we don't sort the input.
      val isOrderSensitive = isRoundRobin && !SQLConf.get.sortBeforeRepartition


      if (needToCopyObjectsBeforeShuffle(part)) {
        newRdd.mapPartitionsWithIndexInternal((_, iter) => {
          val getPartitionKey = getPartitionKeyExtractor()
          iter.map { row => (part.getPartition(getPartitionKey(row)), row.copy()) }
        }, isOrderSensitive = isOrderSensitive)
      } else {
        newRdd.mapPartitionsWithIndexInternal((_, iter) => {
          val getPartitionKey = getPartitionKeyExtractor()
          val mutablePair = new MutablePair[Int, InternalRow]()
          iter.map { row => mutablePair.update(part.getPartition(getPartitionKey(row)), row) }
        }, isOrderSensitive = isOrderSensitive)
      }
    }

    // 现在，我们手动创建一个 ShuffleDependency。
    // 因为 rddWithPartitionIds 中的键值对形式为 (partitionId, row)，
    // 并且每个 partitionId 都在预期的范围 [0, part.numPartitions - 1] 内。
    // 因此，该 Shuffle 的分区器是 PartitionIdPassthrough。
    val dependency =
      new ShuffleDependency[Int, InternalRow, InternalRow](
        rddWithPartitionIds,
        new PartitionIdPassthrough(part.numPartitions),
        serializer,
        shuffleWriterProcessor = createShuffleWriteProcessor(writeMetrics))

    dependency
  }

  /**
   * Create a customized [[ShuffleWriteProcessor]] for SQL which wrap the default metrics reporter
   * with [[SQLShuffleWriteMetricsReporter]] as new reporter for [[ShuffleWriteProcessor]].
   */
  def createShuffleWriteProcessor(metrics: Map[String, SQLMetric]): ShuffleWriteProcessor = {
    new ShuffleWriteProcessor {
      override protected def createMetricsReporter(
          context: TaskContext): ShuffleWriteMetricsReporter = {
        new SQLShuffleWriteMetricsReporter(context.taskMetrics().shuffleWriteMetrics, metrics)
      }
    }
  }
}
