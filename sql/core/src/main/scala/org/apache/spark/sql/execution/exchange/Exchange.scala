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

import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * 多线程或多进程之间交换数据的操作符的基类。
 * 交换是实现并行性的关键类。
 * 虽然实现方式有很大不同，但概念类似于 Goetz Graefe 在
 * 《Volcano -- An Extensible and Parallel Query Evaluation System》中描述的交换操作符。
 */
abstract class Exchange extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output
  final override val nodePatterns: Seq[TreePattern] = Seq(EXCHANGE)

  override def stringArgs: Iterator[Any] = super.stringArgs ++ Iterator(s"[plan_id=$id]")
}

/**
 * 一个包装复用交换的封装器，以拥有不同的输出，
 * 因为产生逻辑上相同输出的两个交换将具有不同的输出属性 ID 集，
 * 所以我们需要保留原始 ID，因为下游操作符期望这些 ID。
 */
case class ReusedExchangeExec(override val output: Seq[Attribute], child: Exchange)
  extends LeafExecNode {

  override def supportsColumnar: Boolean = child.supportsColumnar

  // Ignore this wrapper for canonicalizing.
  override def doCanonicalize(): SparkPlan = child.canonicalized

  def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar()
  }

  override protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.executeBroadcast()
  }

  // `ReusedExchangeExec` can have distinct set of output attribute ids from its child, we need
  // to update the attribute ids in `outputPartitioning` and `outputOrdering`.
  private[sql] lazy val updateAttr: Expression => Expression = {
    val originalAttrToNewAttr = AttributeMap(child.output.zip(output))
    e => e.transform {
      case attr: Attribute => originalAttrToNewAttr.getOrElse(attr, attr)
    }
  }

  override def outputPartitioning: Partitioning = child.outputPartitioning match {
    case e: Expression => updateAttr(e).asInstanceOf[Partitioning]
    case other => other
  }

  override def outputOrdering: Seq[SortOrder] = {
    child.outputOrdering.map(updateAttr(_).asInstanceOf[SortOrder])
  }

  override def verboseStringWithOperatorId(): String = {
    val reuse_op_str = ExplainUtils.getOpId(child)
    s"""
       |$formattedNodeName [Reuses operator id: $reuse_op_str]
       |${ExplainUtils.generateFieldString("Output", output)}
       |""".stripMargin
  }
}
