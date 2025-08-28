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

package org.apache.spark.sql.execution.adaptive

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning, RoundRobinPartitioning, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * 根据运行时统计信息，动态调整shuffle读取的分区安排。
 *
 * @param child           通常是 `ShuffleQueryStageExec`，但在规范化期间可以是shuffle exchange节点
 * @param partitionSpecs  定义分区安排的分区规范，至少需要一个分区
 */
case class AQEShuffleReadExec private(
    child: SparkPlan,
    partitionSpecs: Seq[ShufflePartitionSpec]) extends UnaryExecNode {
  assert(partitionSpecs.nonEmpty, s"${getClass.getSimpleName} requires at least one partition")

  // If this is to read shuffle files locally, then all partition specs should be
  // `PartialMapperPartitionSpec`.
  if (partitionSpecs.exists(_.isInstanceOf[PartialMapperPartitionSpec])) {
    assert(partitionSpecs.forall(_.isInstanceOf[PartialMapperPartitionSpec]))
  }

  override def supportsColumnar: Boolean = child.supportsColumnar

  override def output: Seq[Attribute] = child.output

  override lazy val outputPartitioning: Partitioning = {

    if (partitionSpecs.forall(_.isInstanceOf[PartialMapperPartitionSpec]) &&
        partitionSpecs.map(_.asInstanceOf[PartialMapperPartitionSpec].mapIndex).toSet.size ==
          partitionSpecs.length) {

      // 情况1：本地shuffle读取，每个任务一个mapper
      child match {
        case ShuffleQueryStageExec(_, s: ShuffleExchangeLike, _) =>
          s.child.outputPartitioning
        case ShuffleQueryStageExec(_, r @ ReusedExchangeExec(_, s: ShuffleExchangeLike), _) =>
          s.child.outputPartitioning match {
            case e: Expression => r.updateAttr(e).asInstanceOf[Partitioning]
            case other => other
          }
        case _ =>
          throw new IllegalStateException("operating on canonicalization plan")
      }
    } else if (isCoalescedRead) {
      //  情况2：合并读取

      child.outputPartitioning match {
        // 保持Hash分区语义，只改变分区数量
        case h: HashPartitioning =>
          CurrentOrigin.withOrigin(h.origin)(h.copy(numPartitions = partitionSpecs.length))

        // 保持Range分区语义，只改变分区数量
        case r: RangePartitioning =>
          CurrentOrigin.withOrigin(r.origin)(r.copy(numPartitions = partitionSpecs.length))
        // This can only happen for `REBALANCE_PARTITIONS_BY_NONE`, which uses
        // `RoundRobinPartitioning` but we don't need to retain the number of partitions.
        case r: RoundRobinPartitioning =>
          r.copy(numPartitions = partitionSpecs.length)
        case other @ SinglePartition =>
          throw new IllegalStateException(
            "Unexpected partitioning for coalesced shuffle read: " + other)
        case _ =>
          // Spark plugins may have custom partitioning and may replace this operator
          // during the postStageOptimization phase, so return UnknownPartitioning here
          // rather than throw an exception
          UnknownPartitioning(partitionSpecs.length)
      }
    } else {
      UnknownPartitioning(partitionSpecs.length)
    }
  }

  override def stringArgs: Iterator[Any] = {
    val desc = if (isLocalRead) {
      "local"
    } else if (hasCoalescedPartition && hasSkewedPartition) {
      "coalesced and skewed"
    } else if (hasCoalescedPartition) {
      "coalesced"
    } else if (hasSkewedPartition) {
      "skewed"
    } else {
      ""
    }
    Iterator(desc)
  }

  /////   是不是需要分区合并
  private def isCoalescedSpec(spec: ShufflePartitionSpec) = spec match {
    case CoalescedPartitionSpec(0, 0, _) => true
    case s: CoalescedPartitionSpec => s.endReducerIndex - s.startReducerIndex > 1
    case _ => false
  }

  def hasCoalescedPartition: Boolean = {
    partitionSpecs.exists(isCoalescedSpec)
  }

  //  是否是存在 倾斜分区处理
  def hasSkewedPartition: Boolean =
    partitionSpecs.exists(_.isInstanceOf[PartialReducerPartitionSpec])

  // 是否是 本地读取优化
  def isLocalRead: Boolean =
    partitionSpecs.exists(_.isInstanceOf[PartialMapperPartitionSpec]) ||
      partitionSpecs.exists(_.isInstanceOf[CoalescedMapperPartitionSpec])

  def isCoalescedRead: Boolean = {
    partitionSpecs.sliding(2).forall {
      // A single partition spec which is `CoalescedPartitionSpec` also means coalesced read.
      case Seq(_: CoalescedPartitionSpec) => true
      case Seq(l: CoalescedPartitionSpec, r: CoalescedPartitionSpec) =>
        l.endReducerIndex <= r.startReducerIndex
      case _ => false
    }
  }

  private def shuffleStage = child match {
    case stage: ShuffleQueryStageExec => Some(stage)
    case _ => None
  }

  @transient private lazy val partitionDataSizes: Option[Seq[Long]] = {
    if (!isLocalRead && shuffleStage.get.mapStats.isDefined) {
      Some(partitionSpecs.map {
        case p: CoalescedPartitionSpec =>
          assert(p.dataSize.isDefined)
          p.dataSize.get
        case p: PartialReducerPartitionSpec => p.dataSize
        case p => throw new IllegalStateException(s"unexpected $p")
      })
    } else {
      None
    }
  }

  private def sendDriverMetrics(): Unit = {
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val driverAccumUpdates = ArrayBuffer.empty[(Long, Long)]

    val numPartitionsMetric = metrics("numPartitions")
    numPartitionsMetric.set(partitionSpecs.length)
    driverAccumUpdates += (numPartitionsMetric.id -> partitionSpecs.length.toLong)

    if (hasSkewedPartition) {
      val skewedSpecs = partitionSpecs.collect {
        case p: PartialReducerPartitionSpec => p
      }

      val skewedPartitions = metrics("numSkewedPartitions")
      val skewedSplits = metrics("numSkewedSplits")

      val numSkewedPartitions = skewedSpecs.map(_.reducerIndex).distinct.length
      val numSplits = skewedSpecs.length

      skewedPartitions.set(numSkewedPartitions)
      driverAccumUpdates += (skewedPartitions.id -> numSkewedPartitions)

      skewedSplits.set(numSplits)
      driverAccumUpdates += (skewedSplits.id -> numSplits)
    }

    if (hasCoalescedPartition) {
      val numCoalescedPartitionsMetric = metrics("numCoalescedPartitions")
      val x = partitionSpecs.count(isCoalescedSpec)
      numCoalescedPartitionsMetric.set(x)
      driverAccumUpdates += numCoalescedPartitionsMetric.id -> x
    }

    partitionDataSizes.foreach { dataSizes =>
      val partitionDataSizeMetrics = metrics("partitionDataSize")
      driverAccumUpdates ++= dataSizes.map(partitionDataSizeMetrics.id -> _)
      // Set sum value to "partitionDataSize" metric.
      partitionDataSizeMetrics.set(dataSizes.sum)
    }

    // 将**Driver端计算的指标**发送到Spark的事件总线，用于UI显示和监控。
    SQLMetrics.postDriverMetricsUpdatedByValue(sparkContext, executionId, driverAccumUpdates.toSeq)
  }

  @transient override lazy val metrics: Map[String, SQLMetric] = {
    if (shuffleStage.isDefined) {
      Map("numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions")) ++ {
        if (isLocalRead) {
          // We split the mapper partition evenly when creating local shuffle read, so no
          // data size info is available.
          Map.empty
        } else {
          Map("partitionDataSize" ->
            SQLMetrics.createSizeMetric(sparkContext, "partition data size"))
        }
      } ++ {
        if (hasSkewedPartition) {
          Map("numSkewedPartitions" ->
            SQLMetrics.createMetric(sparkContext, "number of skewed partitions"),
            "numSkewedSplits" ->
              SQLMetrics.createMetric(sparkContext, "number of skewed partition splits"))
        } else {
          Map.empty
        }
      } ++ {
        if (hasCoalescedPartition) {
          Map("numCoalescedPartitions" ->
            SQLMetrics.createMetric(sparkContext, "number of coalesced partitions"))
        } else {
          Map.empty
        }
      }
    } else {
      // It's a canonicalized plan, no need to report metrics.
      Map.empty
    }
  }

  private lazy val shuffleRDD: RDD[_] = {
    shuffleStage match {
      case Some(stage) =>
        sendDriverMetrics()
        stage.shuffle.getShuffleRDD(partitionSpecs.toArray)
      case _ =>
        throw new IllegalStateException("operating on canonicalized plan")
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    shuffleRDD.asInstanceOf[RDD[InternalRow]]
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    shuffleRDD.asInstanceOf[RDD[ColumnarBatch]]
  }

  override protected def withNewChildInternal(newChild: SparkPlan): AQEShuffleReadExec =
    copy(child = newChild)
}
