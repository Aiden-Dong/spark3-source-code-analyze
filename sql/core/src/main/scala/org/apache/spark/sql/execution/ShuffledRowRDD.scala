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

import java.util.Arrays

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLShuffleReadMetricsReporter}
import org.apache.spark.sql.internal.SQLConf

// 统一抽象不同类型的 Shuffle 分区读取策略
// 支持模式匹配， 确保类型安全
// 为 AQE 提供灵活的分区重组能力
//
// Shuffle 数据可以看作一个二维矩阵：
//         Reducer 0  Reducer 1  Reducer 2  Reducer 3
// Map 0   [  data  ] [  data  ] [  data  ] [  data  ]
// Map 1   [  data  ] [  data  ] [  data  ] [  data  ]
// Map 2   [  data  ] [  data  ] [  data  ] [  data  ]
// Map 3   [  data  ] [  data  ] [  data  ] [  data  ]
sealed trait ShufflePartitionSpec

// 用于合并 reduce 分区的能力， 用来读取从 [startReducerIndex, endReducerIndex) 的分区数据
// 这里的下游分区数
case class CoalescedPartitionSpec(
    startReducerIndex: Int,        // 开始的 Reducer 索引
    endReducerIndex: Int,          // 结束的 Reducer 索引
    @transient dataSize: Option[Long] = None) extends ShufflePartitionSpec

object CoalescedPartitionSpec {
  def apply(startReducerIndex: Int,
            endReducerIndex: Int,
            dataSize: Long): CoalescedPartitionSpec = {
    CoalescedPartitionSpec(startReducerIndex, endReducerIndex, Some(dataSize))
  }
}

// 部分 Reducer 分区规格
// 用途：读取单个 Reducer 分区的部分数据（来自指定范围的 Map 任务）
// 典型场景：倾斜优化中分割大分区
// • PartialReducerPartitionSpec(1, 0, 2, 200MB) 读取分区1中来自Map[0,2)的数据
// • PartialReducerPartitionSpec(1, 2, 4, 200MB) 读取分区1中来自Map[2,4)的数据
case class PartialReducerPartitionSpec(
    reducerIndex: Int,       // 目标  Reducer 分区索引
    startMapIndex: Int,      // 开始的 Map  索引
    endMapIndex: Int,        // 结束的 Map 索引
    @transient dataSize: Long) extends ShufflePartitionSpec

// A partition that reads partial data of one mapper, from `startReducerIndex` (inclusive) to
// `endReducerIndex` (exclusive).

// 读取单个 Map 任务的部分输出（发送到指定范围的 Reducer）
// 典型场景：本地读取优化，避免网络传输
case class PartialMapperPartitionSpec(
    mapIndex: Int,           // 目标 map 索引
    startReducerIndex: Int,  // 开始的 reduce 索引位置
    endReducerIndex: Int     // 结束的 reduce 索引位置
) extends ShufflePartitionSpec

// TODO(SPARK-36234): Consider mapper location and shuffle block size when coalescing mappers
//  合并 Mapper 分区规格
// 读取多个 Map 任务的完整输出
// 典型场景：Map 端合并优化
case class CoalescedMapperPartitionSpec(
    startMapIndex: Int,
    endMapIndex: Int,
    numReducers: Int) extends ShufflePartitionSpec

/**
 * The [[Partition]] used by [[ShuffledRowRDD]].
 */
private final case class ShuffledRowRDDPartition(
  index: Int, spec: ShufflePartitionSpec) extends Partition

/**
 * A dummy partitioner for use with records whose partition ids have been pre-computed (i.e. for
 * use on RDDs of (Int, Row) pairs where the Int is a partition id in the expected range).
 */
private class PartitionIdPassthrough(override val numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}

/**
 * A Partitioner that might group together one or more partitions from the parent.
 *
 * @param parent a parent partitioner
 * @param partitionStartIndices indices of partitions in parent that should create new partitions
 *   in child (this should be an array of increasing partition IDs). For example, if we have a
 *   parent with 5 partitions, and partitionStartIndices is [0, 2, 4], we get three output
 *   partitions, corresponding to partition ranges [0, 1], [2, 3] and [4] of the parent partitioner.
 */
class CoalescedPartitioner(val parent: Partitioner, val partitionStartIndices: Array[Int])
  extends Partitioner {

  @transient private lazy val parentPartitionMapping: Array[Int] = {
    val n = parent.numPartitions
    val result = new Array[Int](n)
    for (i <- 0 until partitionStartIndices.length) {
      val start = partitionStartIndices(i)
      val end = if (i < partitionStartIndices.length - 1) partitionStartIndices(i + 1) else n
      for (j <- start until end) {
        result(j) = i
      }
    }
    result
  }

  override def numPartitions: Int = partitionStartIndices.length

  override def getPartition(key: Any): Int = {
    parentPartitionMapping(parent.getPartition(key))
  }

  override def equals(other: Any): Boolean = other match {
    case c: CoalescedPartitioner =>
      c.parent == parent && Arrays.equals(c.partitionStartIndices, partitionStartIndices)
    case _ =>
      false
  }

  override def hashCode(): Int = 31 * parent.hashCode() + Arrays.hashCode(partitionStartIndices)
}

/**
 * 这是 [[org.apache.spark.rdd.ShuffledRDD]] 的特化版本，专门针对行数据的 shuffle 进行了优化，
 * 而不是处理 Java 键值对。注意，像这样的实现最终应该在 Spark 核心中实现，
 * 但目前被一些更通用的 shuffle 接口/内部重构所阻塞。
 *
 * 这个 RDD 接受一个 [[ShuffleDependency]] (`dependency`) 和一个 [[ShufflePartitionSpec]] 数组作为输入参数。
 *
 * `dependency` 包含了这个 RDD 的父 RDD，它代表 shuffle 之前的数据集（即 map 输出）。
 * 这个 RDD 的元素是 (partitionId, Row) 对。
 * 分区 ID 应该在 [0, numPartitions - 1] 范围内。
 * `dependency.partitioner` 是用于分区 map 输出的原始分区器，
 * `dependency.partitioner.numPartitions` 是 shuffle 前的分区数量（即 map 输出的分区数量）。
 */
class ShuffledRowRDD(
    var dependency: ShuffleDependency[Int, InternalRow, InternalRow],   // 定义了 shuffle 的依赖关系
    metrics: Map[String, SQLMetric],
    partitionSpecs: Array[ShufflePartitionSpec])                        // 定义如何读取 shuffle 数据的规格数组
  extends RDD[InternalRow](dependency.rdd.context, Nil) {

  def this(
      dependency: ShuffleDependency[Int, InternalRow, InternalRow],
      metrics: Map[String, SQLMetric]) = {
    this(dependency, metrics,
      Array.tabulate(dependency.partitioner.numPartitions)(i => CoalescedPartitionSpec(i, i + 1)))
  }

  dependency.rdd.context.setLocalProperty(
    SortShuffleManager.FETCH_SHUFFLE_BLOCKS_IN_BATCH_ENABLED_KEY,
    SQLConf.get.fetchShuffleBlocksInBatch.toString)

  override def getDependencies: Seq[Dependency[_]] = List(dependency)

  /***
   * 如果所有分区规范都是CoalescedPartitionSpec类型，则创建CoalescedPartitioner
   * 检查分区索引的唯一性，确保分区器的有效性
   * 否则返回None
   */
  override val partitioner: Option[Partitioner] =
    if (partitionSpecs.forall(_.isInstanceOf[CoalescedPartitionSpec])) {
      val indices = partitionSpecs.map(_.asInstanceOf[CoalescedPartitionSpec].startReducerIndex)
      // TODO this check is based on assumptions of callers' behavior but is sufficient for now.
      if (indices.toSet.size == partitionSpecs.length) {
        Some(new CoalescedPartitioner(dependency.partitioner, indices))
      } else {
        None
      }
    } else {
      None
    }

  // 获取当前 RDD 的 Shuffle 分区限制
  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](partitionSpecs.length) { i =>
      ShuffledRowRDDPartition(i, partitionSpecs(i))
    }
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    partition.asInstanceOf[ShuffledRowRDDPartition].spec match {
      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
        // TODO order by partition size.
        startReducerIndex.until(endReducerIndex).flatMap { reducerIndex =>
          tracker.getPreferredLocationsForShuffle(dependency, reducerIndex)
        }

      case PartialReducerPartitionSpec(_, startMapIndex, endMapIndex, _) =>
        tracker.getMapLocation(dependency, startMapIndex, endMapIndex)

      case PartialMapperPartitionSpec(mapIndex, _, _) =>
        tracker.getMapLocation(dependency, mapIndex, mapIndex + 1)

      case CoalescedMapperPartitionSpec(startMapIndex, endMapIndex, numReducers) =>
        tracker.getMapLocation(dependency, startMapIndex, endMapIndex)
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val tempMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    // `SQLShuffleReadMetricsReporter` will update its own metrics for SQL exchange operator,
    // as well as the `tempMetrics` for basic shuffle metrics.
    val sqlMetricsReporter = new SQLShuffleReadMetricsReporter(tempMetrics, metrics)
    val reader = split.asInstanceOf[ShuffledRowRDDPartition].spec match {
      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
        SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          startReducerIndex,
          endReducerIndex,
          context,
          sqlMetricsReporter)

      case PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex, _) =>
        SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          startMapIndex,
          endMapIndex,
          reducerIndex,
          reducerIndex + 1,
          context,
          sqlMetricsReporter)

      case PartialMapperPartitionSpec(mapIndex, startReducerIndex, endReducerIndex) =>
        SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          mapIndex,
          mapIndex + 1,
          startReducerIndex,
          endReducerIndex,
          context,
          sqlMetricsReporter)

      case CoalescedMapperPartitionSpec(startMapIndex, endMapIndex, numReducers) =>
        SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          startMapIndex,
          endMapIndex,
          0,
          numReducers,
          context,
          sqlMetricsReporter)
    }
    reader.read().asInstanceOf[Iterator[Product2[Int, InternalRow]]].map(_._2)
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    dependency = null
  }
}
