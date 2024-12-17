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

import org.apache.spark.sql.execution.{CoalescedPartitionSpec, ShufflePartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{REBALANCE_PARTITIONS_BY_COL, REBALANCE_PARTITIONS_BY_NONE, ShuffleOrigin}
import org.apache.spark.sql.internal.SQLConf

/**
 * 一个规则，用于基于 map 输出统计信息优化 [[RebalancePartitions]] 中的倾斜 shuffle 分区，
 * 以避免影响性能的数据倾斜。
 *
 * 我们使用 ADVISORY_PARTITION_SIZE_IN_BYTES 大小来决定是否优化某个分区。
 * 假设我们有 3 个 map 和 3 个 shuffle 分区，并假设 r1 存在数据倾斜问题。
 * map 端如下所示：
 *   m0:[b0, b1, b2], m1:[b0, b1, b2], m2:[b0, b1, b2]
 * reduce 端如下所示：
 *                            （没有此规则时） r1[m0-b1, m1-b1, m2-b1]
 *                              /                                     \
 *   r0:[m0-b0, m1-b0, m2-b0], r1-0:[m0-b1], r1-1:[m1-b1], r1-2:[m2-b1], r2[m0-b2, m1-b2, m2-b2]
 */
object OptimizeSkewInRebalancePartitions extends AQEShuffleReadRule {

  override val supportedShuffleOrigins: Seq[ShuffleOrigin] =
    Seq(REBALANCE_PARTITIONS_BY_NONE, REBALANCE_PARTITIONS_BY_COL)

  /**
   * 基于 map 大小和拆分后目标分区大小拆分倾斜分区。
   * 为倾斜分区创建一个 `PartialReducerPartitionSpec` 列表，为正常分区创建 `CoalescedPartition`。
   */
  private def optimizeSkewedPartitions(
      shuffleId: Int,
      bytesByPartitionId: Array[Long],
      targetSize: Long): Seq[ShufflePartitionSpec] = {
    val smallPartitionFactor =
      conf.getConf(SQLConf.ADAPTIVE_REBALANCE_PARTITIONS_SMALL_PARTITION_FACTOR)
    bytesByPartitionId.indices.flatMap { reduceIndex =>
      val bytes = bytesByPartitionId(reduceIndex)
      if (bytes > targetSize) {
        val newPartitionSpec = ShufflePartitionsUtil.createSkewPartitionSpecs(
          shuffleId, reduceIndex, targetSize, smallPartitionFactor)
        if (newPartitionSpec.isEmpty) {
          CoalescedPartitionSpec(reduceIndex, reduceIndex + 1, bytes) :: Nil
        } else {
          logDebug(s"For shuffle $shuffleId, partition $reduceIndex is skew, " +
            s"split it into ${newPartitionSpec.get.size} parts.")
          newPartitionSpec.get
        }
      } else {
        CoalescedPartitionSpec(reduceIndex, reduceIndex + 1, bytes) :: Nil
      }
    }
  }

  private def tryOptimizeSkewedPartitions(shuffle: ShuffleQueryStageExec): SparkPlan = {
    val advisorySize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)
    val mapStats = shuffle.mapStats
    if (mapStats.isEmpty ||
      mapStats.get.bytesByPartitionId.forall(_ <= advisorySize)) {
      return shuffle
    }

    val newPartitionsSpec = optimizeSkewedPartitions(
      mapStats.get.shuffleId, mapStats.get.bytesByPartitionId, advisorySize)
    // return origin plan if we can not optimize partitions
    if (newPartitionsSpec.length == mapStats.get.bytesByPartitionId.length) {
      shuffle
    } else {
      AQEShuffleReadExec(shuffle, newPartitionsSpec)
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.ADAPTIVE_OPTIMIZE_SKEWS_IN_REBALANCE_PARTITIONS_ENABLED)) {
      return plan
    }

    plan transformUp {
      case stage: ShuffleQueryStageExec if isSupported(stage.shuffle) =>
        tryOptimizeSkewedPartitions(stage)
    }
  }
}
