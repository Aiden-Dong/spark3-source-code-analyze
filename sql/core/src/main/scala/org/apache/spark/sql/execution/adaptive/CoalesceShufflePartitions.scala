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

import scala.collection.mutable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{ShufflePartitionSpec, SparkPlan, UnaryExecNode, UnionExec}
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, REBALANCE_PARTITIONS_BY_COL, REBALANCE_PARTITIONS_BY_NONE, REPARTITION_BY_COL, ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * CoalesceShufflePartitions是AQE中的一个核心优化规则，主要用于：
 * • **合并小的shuffle分区**，减少任务数量
 * • **避免大量小任务**，提高整体执行性能
 * • **基于实际数据统计**进行动态分区调整
 */
case class CoalesceShufflePartitions(session: SparkSession) extends AQEShuffleReadRule {

  override val supportedShuffleOrigins: Seq[ShuffleOrigin] =
    Seq(ENSURE_REQUIREMENTS, REPARTITION_BY_COL, REBALANCE_PARTITIONS_BY_NONE,
      REBALANCE_PARTITIONS_BY_COL)


  // 最小分区数量（保证并行度）
  val minNumPartitions = conf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM).getOrElse {
    if (conf.getConf(SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST)) {
      // 如果未设置最小合并分区数，则我们会退回到 Spark 默认并行度，以避免与不进行合并相比的性能回归。
      session.sparkContext.defaultParallelism
    } else {
      1
    }
  }

  // 目标分区大小 spark.sql.adaptive.advisoryPartitionSizeInBytes
  val advisoryTargetSize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)

  // 最小分区大小（默认1MB）
  val minPartitionSize = if (Utils.isTesting) {
    conf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE).min(advisoryTargetSize / 5)
  } else {
    conf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE)
  }

  override def isSupported(shuffle: ShuffleExchangeLike): Boolean = {
    shuffle.outputPartitioning != SinglePartition && super.isSupported(shuffle)
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.coalesceShufflePartitionsEnabled) {
      return plan
    }


    // TODO - 1 :  收集整理要合并的分区  : Seq[Seq[ShuffleStageInfo]]
    val coalesceGroups = collectCoalesceGroups(plan)

    // TODO - 2 : 分配最小分区数给各组
    val minNumPartitionsByGroup = if (coalesceGroups.length == 1) {
      Seq(math.max(minNumPartitions, 1))
    } else {

      // 计算得到每个合并的算子的数据量
      val sizes = coalesceGroups.map(_.flatMap(_.shuffleStage.mapStats.map(_.bytesByPartitionId.sum)).sum)

      val totalSize = sizes.sum  // 计算出总的大小

      // 计算每个组的分区
      sizes.map {
        size =>
          val num = if (totalSize > 0) {
            math.round(minNumPartitions * 1.0 * size / totalSize)  // 需要在解释一下
          } else {
            minNumPartitions
          }
          math.max(num.toInt, 1)
      }
    }

    // 计算每个 shuffle stage 的合并 shuffle 算子树
    val specsMap = mutable.HashMap.empty[Int, Seq[ShufflePartitionSpec]]

    // Coalesce partitions for each coalesce group independently.
    coalesceGroups.zip(minNumPartitionsByGroup)
      .foreach {
        case (shuffleStages, minNumPartitions) =>  // 每个组与每个组的最小分区数

          // 调用核心合并算法，为每个shuffle阶段生成新的分区规格
          val newPartitionSpecs = ShufflePartitionsUtil.coalescePartitions(
          shuffleStages.map(_.shuffleStage.mapStats),    // 输入分区信息
          shuffleStages.map(_.partitionSpecs),           // stage 的分区描述信息 - 这是是处理数据倾斜问题的
          advisoryTargetSize = advisoryTargetSize,       // 动态分区目标数据大小
          minNumPartitions = minNumPartitions,           // 最小分区量
          minPartitionSize = minPartitionSize)           // 最低分区值

        if (newPartitionSpecs.nonEmpty) {
          shuffleStages.zip(newPartitionSpecs).map { case (stageInfo, partSpecs) =>
            specsMap.put(stageInfo.shuffleStage.id, partSpecs)
          }
        }
      }

    if (specsMap.nonEmpty) {
      updateShuffleReads(plan, specsMap.toMap)
    } else {
      plan
    }
  }

  /**
   * 收集所有可合并的组，以便每个 Union 操作符的子操作符的洗牌阶段都在它们各自的独立组中，如果：
   * - 该子操作符的所有叶节点都是洗牌阶段；以及
   * - 所有这些洗牌阶段都支持合并。
   *
   * shuffle阶段信息的分组序列，每个内层序列是一个独立的合并组
   */
  private def collectCoalesceGroups(plan: SparkPlan): Seq[Seq[ShuffleStageInfo]] = plan match {
    case r @ AQEShuffleReadExec(q: ShuffleQueryStageExec, _) if isSupported(q.shuffle) => Seq(collectShuffleStageInfos(r))
    case unary: UnaryExecNode => collectCoalesceGroups(unary.child)

    case union: UnionExec => union.children.flatMap(collectCoalesceGroups)
    // 如果并非所有叶节点都是查询阶段，那么减少洗牌分区数量可能会破坏 Spark 计划中子操作符之间关于输出分区数量的假设，导致任务执行失败。

    // 处理 JOIN 之类的类型
    case p if p.collectLeaves().forall(_.isInstanceOf[QueryStageExec]) =>
      val shuffleStages = collectShuffleStageInfos(p)
      // 由重新分区引入的 ShuffleExchange 不支持更改分区数量。
      // 只有如果所有的 ShuffleExchange 都支持，我们才会更改分区数量。
      if (shuffleStages.forall(s => isSupported(s.shuffleStage.shuffle))) {
        Seq(shuffleStages)
      } else {
        Seq.empty
      }
    case _ => Seq.empty
  }

  private def collectShuffleStageInfos(plan: SparkPlan): Seq[ShuffleStageInfo] = plan match {
    case ShuffleStageInfo(stage, specs) => Seq(new ShuffleStageInfo(stage, specs))
    case _ => plan.children.flatMap(collectShuffleStageInfos)
  }

  private def updateShuffleReads(
      plan: SparkPlan, specsMap: Map[Int, Seq[ShufflePartitionSpec]]): SparkPlan = plan match {
    // Even for shuffle exchange whose input RDD has 0 partition, we should still update its
    // `partitionStartIndices`, so that all the leaf shuffles in a stage have the same
    // number of output partitions.
    case ShuffleStageInfo(stage, _) =>
      specsMap.get(stage.id).map { specs =>
        AQEShuffleReadExec(stage, specs)
      }.getOrElse(plan)
    case other => other.mapChildren(updateShuffleReads(_, specsMap))
  }
}

private class ShuffleStageInfo(
    val shuffleStage: ShuffleQueryStageExec,
    val partitionSpecs: Option[Seq[ShufflePartitionSpec]])

private object ShuffleStageInfo {
  def unapply(plan: SparkPlan)
  : Option[(ShuffleQueryStageExec, Option[Seq[ShufflePartitionSpec]])] = plan match {
    case stage: ShuffleQueryStageExec =>
      Some((stage, None))
    case AQEShuffleReadExec(s: ShuffleQueryStageExec, partitionSpecs) =>
      Some((s, Some(partitionSpecs)))
    case _ => None
  }
}
