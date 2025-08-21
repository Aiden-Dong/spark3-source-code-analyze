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

import org.apache.commons.io.FileUtils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, EnsureRequirements, ValidateRequirements}
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * 一个优化规则，用于优化倾斜连接，避免数据量显著大于其他任务的数据量的滞后任务。
 *
 * 一般思路是将每个倾斜分区划分为更小的分区，并在连接的另一侧复制其匹配分区，以便它们能够并行运行任务。
 * 注意，当左右两侧的匹配分区都存在倾斜时，它将变成左右分区的笛卡尔积连接。
 *
 * 例如，假设 Sort-Merge 连接有 4 个分区：
 * 左侧：[L1, L2, L3, L4]
 * 右侧：[R1, R2, R3, R4]
 *
 * 假设 L2、L4 和 R3、R4 是倾斜的，每个分区被拆分成 2 个子分区。最初计划运行 4 个任务：
 * (L1, R1)，(L2, R2)，(L3, R3)，(L4, R4)。
 * 该规则将其扩展为 9 个任务以增加并行性：
 * (L1, R1)，
 * (L2-1, R2)，(L2-2, R2)，
 * (L3, R3-1)，(L3, R3-2)，
 * (L4-1, R4-1)，(L4-2, R4-1)，(L4-1, R4-2)，(L4-2, R4-2)
 */
case class OptimizeSkewedJoin(ensureRequirements: EnsureRequirements)
  extends Rule[SparkPlan] {

  /**
   * A partition is considered as a skewed partition if its size is larger than the median
   * partition size * SKEW_JOIN_SKEWED_PARTITION_FACTOR and also larger than
   * SKEW_JOIN_SKEWED_PARTITION_THRESHOLD. Thus we pick the larger one as the skew threshold.
   */
  def getSkewThreshold(medianSize: Long): Long = {
    conf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD).max(
      medianSize * conf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_FACTOR))
  }

  /**
   * The goal of skew join optimization is to make the data distribution more even. The target size
   * to split skewed partitions is the average size of non-skewed partition, or the
   * advisory partition size if avg size is smaller than it.
   */
  private def targetSize(sizes: Array[Long], skewThreshold: Long): Long = {
    val advisorySize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)
    val nonSkewSizes = sizes.filter(_ <= skewThreshold)
    if (nonSkewSizes.isEmpty) {
      advisorySize
    } else {
      math.max(advisorySize, nonSkewSizes.sum / nonSkewSizes.length)
    }
  }

  private def canSplitLeftSide(joinType: JoinType) = {
    joinType == Inner || joinType == Cross || joinType == LeftSemi ||
      joinType == LeftAnti || joinType == LeftOuter
  }

  private def canSplitRightSide(joinType: JoinType) = {
    joinType == Inner || joinType == Cross || joinType == RightOuter
  }

  private def getSizeInfo(medianSize: Long, sizes: Array[Long]): String = {
    s"median size: $medianSize, max size: ${sizes.max}, min size: ${sizes.min}, avg size: " +
      sizes.sum / sizes.length
  }

  /**
   * 该方法旨在通过以下步骤优化倾斜连接(join)：
   * 基于原始shuffle连接(SMJ和SHJ)中的分区大小中位数和倾斜阈值，检测shuffle分区是否存在数据倾斜。
   * 假设左侧partition0存在倾斜，且有5个映射任务(Map0到Map4)。
   * 根据映射任务数据量和最大拆分数量，将这5个映射任务拆分为3个区间：
   * [(Map0,Map1), (Map2,Map3), (Map4)]。
   * 在join左子节点外包装一个特殊的shuffle读取器，每个映射区间用一个任务(共3个任务)加载。
   * 在join右子节点外包装一个特殊的shuffle读取器，由3个独立任务分别加载partition0数据3次。
   *
   * -- 假设我们有这样的查询
   * SELECT * FROM large_table l JOIN small_table s ON l.id = s.id
   *
   * -- 数据分布情况：
   * -- large_table: 1000万条记录，按 id 分成 4 个分区
   * -- small_table: 100万条记录，按 id 分成 4 个分区
   * -- 但数据分布不均匀，存在倾斜
   *
   * // 假设经过 Shuffle 后的分区大小统计
   * val leftSizes = Array(50MB, 800MB, 60MB, 90MB)   // large_table 各分区大小
   * val rightSizes = Array(5MB, 80MB, 6MB, 9MB)      // small_table 各分区大小
   *
   * // 可以看到分区1存在严重倾斜：
   * // - 左侧分区1: 800MB (远大于其他分区的 50-90MB)
   * // - 右侧分区1: 80MB (远大于其他分区的 5-9MB)
   *
   */
  private def tryOptimizeJoinChildren(
      left: ShuffleQueryStageExec,
      right: ShuffleQueryStageExec,
      joinType: JoinType): Option[(SparkPlan, SparkPlan)] = {
    val canSplitLeft = canSplitLeftSide(joinType)
    val canSplitRight = canSplitRightSide(joinType)
    if (!canSplitLeft && !canSplitRight) return None

    val leftSizes = left.mapStats.get.bytesByPartitionId    // // [50MB, 800MB, 60MB, 90MB]
    val rightSizes = right.mapStats.get.bytesByPartitionId  // // [5MB, 80MB, 6MB, 9MB]
    assert(leftSizes.length == rightSizes.length)

    val numPartitions = leftSizes.length

    // 计算分区中位数
    val leftMedSize = Utils.median(leftSizes, false)     // median([50, 800, 60, 90]) = 75MB  (60 + 90) / 2 = 75MB
    val rightMedSize = Utils.median(rightSizes, false)   // median([5, 80, 6, 9]) = 7.5MB  (6 + 9) / 2 = 7.5MB
    logDebug(
      s"""
         |Optimizing skewed join.
         |Left side partitions size info:
         |${getSizeInfo(leftMedSize, leftSizes)}
         |Right side partitions size info:
         |${getSizeInfo(rightMedSize, rightSizes)}
      """.stripMargin)

    // 计算倾斜阈值和目标大小
    val leftSkewThreshold = getSkewThreshold(leftMedSize)            // max(256MB, 75MB * 5) = max(256MB, 375MB) = 375MB
    val rightSkewThreshold = getSkewThreshold(rightMedSize)          // max(256MB, 7.5MB * 5) = max(256MB, 37.5MB) = 256MB


    val leftTargetSize = targetSize(leftSizes, leftSkewThreshold)      // 计算左侧目标分区大小  (50 + 60 + 90) / 3 = 66.7MB  (过滤掉 800MB（> 375MB)
    val rightTargetSize = targetSize(rightSizes, rightSkewThreshold)   // 计算右侧目标分区大小 max(64M, (5 + 6 + 9 + 80) / 4 = 25MB)

    // 初始化结果容器
    val leftSidePartitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
    val rightSidePartitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
    var numSkewedLeft = 0
    var numSkewedRight = 0


    for (partitionIndex <- 0 until numPartitions) {    // 遍历分区 0, 1, 2, 3

      val leftSize = leftSizes(partitionIndex)                         // 左侧该分区分区大小  [50MB, 800MB, 60MB, 90MB]
      val isLeftSkew = canSplitLeft && leftSize > leftSkewThreshold    // 左侧该分区有没有发生倾斜

      val rightSize = rightSizes(partitionIndex)                          // 右侧该分区分区大小      [5MB, 80MB, 6MB, 9MB]
      val isRightSkew = canSplitRight && rightSize > rightSkewThreshold   // 右侧该分区有没有发生倾斜

      // 创建非倾斜分区规格
      val leftNoSkewPartitionSpec =
        Seq(CoalescedPartitionSpec(partitionIndex, partitionIndex + 1, leftSize))
      val rightNoSkewPartitionSpec =
        Seq(CoalescedPartitionSpec(partitionIndex, partitionIndex + 1, rightSize))

      // 左侧拆分结果分区
      val leftParts = if (isLeftSkew) {
        // 相对于 partition - 1 : 800M 可能切分为 :
        //  PartialReducerPartitionSpec(1, 0, 2, 200MB),  // 对应 Task 1
        //  PartialReducerPartitionSpec(1, 2, 4, 200MB),  // 对应 Task 2
        //  PartialReducerPartitionSpec(1, 4, 6, 200MB),  // 对应 Task 3
        //  PartialReducerPartitionSpec(1, 6, 8, 200MB),  // 对应 Task 4

        val skewSpecs = ShufflePartitionsUtil.createSkewPartitionSpecs(
          left.mapStats.get.shuffleId, partitionIndex, leftTargetSize)

        if (skewSpecs.isDefined) {
          logDebug(s"Left side partition $partitionIndex " +
            s"(${FileUtils.byteCountToDisplaySize(leftSize)}) is skewed, " +
            s"split it into ${skewSpecs.get.length} parts.")
          numSkewedLeft += 1
        }
        skewSpecs.getOrElse(leftNoSkewPartitionSpec)
      } else {
        leftNoSkewPartitionSpec
      }

      // 右侧拆分结果分区
      val rightParts = if (isRightSkew) {
        val skewSpecs = ShufflePartitionsUtil.createSkewPartitionSpecs(
          right.mapStats.get.shuffleId, partitionIndex, rightTargetSize)
        if (skewSpecs.isDefined) {
          logDebug(s"Right side partition $partitionIndex " +
            s"(${FileUtils.byteCountToDisplaySize(rightSize)}) is skewed, " +
            s"split it into ${skewSpecs.get.length} parts.")
          numSkewedRight += 1
        }
        skewSpecs.getOrElse(rightNoSkewPartitionSpec)
      } else {
        rightNoSkewPartitionSpec
      }

      // 分区1处理
      //leftParts = Seq(
      //  PartialReducerPartitionSpec(1, 0, 2, 200MB),  // 子分区1-1
      //  PartialReducerPartitionSpec(1, 2, 4, 200MB),  // 子分区1-2
      //  PartialReducerPartitionSpec(1, 4, 6, 200MB),  // 子分区1-3
      //  PartialReducerPartitionSpec(1, 6, 8, 200MB)   // 子分区1-4
      //)
      //
      //rightParts = Seq(
      //  CoalescedPartitionSpec(1, 2, 80MB)  // 右侧分区1保持不变
      //)
      // 生成4个任务组合：
      // (PartialReducerPartitionSpec(1,0,2,200MB), CoalescedPartitionSpec(1,2,80MB))
      // (PartialReducerPartitionSpec(1,2,4,200MB), CoalescedPartitionSpec(1,2,80MB))
      // (PartialReducerPartitionSpec(1,4,6,200MB), CoalescedPartitionSpec(1,2,80MB))
      // (PartialReducerPartitionSpec(1,6,8,200MB), CoalescedPartitionSpec(1,2,80MB))
      for {
        leftSidePartition <- leftParts        // 4 个分区
        rightSidePartition <- rightParts      // 1 个分区
      } {
        leftSidePartitions += leftSidePartition
        rightSidePartitions += rightSidePartition
      }
    }

    logDebug(s"number of skewed partitions: left $numSkewedLeft, right $numSkewedRight")
    if (numSkewedLeft > 0 || numSkewedRight > 0) {
      Some((
        SkewJoinChildWrapper(AQEShuffleReadExec(left, leftSidePartitions.toSeq)),
        SkewJoinChildWrapper(AQEShuffleReadExec(right, rightSidePartitions.toSeq))
      ))
    } else {
      None
    }
  }

  def optimizeSkewJoin(plan: SparkPlan): SparkPlan = plan.transformUp {
    case smj @ SortMergeJoinExec(_, _, joinType, _,
        s1 @ SortExec(_, _, ShuffleStage(left: ShuffleQueryStageExec), _),
        s2 @ SortExec(_, _, ShuffleStage(right: ShuffleQueryStageExec), _), false) =>
      tryOptimizeJoinChildren(left, right, joinType).map {
        case (newLeft, newRight) =>
          smj.copy(
            left = s1.copy(child = newLeft), right = s2.copy(child = newRight), isSkewJoin = true)
      }.getOrElse(smj)

    case shj @ ShuffledHashJoinExec(_, _, joinType, _, _,
        ShuffleStage(left: ShuffleQueryStageExec),
        ShuffleStage(right: ShuffleQueryStageExec), false) =>
      tryOptimizeJoinChildren(left, right, joinType).map {
        case (newLeft, newRight) =>
          shj.copy(left = newLeft, right = newRight, isSkewJoin = true)
      }.getOrElse(shj)
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.SKEW_JOIN_ENABLED)) {
      return plan
    }

    // We try to optimize every skewed sort-merge/shuffle-hash joins in the query plan. If this
    // introduces extra shuffles, we give up the optimization and return the original query plan, or
    // accept the extra shuffles if the force-apply config is true.
    // TODO: It's possible that only one skewed join in the query plan leads to extra shuffles and
    //       we only need to skip optimizing that join. We should make the strategy smarter here.
    val optimized = optimizeSkewJoin(plan)
    val requirementSatisfied = if (ensureRequirements.requiredDistribution.isDefined) {
      ValidateRequirements.validate(optimized, ensureRequirements.requiredDistribution.get)
    } else {
      ValidateRequirements.validate(optimized)
    }
    if (requirementSatisfied) {
      optimized.transform {
        case SkewJoinChildWrapper(child) => child
      }
    } else if (conf.getConf(SQLConf.ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN)) {
      ensureRequirements.apply(optimized).transform {
        case SkewJoinChildWrapper(child) => child
      }
    } else {
      plan
    }
  }

  object ShuffleStage {
    def unapply(plan: SparkPlan): Option[ShuffleQueryStageExec] = plan match {
      case s: ShuffleQueryStageExec if s.isMaterialized && s.mapStats.isDefined &&
        s.shuffle.shuffleOrigin == ENSURE_REQUIREMENTS =>
        Some(s)
      case _ => None
    }
  }
}

// After optimizing skew joins, we need to run EnsureRequirements again to add necessary shuffles
// caused by skew join optimization. However, this shouldn't apply to the sub-plan under skew join,
// as it's guaranteed to satisfy distribution requirement.
case class SkewJoinChildWrapper(plan: SparkPlan) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
  override def output: Seq[Attribute] = plan.output
  override def outputPartitioning: Partitioning = plan.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = plan.outputOrdering
}
