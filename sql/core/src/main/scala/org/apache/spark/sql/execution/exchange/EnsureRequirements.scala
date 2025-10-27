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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * EnsureRequirements 是 Spark SQL 物理计划优化阶段的一个关键规则，主要负责：
 *
 * 1. 分布要求保证：确保每个物理操作符的输入数据满足其 Distribution 要求
 * 2. 排序要求保证：确保输入数据满足操作符的排序要求
 * 3. Shuffle 优化：在必要时插入 ShuffleExchange 或 BroadcastExchange
 * 4. Join 键重排序：优化 Join 操作的键顺序以减少不必要的 Shuffle
 */
case class EnsureRequirements(
    optimizeOutRepartition: Boolean = true,
    requiredDistribution: Option[Distribution] = None)
  extends Rule[SparkPlan] {

  /***
   * 功能：确保子节点满足分布和排序要求
   * • **第一步**：为每个子节点插入必要的 Exchange 操作
   * • **第二步**：处理多子节点的协同分区优化
   * • **第三步**：添加 SortExec 满足排序要求
   */
  private def ensureDistributionAndOrdering(
      originalChildren: Seq[SparkPlan],                    // 原始子节点
      requiredChildDistributions: Seq[Distribution],       // 每个子节点需要的数据分布
      requiredChildOrderings: Seq[Seq[SortOrder]],         // 每个子节点需要的排序
      shuffleOrigin: ShuffleOrigin): Seq[SparkPlan] = {    // 每个子节点的 Shuffle 要求

    assert(requiredChildDistributions.length == originalChildren.length)
    assert(requiredChildOrderings.length == originalChildren.length)

    //////////////////////////////
    // 第一阶段：满足基本分布要求  //
    //////////////////////////////
    var children = originalChildren.zip(requiredChildDistributions).map {

      // // 已经满足要求，不需要额外操作
      case (child, distribution) if child.outputPartitioning.satisfies(distribution) =>
        child

      // 需要广播
      case (child, BroadcastDistribution(mode)) =>
        BroadcastExchangeExec(mode, child)

      // 子节点的分区模式跟数据分布模式不一样， 需要插入 ShuffleExchangeExec 节点
      // 如果 TableScanA 输出是 SinglePartition，但需要 ClusteredDistribution([col1])
      // 则插入 ShuffleExchangeExec(HashPartitioning([col1], 200), TableScanA)
      case (child, distribution) =>
        val numPartitions = distribution.requiredNumPartitions
          .getOrElse(conf.numShufflePartitions)

        ShuffleExchangeExec(distribution.createPartitioning(numPartitions), child, shuffleOrigin)
    }

    // 第二阶段：处理多子节点的协同分区

    // 找出需要 ClusteredDistribution 的子节点索引
    val childrenIndexes = requiredChildDistributions.zipWithIndex.filter {
      case (_: ClusteredDistribution, _) => true
      case _ => false
    }.map(_._2)

    // 检查是否所有相关子节点都是单分区（
    val allSinglePartition =
      childrenIndexes.forall(children(_).outputPartitioning == SinglePartition)


    if (childrenIndexes.length > 1 && !allSinglePartition) {

      val specs = childrenIndexes.map(i => {
        val requiredDist = requiredChildDistributions(i)

        assert(requiredDist.isInstanceOf[ClusteredDistribution], s"Expected ClusteredDistribution but found ${requiredDist.getClass.getSimpleName}")

        i -> children(i).outputPartitioning.createShuffleSpec(
          requiredDist.asInstanceOf[ClusteredDistribution])
      }).toMap

      // Find out the shuffle spec that gives better parallelism. Currently this is done by
      // picking the spec with the largest number of partitions.
      //
      // NOTE: this is not optimal for the case when there are more than 2 children. Consider:
      //   (10, 10, 11)
      // where the number represent the number of partitions for each child, it's better to pick 10
      // here since we only need to shuffle one side - we'd need to shuffle two sides if we pick 11.
      //
      // However this should be sufficient for now since in Spark nodes with multiple children
      // always have exactly 2 children.

      // Whether we should consider `spark.sql.shuffle.partitions` and ensure enough parallelism
      // during shuffle. To achieve a good trade-off between parallelism and shuffle cost, we only
      // consider the minimum parallelism iff ALL children need to be re-shuffled.
      //
      // A child needs to be re-shuffled iff either one of below is true:
      //   1. It can't create partitioning by itself, i.e., `canCreatePartitioning` returns false
      //      (as for the case of `RangePartitioning`), therefore it needs to be re-shuffled
      //      according to other shuffle spec.
      //   2. It already has `ShuffleExchangeLike`, so we can re-use existing shuffle without
      //      introducing extra shuffle.
      //
      // On the other hand, in scenarios such as:
      //   HashPartitioning(5) <-> HashPartitioning(6)
      // while `spark.sql.shuffle.partitions` is 10, we'll only re-shuffle the left side and make it
      // HashPartitioning(6).

      // 判断是否需要考虑最小并行度（当所有子节点都需要重新 shuffle 时）
      val shouldConsiderMinParallelism = specs.forall(p =>
        !p._2.canCreatePartitioning || children(p._1).isInstanceOf[ShuffleExchangeLike]
      )
      // 选择可以用来创建分区的候选规格
      val candidateSpecs = specs
          .filter(_._2.canCreatePartitioning)
          .filter(p => !shouldConsiderMinParallelism ||
              children(p._1).outputPartitioning.numPartitions >= conf.defaultNumShufflePartitions)

      // 寻找最优的分区规则
      val bestSpecOpt = if (candidateSpecs.isEmpty) {
        None
      } else {
        // When choosing specs, we should consider those children with no `ShuffleExchangeLike` node
        // first. For instance, if we have:
        //   A: (No_Exchange, 100) <---> B: (Exchange, 120)
        // it's better to pick A and change B to (Exchange, 100) instead of picking B and insert a
        // new shuffle for A.
        val candidateSpecsWithoutShuffle = candidateSpecs.filter { case (k, _) =>
          !children(k).isInstanceOf[ShuffleExchangeLike]
        }
        val finalCandidateSpecs = if (candidateSpecsWithoutShuffle.nonEmpty) {
          candidateSpecsWithoutShuffle
        } else {
          candidateSpecs
        }
        // Pick the spec with the best parallelism
        Some(finalCandidateSpecs.values.maxBy(_.numPartitions))
      }

      // Check if 1) all children are of `KeyGroupedPartitioning` and 2) they are all compatible
      // with each other. If both are true, skip shuffle.
      val allCompatible = childrenIndexes.sliding(2).forall {
        case Seq(a, b) =>
          checkKeyGroupedSpec(specs(a)) && checkKeyGroupedSpec(specs(b)) &&
            specs(a).isCompatibleWith(specs(b))
      }

      // 第四阶段：应用最优规格进行重新分区
      children = children.zip(requiredChildDistributions).zipWithIndex.map {
        case ((child, _), idx) if allCompatible || !childrenIndexes.contains(idx) =>
          child
        case ((child, dist), idx) =>
          if (bestSpecOpt.isDefined && bestSpecOpt.get.isCompatibleWith(specs(idx))) {
            child
          } else {
            val newPartitioning = bestSpecOpt.map { bestSpec =>
              // Use the best spec to create a new partitioning to re-shuffle this child
              val clustering = dist.asInstanceOf[ClusteredDistribution].clustering
              bestSpec.createPartitioning(clustering)
            }.getOrElse {
              // No best spec available, so we create default partitioning from the required
              // distribution
              val numPartitions = dist.requiredNumPartitions
                  .getOrElse(conf.numShufflePartitions)
              dist.createPartitioning(numPartitions)
            }

            child match {
              case ShuffleExchangeExec(_, c, so) => ShuffleExchangeExec(newPartitioning, c, so)
              case _ => ShuffleExchangeExec(newPartitioning, child)
            }
          }
      }
    }

    // 第五阶段：确保排序要求
    children = children.zip(requiredChildOrderings).map { case (child, requiredOrdering) =>
      // If child.outputOrdering already satisfies the requiredOrdering, we do not need to sort.
      if (SortOrder.orderingSatisfies(child.outputOrdering, requiredOrdering)) {
        child
      } else {
        SortExec(requiredOrdering, global = false, child = child)
      }
    }

    children
  }

  /****
   * 功能：检查 KeyGroupedShuffleSpec 是否有效
   * • 验证分区表达式与聚类表达式的匹配关系
   * • 根据 REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION 配置决定检查严格程度
   */
  private def checkKeyGroupedSpec(shuffleSpec: ShuffleSpec): Boolean = {
    def check(spec: KeyGroupedShuffleSpec): Boolean = {
      val attributes = spec.partitioning.expressions.flatMap(_.collectLeaves())
      val clustering = spec.distribution.clustering

      if (SQLConf.get.getConf(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION)) {
        attributes.length == clustering.length && attributes.zip(clustering).forall {
          case (l, r) => l.semanticEquals(r)
        }
      } else {
        true // already validated in `KeyGroupedPartitioning.satisfies`
      }
    }
    shuffleSpec match {
      case spec: KeyGroupedShuffleSpec => check(spec)
      case ShuffleSpecCollection(specs) => specs.exists(checkKeyGroupedSpec)
      case _ => false
    }
  }

  /****
   * 尝试重排序 Join 键以匹配期望顺序
   * • 构建表达式到位置的映射
   * • 按期望顺序重新组织左右键
   * • 返回 None 表示无法重排序
   */
  private def reorder(
      leftKeys: IndexedSeq[Expression],
      rightKeys: IndexedSeq[Expression],
      expectedOrderOfKeys: Seq[Expression],
      currentOrderOfKeys: Seq[Expression]): Option[(Seq[Expression], Seq[Expression])] = {
    if (expectedOrderOfKeys.size != currentOrderOfKeys.size) {
      return None
    }

    // Check if the current order already satisfies the expected order.
    if (expectedOrderOfKeys.zip(currentOrderOfKeys).forall(p => p._1.semanticEquals(p._2))) {
      return Some(leftKeys, rightKeys)
    }

    // Build a lookup between an expression and the positions its holds in the current key seq.
    val keyToIndexMap = mutable.Map.empty[Expression, mutable.BitSet]
    currentOrderOfKeys.zipWithIndex.foreach {
      case (key, index) =>
        keyToIndexMap.getOrElseUpdate(key.canonicalized, mutable.BitSet.empty).add(index)
    }

    // Reorder the keys.
    val leftKeysBuffer = new ArrayBuffer[Expression](leftKeys.size)
    val rightKeysBuffer = new ArrayBuffer[Expression](rightKeys.size)
    val iterator = expectedOrderOfKeys.iterator
    while (iterator.hasNext) {
      // Lookup the current index of this key.
      keyToIndexMap.get(iterator.next().canonicalized) match {
        case Some(indices) if indices.nonEmpty =>
          // Take the first available index from the map.
          val index = indices.firstKey
          indices.remove(index)

          // Add the keys for that index to the reordered keys.
          leftKeysBuffer += leftKeys(index)
          rightKeysBuffer += rightKeys(index)
        case _ =>
          // The expression cannot be found, or we have exhausted all indices for that expression.
          return None
      }
    }
    Some(leftKeysBuffer.toSeq, rightKeysBuffer.toSeq)
  }

  /****
   * 功能：尝试重排序 Join 键以匹配期望顺序
   * • 构建表达式到位置的映射
   * • 按期望顺序重新组织左右键
   * • 返回 None 表示无法重排序
   */
  private def reorderJoinKeys(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      leftPartitioning: Partitioning,
      rightPartitioning: Partitioning): (Seq[Expression], Seq[Expression]) = {
    if (leftKeys.forall(_.deterministic) && rightKeys.forall(_.deterministic)) {
      reorderJoinKeysRecursively(
        leftKeys,
        rightKeys,
        Some(leftPartitioning),
        Some(rightPartitioning))
        .getOrElse((leftKeys, rightKeys))
    } else {
      (leftKeys, rightKeys)
    }
  }

  /**
   * 功能：Join 键重排序的公共入口
   *    • 检查键的确定性
   *    • 调用递归重排序方法
   *    • 失败时返回原始键序列
   */
  private def reorderJoinKeysRecursively(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      leftPartitioning: Option[Partitioning],
      rightPartitioning: Option[Partitioning]): Option[(Seq[Expression], Seq[Expression])] = {
    (leftPartitioning, rightPartitioning) match {
      case (Some(HashPartitioning(leftExpressions, _)), _) =>
        reorder(leftKeys.toIndexedSeq, rightKeys.toIndexedSeq, leftExpressions, leftKeys)
          .orElse(reorderJoinKeysRecursively(
            leftKeys, rightKeys, None, rightPartitioning))
      case (_, Some(HashPartitioning(rightExpressions, _))) =>
        reorder(leftKeys.toIndexedSeq, rightKeys.toIndexedSeq, rightExpressions, rightKeys)
          .orElse(reorderJoinKeysRecursively(
            leftKeys, rightKeys, leftPartitioning, None))
      case (Some(KeyGroupedPartitioning(clustering, _, _)), _) =>
        val leafExprs = clustering.flatMap(_.collectLeaves())
        reorder(leftKeys.toIndexedSeq, rightKeys.toIndexedSeq, leafExprs, leftKeys)
            .orElse(reorderJoinKeysRecursively(
              leftKeys, rightKeys, None, rightPartitioning))
      case (_, Some(KeyGroupedPartitioning(clustering, _, _))) =>
        val leafExprs = clustering.flatMap(_.collectLeaves())
        reorder(leftKeys.toIndexedSeq, rightKeys.toIndexedSeq, leafExprs, rightKeys)
            .orElse(reorderJoinKeysRecursively(
              leftKeys, rightKeys, leftPartitioning, None))
      case (Some(PartitioningCollection(partitionings)), _) =>
        partitionings.foldLeft(Option.empty[(Seq[Expression], Seq[Expression])]) { (res, p) =>
          res.orElse(reorderJoinKeysRecursively(leftKeys, rightKeys, Some(p), rightPartitioning))
        }.orElse(reorderJoinKeysRecursively(leftKeys, rightKeys, None, rightPartitioning))
      case (_, Some(PartitioningCollection(partitionings))) =>
        partitionings.foldLeft(Option.empty[(Seq[Expression], Seq[Expression])]) { (res, p) =>
          res.orElse(reorderJoinKeysRecursively(leftKeys, rightKeys, leftPartitioning, Some(p)))
        }.orElse(None)
      case _ =>
        None
    }
  }

  /**
   * 功能：专门处理 Join 操作符的键重排序
   * • **ShuffledHashJoinExec**：重排序后创建新的 ShuffledHashJoin
   * • **SortMergeJoinExec**：重排序后创建新的 SortMergeJoin
   * • 其他操作符直接返回
   */
  private def reorderJoinPredicates(plan: SparkPlan): SparkPlan = {
    plan match {
      case ShuffledHashJoinExec(
        leftKeys, rightKeys, joinType, buildSide, condition, left, right, isSkew) =>
        val (reorderedLeftKeys, reorderedRightKeys) =
          reorderJoinKeys(leftKeys, rightKeys, left.outputPartitioning, right.outputPartitioning)
        ShuffledHashJoinExec(reorderedLeftKeys, reorderedRightKeys, joinType, buildSide, condition,
          left, right, isSkew)

      case SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right, isSkew) =>
        val (reorderedLeftKeys, reorderedRightKeys) =
          reorderJoinKeys(leftKeys, rightKeys, left.outputPartitioning, right.outputPartitioning)
        SortMergeJoinExec(reorderedLeftKeys, reorderedRightKeys, joinType, condition,
          left, right, isSkew)

      case other => other
    }
  }

  /****
   * 功能：规则的主入口，对整个物理计划树进行转换
   * • 自底向上遍历计划树 (transformUp)
   * • 优化冗余的重分区操作
   * • 对每个操作符应用分布和排序要求
   * • 处理顶层的 requiredDistribution 参数
   *
   * apply()
   * ├── transformUp() 遍历每个节点
   * │   ├── 冗余重分区检查
   * │   └── reorderJoinPredicates()
   * │       └── reorderJoinKeys()
   * │           └── reorderJoinKeysRecursively()
   * │               └── reorder()
   * ├── ensureDistributionAndOrdering()
   * │   ├── 插入 BroadcastExchange/ShuffleExchange
   * │   ├── 协同分区优化
   * │   │   └── checkKeyGroupedSpec()
   * │   └── 插入 SortExec
   * └── 处理顶层 requiredDistribution
   */
  def apply(plan: SparkPlan): SparkPlan = {

    // 自底向上遍历物理计划树，对每个节点应用转换规则
    val newPlan = plan.transformUp {

      //////////////////////////////////////////////////////////////////////////////////////////////
      // 冗余重分区优化分支- 如果ShuffleExchangeExec 子节点的分区形式跟父节点相同，则消除该 Exchange 节点 //
      //////////////////////////////////////////////////////////////////////////////////////////////

      case operator @ ShuffleExchangeExec(upper: HashPartitioning, child, shuffleOrigin)
          if optimizeOutRepartition &&
            // 当前分区策略是按照列 || 按照数量分区
            // 检测由用户显式调用 repartition() 产生的 ShuffleExchange
            (shuffleOrigin == REPARTITION_BY_COL || shuffleOrigin == REPARTITION_BY_NUM) =>

        // 检查子节点的分区是否与上层 ShuffleExchange 语义相等
        def hasSemanticEqualPartitioning(partitioning: Partitioning): Boolean = {
          partitioning match {
            case lower: HashPartitioning if upper.semanticEquals(lower) => true
            case lower: PartitioningCollection =>
              lower.partitionings.exists(hasSemanticEqualPartitioning)
            case _ => false
          }
        }

        if (hasSemanticEqualPartitioning(child.outputPartitioning)) {
          child      // // 直接返回子节点，消除冗余 ShuffleExchange
        } else {
          operator    // 保留 ShuffleExchange
        }


      //////////////////////////////////////////////////////////////
      //                      通用操作符处理分支                    //
      //////////////////////////////////////////////////////////////
      case operator: SparkPlan =>

        // 重排序 Join 操作的键，优化分区匹配
        //
        // 重排序前 : leftKeys = [a, b], rightKeys = [b, a]
        // 重排序后 : leftKeys = [a, b], rightKeys = [a, b]  // 避免额外 shuffle
        val reordered = reorderJoinPredicates(operator)

        // 确保每个子节点满足当前操作符的分布和排序要求
        // 原始子节点
        // children = [TableScan(t1), TableScan(t2)]
        // 插入必要的 Exchange 和 Sort
        // newChildren = [
        //    SortExec(sortOrder=[a ASC, b ASC],
        //    ShuffleExchangeExec(HashPartitioning([a,b], 200), TableScan(t1))),
        //    SortExec(sortOrder=[a ASC, b ASC],
        //    ShuffleExchangeExec(HashPartitioning([a,b], 200), TableScan(t2)))
        //  ]
        val newChildren = ensureDistributionAndOrdering(
          reordered.children,
          reordered.requiredChildDistribution,
          reordered.requiredChildOrdering,
          ENSURE_REQUIREMENTS)

        reordered.withNewChildren(newChildren)
    }

    // 顶层分布要求处理
    if (requiredDistribution.isDefined) {

      // 处理整个查询的顶层分布要求（通常用于测试或特殊场景）
      val shuffleOrigin = if (requiredDistribution.get.requiredNumPartitions.isDefined) {
        REPARTITION_BY_NUM     // 按分区数重分区
      } else {
        REPARTITION_BY_COL     // 按列重分区
      }

      // 重分区
      val finalPlan = ensureDistributionAndOrdering(
        newPlan :: Nil,
        requiredDistribution.get :: Nil,
        Seq(Nil),
        shuffleOrigin)

      assert(finalPlan.size == 1)
      finalPlan.head
    } else {
      newPlan
    }
  }
}
