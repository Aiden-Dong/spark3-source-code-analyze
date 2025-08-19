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

package org.apache.spark.sql.execution.dynamicpruning

import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.read.SupportsRuntimeFiltering
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

/**
 * 动态分区剪枝优化根据join操作的类型和选择性来执行。在查询优化过程中，我们通过以下方式在可筛选表上插入谓词：
 *  1. 使用连接另一侧的筛选条件
 *  2. 采用名为DynamicPruning的自定义包装器
 *
 * 动态分区剪枝(DPP)的基础实现机制是：在满足以下条件时插入包含另一侧筛选条件的重复子查询：
 *    (1) 待剪枝表可通过JOIN键进行过滤
 *    (2) 连接操作属于以下类型之一：INNER、LEFT SEMI、LEFT OUTER（右侧分区）或RIGHT OUTER（左侧分区）
 *
 * 为了实现广播场景的直接分区剪枝，我们采用自定义的DynamicPruning子句，该子句将In条件、子查询和收益评估封装在一起。
 * 在查询规划阶段确定连接类型后，决策机制如下：
 *
 *    (1) 若为广播哈希连接，则用广播结果复用替代重复子查询
 *    (2) 若分区剪枝的预估收益超过子查询重复执行的开销，则保留重复子查询
 *    (3) 否则移除子查询.
 */
object PartitionPruning extends Rule[LogicalPlan] with PredicateHelper with JoinSelectionHelper {

  /**
   * Searches for a table scan that can be filtered for a given column in a logical plan.
   *
   * This methods tries to find either a v1 or Hive serde partitioned scan for a given
   * partition column or a v2 scan that support runtime filtering on a given attribute.
   */
  def getFilterableTableScan(a: Expression, plan: LogicalPlan): Option[LogicalPlan] = {
    val srcInfo: Option[(Expression, LogicalPlan)] = findExpressionAndTrackLineageDown(a, plan)
    srcInfo.flatMap {
      case (resExp, l: LogicalRelation) =>
        l.relation match {
          case fs: HadoopFsRelation =>
            val partitionColumns = AttributeSet(
              l.resolve(fs.partitionSchema, fs.sparkSession.sessionState.analyzer.resolver))
            if (resExp.references.subsetOf(partitionColumns)) {
              return Some(l)
            } else {
              None
            }
          case _ => None
        }
      case (resExp, l: HiveTableRelation) =>
        if (resExp.references.subsetOf(AttributeSet(l.partitionCols))) {
          return Some(l)
        } else {
          None
        }
      case (resExp, r @ DataSourceV2ScanRelation(_, scan: SupportsRuntimeFiltering, _, _)) =>
        val filterAttrs = V2ExpressionUtils.resolveRefs[Attribute](scan.filterAttributes, r)
        if (resExp.references.subsetOf(AttributeSet(filterAttrs))) {
          Some(r)
        } else {
          None
        }
      case _ => None
    }
  }

  /**
   * Insert a dynamic partition pruning predicate on one side of the join using the filter on the
   * other side of the join.
   *  - to be able to identify this filter during query planning, we use a custom
   *    DynamicPruning expression that wraps a regular In expression
   *  - we also insert a flag that indicates if the subquery duplication is worthwhile and it
   *  should run regardless of the join strategy, or is too expensive and it should be run only if
   *  we can reuse the results of a broadcast
   */
  private def insertPredicate(
      pruningKey: Expression,
      pruningPlan: LogicalPlan,
      filteringKey: Expression,
      filteringPlan: LogicalPlan,
      joinKeys: Seq[Expression],
      partScan: LogicalPlan): LogicalPlan = {
    val reuseEnabled = conf.exchangeReuseEnabled
    val index = joinKeys.indexOf(filteringKey)
    lazy val hasBenefit = pruningHasBenefit(pruningKey, partScan, filteringKey, filteringPlan)
    if (reuseEnabled || hasBenefit) {
      // insert a DynamicPruning wrapper to identify the subquery during query planning
      Filter(
        DynamicPruningSubquery(
          pruningKey,
          filteringPlan,
          joinKeys,
          index,
          conf.dynamicPartitionPruningReuseBroadcastOnly || !hasBenefit),
        pruningPlan)
    } else {
      // abort dynamic partition pruning
      pruningPlan
    }
  }

  /**
   * Given an estimated filtering ratio we assume the partition pruning has benefit if
   * the size in bytes of the partitioned plan after filtering is greater than the size
   * in bytes of the plan on the other side of the join. We estimate the filtering ratio
   * using column statistics if they are available, otherwise we use the config value of
   * `spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio`.
   */
  private def pruningHasBenefit(
      partExpr: Expression,
      partPlan: LogicalPlan,
      otherExpr: Expression,
      otherPlan: LogicalPlan): Boolean = {

    // get the distinct counts of an attribute for a given table
    def distinctCounts(attr: Attribute, plan: LogicalPlan): Option[BigInt] = {
      plan.stats.attributeStats.get(attr).flatMap(_.distinctCount)
    }

    // the default filtering ratio when CBO stats are missing, but there is a
    // predicate that is likely to be selective
    val fallbackRatio = conf.dynamicPartitionPruningFallbackFilterRatio
    // the filtering ratio based on the type of the join condition and on the column statistics
    val filterRatio = (partExpr.references.toList, otherExpr.references.toList) match {
      // filter out expressions with more than one attribute on any side of the operator
      case (leftAttr :: Nil, rightAttr :: Nil)
        if conf.dynamicPartitionPruningUseStats =>
          // get the CBO stats for each attribute in the join condition
          val partDistinctCount = distinctCounts(leftAttr, partPlan)
          val otherDistinctCount = distinctCounts(rightAttr, otherPlan)
          val availableStats = partDistinctCount.isDefined && partDistinctCount.get > 0 &&
            otherDistinctCount.isDefined
          if (!availableStats) {
            fallbackRatio
          } else if (partDistinctCount.get.toDouble <= otherDistinctCount.get.toDouble) {
            // there is likely an estimation error, so we fallback
            fallbackRatio
          } else {
            1 - otherDistinctCount.get.toDouble / partDistinctCount.get.toDouble
          }
      case _ => fallbackRatio
    }

    val estimatePruningSideSize = filterRatio * partPlan.stats.sizeInBytes.toFloat
    val overhead = calculatePlanOverhead(otherPlan)
    estimatePruningSideSize > overhead
  }

  /**
   * Calculates a heuristic overhead of a logical plan. Normally it returns the total
   * size in bytes of all scan relations. We don't count in-memory relation which uses
   * only memory.
   */
  private def calculatePlanOverhead(plan: LogicalPlan): Float = {
    val (cached, notCached) = plan.collectLeaves().partition(p => p match {
      case _: InMemoryRelation => true
      case _ => false
    })
    val scanOverhead = notCached.map(_.stats.sizeInBytes).sum.toFloat
    val cachedOverhead = cached.map {
      case m: InMemoryRelation if m.cacheBuilder.storageLevel.useDisk &&
          !m.cacheBuilder.storageLevel.useMemory =>
        m.stats.sizeInBytes.toFloat
      case m: InMemoryRelation if m.cacheBuilder.storageLevel.useDisk =>
        m.stats.sizeInBytes.toFloat * 0.2
      case m: InMemoryRelation if m.cacheBuilder.storageLevel.useMemory =>
        0.0
    }.sum.toFloat
    scanOverhead + cachedOverhead
  }


  /**
   * Search a filtering predicate in a given logical plan
   */
  private def hasSelectivePredicate(plan: LogicalPlan): Boolean = {
    plan.exists {
      case f: Filter => isLikelySelective(f.condition)
      case _ => false
    }
  }

  /**
   * To be able to prune partitions on a join key, the filtering side needs to
   * meet the following requirements:
   *   (1) it can not be a stream
   *   (2) it needs to contain a selective predicate used for filtering
   */
  private def hasPartitionPruningFilter(plan: LogicalPlan): Boolean = {
    !plan.isStreaming && hasSelectivePredicate(plan)
  }

  private def prune(plan: LogicalPlan): LogicalPlan = {
    plan transformUp {
      // skip this rule if there's already a DPP subquery on the LHS of a join
      case j @ Join(Filter(_: DynamicPruningSubquery, _), _, _, _, _) => j
      case j @ Join(_, Filter(_: DynamicPruningSubquery, _), _, _, _) => j
      case j @ Join(left, right, joinType, Some(condition), hint) =>
        var newLeft = left
        var newRight = right

        // extract the left and right keys of the join condition
        val (leftKeys, rightKeys) = j match {
          case ExtractEquiJoinKeys(_, lkeys, rkeys, _, _, _, _, _) => (lkeys, rkeys)
          case _ => (Nil, Nil)
        }

        // checks if two expressions are on opposite sides of the join
        def fromDifferentSides(x: Expression, y: Expression): Boolean = {
          def fromLeftRight(x: Expression, y: Expression) =
            !x.references.isEmpty && x.references.subsetOf(left.outputSet) &&
              !y.references.isEmpty && y.references.subsetOf(right.outputSet)
          fromLeftRight(x, y) || fromLeftRight(y, x)
        }

        splitConjunctivePredicates(condition).foreach {
          case EqualTo(a: Expression, b: Expression)
              if fromDifferentSides(a, b) =>
            val (l, r) = if (a.references.subsetOf(left.outputSet) &&
              b.references.subsetOf(right.outputSet)) {
              a -> b
            } else {
              b -> a
            }

            // there should be a partitioned table and a filter on the dimension table,
            // otherwise the pruning will not trigger
            var filterableScan = getFilterableTableScan(l, left)
            if (filterableScan.isDefined && canPruneLeft(joinType) &&
                hasPartitionPruningFilter(right)) {
              newLeft = insertPredicate(l, newLeft, r, right, rightKeys, filterableScan.get)
            } else {
              filterableScan = getFilterableTableScan(r, right)
              if (filterableScan.isDefined && canPruneRight(joinType) &&
                  hasPartitionPruningFilter(left) ) {
                newRight = insertPredicate(r, newRight, l, left, leftKeys, filterableScan.get)
              }
            }
          case _ =>
        }
        Join(newLeft, newRight, joinType, Some(condition), hint)
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // Do not rewrite subqueries.
    case s: Subquery if s.correlated => plan
    case _ if !conf.dynamicPartitionPruningEnabled => plan
    case _ => prune(plan)
  }
}
