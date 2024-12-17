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

package org.apache.spark.sql.execution.bucketing

import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.Exchange


/***
 * 根据实际的物理查询计划禁用不必要的分桶表扫描。
 * 注意：此规则设计为在 [[EnsureRequirements]] 之后应用，在此阶段所有 [[ShuffleExchangeExec]] 和
 * [[SortExec]] 已正确添加到计划中。
 *
 * 当 BUCKETING_ENABLED 和 AUTO_BUCKETED_SCAN_ENABLED 设置为 true 时，遍历查询计划以检查在哪些地方不需要分桶表扫描，
 * 并在以下情况下禁用分桶表扫描：
 *
 * 1. 从根节点到分桶表扫描的子计划不包含 [[hasInterestingPartition]] 操作符。
 *
 * 2. 从最近的下游 [[hasInterestingPartition]] 操作符到分桶表扫描的子计划仅包含 [[isAllowedUnaryExecNode]] 操作符，
 *    且至少包含一个 [[Exchange]]。
 *
 * 示例：
 * 1. 没有 [[hasInterestingPartition]] 操作符：
 *                Project
 *                   |
 *                 Filter
 *                   |
 *             Scan(t1: i, j)
 *  （按列 j 分桶，禁用分桶扫描）
 *
 * 2. 连接：
 *         SortMergeJoin(t1.i = t2.j)
 *            /            \
 *        Sort(i)        Sort(j)
 *          /               \
 *      Shuffle(i)       Scan(t2: i, j)
 *        /         （按列 j 分桶，启用分桶扫描）
 *   Scan(t1: i, j)
 * （按列 j 分桶，禁用分桶扫描）
 *
 * 3. 聚合：
 *         HashAggregate(i, ..., Final)
 *                      |
 *                  Shuffle(i)
 *                      |
 *         HashAggregate(i, ..., Partial)
 *                      |
 *                    Filter
 *                      |
 *                  Scan(t1: i, j)
 *  （按列 j 分桶，禁用分桶扫描）
 *
 * [[hasInterestingPartition]] 的思想来源于论文 "Access Path Selection in a Relational Database Management System"
 * 中的 "interesting order"（https://dl.acm.org/doi/10.1145/582095.582099）。
 **/

object DisableUnnecessaryBucketedScan extends Rule[SparkPlan] {

  /**
   * Disable bucketed table scan with pre-order traversal of plan.
   *
   * @param withInterestingPartition The traversed plan has operator with interesting partition.
   * @param withExchange The traversed plan has [[Exchange]] operator.
   * @param withAllowedNode The traversed plan has only [[isAllowedUnaryExecNode]] operators.
   */
  private def disableBucketWithInterestingPartition(
      plan: SparkPlan,
      withInterestingPartition: Boolean,
      withExchange: Boolean,
      withAllowedNode: Boolean): SparkPlan = {
    plan match {
      case p if hasInterestingPartition(p) =>
        // Operator with interesting partition, propagates `withInterestingPartition` as true
        // to its children, and resets `withExchange` and `withAllowedNode`.
        p.mapChildren(disableBucketWithInterestingPartition(_, true, false, true))
      case exchange: Exchange =>
        // Exchange operator propagates `withExchange` as true to its child.
        exchange.mapChildren(disableBucketWithInterestingPartition(
          _, withInterestingPartition, true, withAllowedNode))
      case scan: FileSourceScanExec =>
        if (scan.bucketedScan) {
          if (!withInterestingPartition || (withExchange && withAllowedNode)) {
            val nonBucketedScan = scan.copy(disableBucketedScan = true)
            scan.logicalLink.foreach(nonBucketedScan.setLogicalLink)
            nonBucketedScan
          } else {
            scan
          }
        } else {
          scan
        }
      case o =>
        o.mapChildren(disableBucketWithInterestingPartition(
          _,
          withInterestingPartition,
          withExchange,
          withAllowedNode && isAllowedUnaryExecNode(o)))
    }
  }

  private def hasInterestingPartition(plan: SparkPlan): Boolean = {
    plan.requiredChildDistribution.exists {
      case _: ClusteredDistribution | AllTuples => true
      case _ => false
    }
  }

  /**
   * Check if the operator is allowed single-child operator.
   * We may revisit this method later as we probably can
   * remove this restriction to allow arbitrary operator between
   * bucketed table scan and operator with interesting partition.
   */
  private def isAllowedUnaryExecNode(plan: SparkPlan): Boolean = {
    plan match {
      case _: SortExec | _: ProjectExec | _: FilterExec => true
      case partialAgg: BaseAggregateExec =>
        partialAgg.requiredChildDistributionExpressions.isEmpty
      case _ => false
    }
  }

  def apply(plan: SparkPlan): SparkPlan = {
    lazy val hasBucketedScan = plan.exists {
      case scan: FileSourceScanExec => scan.bucketedScan
      case _ => false
    }

    if (!conf.bucketingEnabled || !conf.autoBucketedScanEnabled || !hasBucketedScan) {
      plan
    } else {
      disableBucketWithInterestingPartition(plan, false, false, true)
    }
  }
}
