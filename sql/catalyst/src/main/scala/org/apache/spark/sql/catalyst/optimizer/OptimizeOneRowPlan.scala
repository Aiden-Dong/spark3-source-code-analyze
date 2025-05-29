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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.trees.TreePattern._

/**
 * 该规则同时应用于常规优化器和AQE优化器。它基于最大行数对执行计划进行优化：
 * - 若排序算子子节点的最大行数 ≤ 1，则消除排序操作
 * - 若本地排序算子每个分区的最大行数 ≤ 1，则消除本地排序
 * - 若聚合算子子节点的最大行数 ≤ 1 且该子节点仅包含分组操作（包含重写后的去重计划），则将聚合转换为投影
 * - 若聚合算子子节点的最大行数 ≤ 1，则将聚合表达式中的distinct标志置为false
 */
object OptimizeOneRowPlan extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformUpWithPruning(_.containsAnyPattern(SORT, AGGREGATE), ruleId) {
      case Sort(_, _, child) if child.maxRows.exists(_ <= 1L) => child
      case Sort(_, false, child) if child.maxRowsPerPartition.exists(_ <= 1L) => child
      case agg @ Aggregate(_, _, child) if agg.groupOnly && child.maxRows.exists(_ <= 1L) =>
        Project(agg.aggregateExpressions, child)
      case agg: Aggregate if agg.child.maxRows.exists(_ <= 1L) =>
        agg.transformExpressions {
          case aggExpr: AggregateExpression if aggExpr.isDistinct =>
            aggExpr.copy(isDistinct = false)
        }
    }
  }
}
