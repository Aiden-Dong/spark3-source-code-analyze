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

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, Final, Partial}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * 如果满足以下条件，将在 Spark 计划中用排序聚合替换基于哈希的聚合：
 *
 * 1. 计划是一个部分和最终 [[HashAggregateExec]] 或 [[ObjectHashAggregateExec]] 的组合，
 *    并且部分聚合的子节点满足对应 [[SortAggregateExec]] 的排序顺序。
 * 或者
 * 2. 计划是一个 [[HashAggregateExec]] 或 [[ObjectHashAggregateExec]]，
 *    并且子节点满足对应 [[SortAggregateExec]] 的排序顺序。
 *
 * 示例：
 * 1. 连接后的聚合：
 *
 *  HashAggregate(t1.i, SUM, final)
 *               |                         SortAggregate(t1.i, SUM, complete)
 * HashAggregate(t1.i, SUM, partial)   =>                |
 *               |                            SortMergeJoin(t1.i = t2.j)
 *    SortMergeJoin(t1.i = t2.j)
 *
 * 2. 排序后的聚合：
 *
 * HashAggregate(t1.i, SUM, partial)         SortAggregate(t1.i, SUM, partial)
 *               |                     =>                  |
 *           Sort(t1.i)                                Sort(t1.i)
 *
 * 当基于哈希的聚合的子节点满足对应排序聚合的排序顺序时，可以用排序聚合替换哈希聚合。
 * 排序聚合在某种意义上更快，因为它没有哈希聚合的哈希开销。
 */
object ReplaceHashWithSortAgg extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.REPLACE_HASH_WITH_SORT_AGG_ENABLED)) {
      plan
    } else {
      replaceHashAgg(plan)
    }
  }

  /**
   * Replace [[HashAggregateExec]] and [[ObjectHashAggregateExec]] with [[SortAggregateExec]].
   */
  private def replaceHashAgg(plan: SparkPlan): SparkPlan = {
    plan.transformDown {
      case hashAgg: BaseAggregateExec if isHashBasedAggWithKeys(hashAgg) =>
        val sortAgg = hashAgg.toSortAggregate
        hashAgg.child match {
          case partialAgg: BaseAggregateExec
            if isHashBasedAggWithKeys(partialAgg) && isPartialAgg(partialAgg, hashAgg) =>
            if (SortOrder.orderingSatisfies(
                partialAgg.child.outputOrdering, sortAgg.requiredChildOrdering.head)) {
              sortAgg.copy(
                aggregateExpressions = sortAgg.aggregateExpressions.map(_.copy(mode = Complete)),
                child = partialAgg.child)
            } else {
              hashAgg
            }
          case other =>
            if (SortOrder.orderingSatisfies(
                other.outputOrdering, sortAgg.requiredChildOrdering.head)) {
              sortAgg
            } else {
              hashAgg
            }
        }
      case other => other
    }
  }

  /**
   * Check if `partialAgg` to be partial aggregate of `finalAgg`.
   */
  private def isPartialAgg(partialAgg: BaseAggregateExec, finalAgg: BaseAggregateExec): Boolean = {
    if (partialAgg.aggregateExpressions.forall(_.mode == Partial) &&
        finalAgg.aggregateExpressions.forall(_.mode == Final)) {
      (finalAgg.logicalLink, partialAgg.logicalLink) match {
        case (Some(agg1), Some(agg2)) => agg1.sameResult(agg2)
        case _ => false
      }
    } else {
      false
    }
  }

  /**
   * Check if `agg` is [[HashAggregateExec]] or [[ObjectHashAggregateExec]],
   * and has grouping keys.
   */
  private def isHashBasedAggWithKeys(agg: BaseAggregateExec): Boolean = {
    val isHashBasedAgg = agg match {
      case _: HashAggregateExec | _: ObjectHashAggregateExec => true
      case _ => false
    }
    isHashBasedAgg && agg.groupingExpressions.nonEmpty
  }
}
