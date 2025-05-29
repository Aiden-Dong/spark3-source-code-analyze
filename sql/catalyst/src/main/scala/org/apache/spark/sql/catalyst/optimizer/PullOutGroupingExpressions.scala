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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE

/**
 * 本规则确保在优化阶段 [[Aggregate]] 节点不包含复杂分组表达式
 * 复杂分组表达式会被提取到[[Aggregate]]下的[[Project]]节点中，
 * 并在分组表达式和不含聚合函数的聚合表达式中被引用。
 * 这些引用能保证优化规则不会将聚合表达式错误地转换为不再引用任何分组表达式的无效形式，
 * 同时简化节点上的表达式转换（只需转换表达式一次）。
 * 示例：在以下查询中，Spark不应将聚合表达式Not(IsNull(c))优化为IsNotNull(c)，
 * 因为分组表达式是 IsNull(c):
 *     SELECT not(c IS NULL) FROM t GROUP BY c IS NULL
 * 正确的处理方式是让聚合表达式引用_groupingexpression属性：
 * Aggregate [_groupingexpression#233], [NOT _groupingexpression#233 AS (NOT (c IS NULL))#230]
 * +- Project [isnull(c#219) AS _groupingexpression#233]
 * +- LocalRelation [c#219]
 */
object PullOutGroupingExpressions extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformWithPruning(_.containsPattern(AGGREGATE)) {
      case a: Aggregate if a.resolved =>
        val complexGroupingExpressionMap = mutable.LinkedHashMap.empty[Expression, NamedExpression]
        val newGroupingExpressions = a.groupingExpressions.toIndexedSeq.map {
          case e if !e.foldable && e.children.nonEmpty =>
            complexGroupingExpressionMap
              .getOrElseUpdate(e.canonicalized, Alias(e, s"_groupingexpression")())
              .toAttribute
          case o => o
        }
        if (complexGroupingExpressionMap.nonEmpty) {
          def replaceComplexGroupingExpressions(e: Expression): Expression = {
            e match {
              case _ if AggregateExpression.isAggregate(e) => e
              case _ if e.foldable => e
              case _ if complexGroupingExpressionMap.contains(e.canonicalized) =>
                complexGroupingExpressionMap.get(e.canonicalized).map(_.toAttribute).getOrElse(e)
              case _ => e.mapChildren(replaceComplexGroupingExpressions)
            }
          }

          val newAggregateExpressions = a.aggregateExpressions
            .map(replaceComplexGroupingExpressions(_).asInstanceOf[NamedExpression])
          val newChild = Project(a.child.output ++ complexGroupingExpressionMap.values, a.child)
          Aggregate(newGroupingExpressions, newAggregateExpressions, newChild)
        } else {
          a
        }
    }
  }
}
