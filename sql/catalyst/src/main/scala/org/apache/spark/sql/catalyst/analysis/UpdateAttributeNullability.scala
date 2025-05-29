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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.AlwaysProcess

/**
 * 通过使用子节点输出属性中对应属性的可空性，更新已解析LogicalPlan中属性的可空性。
 * 此步骤是必要的，因为用户可以在Dataset API中使用已解析的AttributeReference，
 * 而外连接操作可能会改变AttributeReference的可空性。
 * 若没有此规则，原本可为空的列可能被错误标记为不可为空，从而导致非法优化（如NULL传播）
 * 并产生错误结果。
 * 具体案例请参阅SPARK-13484和SPARK-13801中的查询示例。
 */
object UpdateAttributeNullability extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    AlwaysProcess.fn, ruleId) {
    // Skip unresolved nodes.
    case p if !p.resolved => p
    // Skip leaf node, as it has no child and no need to update nullability.
    case p: LeafNode => p
    case p: LogicalPlan if p.childrenResolved =>
      val nullabilities = p.children.flatMap(c => c.output).groupBy(_.exprId).map {
        // If there are multiple Attributes having the same ExprId, we need to resolve
        // the conflict of nullable field. We do not really expect this to happen.
        case (exprId, attributes) => exprId -> attributes.exists(_.nullable)
      }
      // For an Attribute used by the current LogicalPlan, if it is from its children,
      // we fix the nullable field by using the nullability setting of the corresponding
      // output Attribute from the children.
      p.transformExpressions {
        case attr: Attribute if nullabilities.contains(attr.exprId) =>
          attr.withNullability(nullabilities(attr.exprId))
      }
  }
}
