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

package org.apache.spark.sql.catalyst.planning

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNode

/**
 * Given a [[LogicalPlan]], returns a list of `PhysicalPlan`s that can
 * be used for execution. If this strategy does not apply to the given logical operation then an
 * empty list should be returned.
 */
abstract class GenericStrategy[PhysicalPlan <: TreeNode[PhysicalPlan]] extends Logging {

  /**
   * Returns a placeholder for a physical plan that executes `plan`. This placeholder will be
   * filled in automatically by the QueryPlanner using the other execution strategies that are
   * available.
   */
  protected def planLater(plan: LogicalPlan): PhysicalPlan

  def apply(plan: LogicalPlan): Seq[PhysicalPlan]
}
/**
 * 用于将 [[LogicalPlan]] 转换为物理计划的抽象类。
 * 子类负责指定一组 [[GenericStrategy]] 对象，每个对象可以返回一组可能的物理计划选项。
 * 如果某个策略无法规划树中所有剩余的操作符，
 * 它可以调用 [[GenericStrategy#planLater planLater]]，该方法返回一个占位符
 * 对象，该对象将被 [[collectPlaceholders 收集]] 并使用其他可用策略进行填充。
 *
 * TODO：目前只返回一个计划...
 *       计划空间探索将在后续实现。
 *
 * @tparam PhysicalPlan 由此 [[QueryPlanner]] 生成的物理计划类型
 */
abstract class QueryPlanner[PhysicalPlan <: TreeNode[PhysicalPlan]] {
  /** A list of execution strategies that can be used by the planner */
  def strategies: Seq[GenericStrategy[PhysicalPlan]]

  def plan(plan: LogicalPlan): Iterator[PhysicalPlan] = {
    // 显然，这里还有很多工作要做...

    // 收集物理计划候选方案。
    val candidates = strategies.iterator.flatMap(_(plan))

    // 候选方案可能包含标记为 [[planLater]] 的占位符，
    // 因此尝试通过它们的子计划替换这些占位符。
    val plans = candidates.flatMap { candidate =>
      val placeholders = collectPlaceholders(candidate)  // 收集占位符

      if (placeholders.isEmpty) {
        // Take the candidate as is because it does not contain placeholders.
        Iterator(candidate)
      } else {
        // Plan the logical plan marked as [[planLater]] and replace the placeholders.
        placeholders.iterator.foldLeft(Iterator(candidate)) {
          case (candidatesWithPlaceholders, (placeholder, logicalPlan)) =>
            // Plan the logical plan for the placeholder.
            val childPlans = this.plan(logicalPlan)  // 对于占位符重新计算物理计划

            candidatesWithPlaceholders.flatMap { candidateWithPlaceholders =>
              childPlans.map { childPlan =>
                // Replace the placeholder by the child plan
                candidateWithPlaceholders.transformUp {
                  case p if p.eq(placeholder) => childPlan   // 替换占位符计划
                }
              }
            }
        }
      }
    }

    val pruned = prunePlans(plans)
    assert(pruned.hasNext, s"No plan for $plan")
    pruned
  }

  /**
   * Collects placeholders marked using [[GenericStrategy#planLater planLater]]
   * by [[strategies]].
   */
  protected def collectPlaceholders(plan: PhysicalPlan): Seq[(PhysicalPlan, LogicalPlan)]

  /** Prunes bad plans to prevent combinatorial explosion. */
  protected def prunePlans(plans: Iterator[PhysicalPlan]): Iterator[PhysicalPlan]
}
