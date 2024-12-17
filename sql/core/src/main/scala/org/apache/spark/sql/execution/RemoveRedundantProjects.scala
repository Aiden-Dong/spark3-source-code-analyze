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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.aggregate.{Final, PartialMerge}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExecBase
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf

/**
 * 从 Spark 计划中移除冗余的 ProjectExec 节点。当以下条件满足时，ProjectExec 节点是冗余的：
 * - 它的输出属性和顺序与其子节点的输出相同，并且属性的顺序是必要的。
 * - 当属性输出顺序不是必需时，它的输出属性与其子节点的输出相同。
 * 这个规则需要是一个物理规则，因为在逻辑优化过程中，Project 节点有助于修剪数据。
 * 在物理规划阶段，可以移除冗余的 Project 节点以简化查询计划。
 */
object RemoveRedundantProjects extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.REMOVE_REDUNDANT_PROJECTS_ENABLED)) {
      plan
    } else {
      removeProject(plan, true)
    }
  }

  private def removeProject(plan: SparkPlan, requireOrdering: Boolean): SparkPlan = {
    plan match {
      case p @ ProjectExec(_, child) =>
        if (isRedundant(p, child, requireOrdering) && canRemove(p, child)) {
          val newPlan = removeProject(child, requireOrdering)
          // The `newPlan` should retain the logical plan link already. We call `setLogicalLink`
          // here to make sure the `newPlan` sets the `LOGICAL_PLAN_TAG` tag.
          newPlan.setLogicalLink(child.logicalLink.get)
          newPlan
        } else {
          p.mapChildren(removeProject(_, false))
        }
      case op: TakeOrderedAndProjectExec =>
        op.mapChildren(removeProject(_, false))
      case a: BaseAggregateExec =>
        // BaseAggregateExec require specific column ordering when mode is Final or PartialMerge.
        // See comments in BaseAggregateExec inputAttributes method.
        val keepOrdering = a.aggregateExpressions
          .exists(ae => ae.mode.equals(Final) || ae.mode.equals(PartialMerge))
        a.mapChildren(removeProject(_, keepOrdering))
      case o =>
        val required = if (canPassThrough(o)) requireOrdering else true
        o.mapChildren(removeProject(_, requireOrdering = required))
    }
  }

  /**
   * Check if the given node can pass the ordering requirement from its parent.
   */
  private def canPassThrough(plan: SparkPlan): Boolean = plan match {
    case _: FilterExec => true
    // JoinExec ordering requirement should inherit from its parent. If there is no ProjectExec in
    // its ancestors, JoinExec should require output columns to be ordered, and vice versa.
    case _: BaseJoinExec => true
    case _: WindowExec => true
    case _: ExpandExec => true
    case _ => false
  }

  /**
   * Check if the nullability change is positive. It catches the case when the project output
   * attribute is not nullable, but the child output attribute is nullable.
   */
  private def checkNullability(output: Seq[Attribute], childOutput: Seq[Attribute]): Boolean =
    output.zip(childOutput).forall { case (attr1, attr2) => attr1.nullable || !attr2.nullable }

  private def isRedundant(
      project: ProjectExec,
      child: SparkPlan,
      requireOrdering: Boolean): Boolean = {
    child match {
      // If a DataSourceV2ScanExec node does not support columnar, a ProjectExec node is required
      // to convert the rows to UnsafeRow. See DataSourceV2Strategy for more details.
      case d: DataSourceV2ScanExecBase if !d.supportsColumnar => false
      case FilterExec(_, d: DataSourceV2ScanExecBase) if !d.supportsColumnar => false
      case _ =>
        if (requireOrdering) {
          project.output.map(_.exprId.id) == child.output.map(_.exprId.id) &&
            checkNullability(project.output, child.output)
        } else {
          val orderedProjectOutput = project.output.sortBy(_.exprId.id)
          val orderedChildOutput = child.output.sortBy(_.exprId.id)
          orderedProjectOutput.map(_.exprId.id) == orderedChildOutput.map(_.exprId.id) &&
            checkNullability(orderedProjectOutput, orderedChildOutput)
        }
    }
  }

  // SPARK-36020: Currently a project can only be removed if (1) its logical link is empty or (2)
  // its logical link is the same as the child's logical link. This is to ensure the physical
  // plan node can correctly map to its logical plan node in AQE.
  private def canRemove(project: ProjectExec, child: SparkPlan): Boolean = {
    project.logicalLink.isEmpty || project.logicalLink.exists(child.logicalLink.contains)
  }
}
