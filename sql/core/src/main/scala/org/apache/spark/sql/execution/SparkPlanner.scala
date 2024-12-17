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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.adaptive.LogicalQueryStageStrategy
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, FileSourceStrategy}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy

class SparkPlanner(val session: SparkSession, val experimentalMethods: ExperimentalMethods)
  extends SparkStrategies with SQLConfHelper {

  def numPartitions: Int = conf.numShufflePartitions

  override def strategies: Seq[Strategy] =
    experimentalMethods.extraStrategies ++
      extraPlanningStrategies ++ (
      LogicalQueryStageStrategy ::
      PythonEvals ::
      new DataSourceV2Strategy(session) ::
      FileSourceStrategy ::
      DataSourceStrategy ::
      SpecialLimits ::
      Aggregation ::
      Window ::
      JoinSelection ::
      InMemoryScans ::
      SparkScripts ::
      BasicOperators :: Nil)

  /**
   * Override to add extra planning strategies to the planner. These strategies are tried after
   * the strategies defined in [[ExperimentalMethods]], and before the regular strategies.
   */
  def extraPlanningStrategies: Seq[Strategy] = Nil

  override protected def collectPlaceholders(plan: SparkPlan): Seq[(SparkPlan, LogicalPlan)] = {
    plan.collect {
      case placeholder @ PlanLater(logicalPlan) => placeholder -> logicalPlan
    }
  }

  override protected def prunePlans(plans: Iterator[SparkPlan]): Iterator[SparkPlan] = {
    // TODO: We will need to prune bad plans when we improve plan space exploration
    //       to prevent combinatorial explosion.
    plans
  }

  /**
   * 用于构建表扫描操作符，其中复杂的投影和过滤操作通过独立的物理操作符完成。
   * 该函数仅在需要时返回带有 Project 和 Filter 节点的给定扫描操作符。例如，只有当最终所需输出
   * 需要评估复杂表达式或在过滤完成后可以进一步消除列时，才会使用 Project 操作符。
   *
   * `prunePushedDownFilters` 参数用于移除那些可以通过过滤器下推优化被优化掉的过滤器。
   *
   * 过滤和表达式评估所需的属性被传递给提供的 `scanBuilder` 函数，以便它避免不必要的列物化。
   */
  def pruneFilterProject(
      projectList: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      prunePushedDownFilters: Seq[Expression] => Seq[Expression],
      scanBuilder: Seq[Attribute] => SparkPlan): SparkPlan = {

    val projectSet = AttributeSet(projectList.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
    val filterCondition: Option[Expression] =
      prunePushedDownFilters(filterPredicates).reduceLeftOption(catalyst.expressions.And)

    // Right now we still use a projection even if the only evaluation is applying an alias
    // to a column.  Since this is a no-op, it could be avoided. However, using this
    // optimization with the current implementation would change the output schema.
    // TODO: Decouple final output schema from expression evaluation so this copy can be
    // avoided safely.

    if (AttributeSet(projectList.map(_.toAttribute)) == projectSet &&
        filterSet.subsetOf(projectSet)) {
      // When it is possible to just use column pruning to get the right projection and
      // when the columns of this projection are enough to evaluate all filter conditions,
      // just do a scan followed by a filter, with no extra project.
      val scan = scanBuilder(projectList.asInstanceOf[Seq[Attribute]])
      filterCondition.map(FilterExec(_, scan)).getOrElse(scan)
    } else {
      val scan = scanBuilder((projectSet ++ filterSet).toSeq)
      ProjectExec(projectList, filterCondition.map(FilterExec(_, scan)).getOrElse(scan))
    }
  }
}
