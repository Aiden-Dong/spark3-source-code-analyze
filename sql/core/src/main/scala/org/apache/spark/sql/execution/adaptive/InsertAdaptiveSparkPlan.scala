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

package org.apache.spark.sql.execution.adaptive

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{ListQuery, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.internal.SQLConf

/**
 * 此规则将查询计划包装在 [[AdaptiveSparkPlanExec]] 中，该执行计划根据运行时数据统计在执行过程中重新优化计划。
 *
 * 注意，此规则是有状态的，因此不应在多次查询执行中重用。
 */
case class InsertAdaptiveSparkPlan(
    adaptiveExecutionContext: AdaptiveExecutionContext) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = applyInternal(plan, false)

  private def applyInternal(plan: SparkPlan, isSubquery: Boolean): SparkPlan = plan match {
    case _ if !conf.adaptiveExecutionEnabled => plan
    case _: ExecutedCommandExec => plan
    case _: CommandResultExec => plan
    case c: DataWritingCommandExec => c.copy(child = apply(c.child))
    case c: V2CommandExec => c.withNewChildren(c.children.map(apply))
    case _ if shouldApplyAQE(plan, isSubquery) =>  // AQE 优化
      if (supportAdaptive(plan)) {
        try {
          // 递归规划子查询，并传入共享的阶段缓存以便exchange重用.
          // 如果任何子查询不支持 AQE，则回退到非 AQE 模式。
          val subqueryMap = buildSubqueryMap(plan)   // 子查询处理
          val planSubqueriesRule = PlanAdaptiveSubqueries(subqueryMap)
          val preprocessingRules = Seq(
            planSubqueriesRule)
          // Run pre-processing rules.
          val newPlan = AdaptiveSparkPlanExec.applyPhysicalRules(plan, preprocessingRules)
          logDebug(s"Adaptive execution enabled for plan: $plan")
          AdaptiveSparkPlanExec(newPlan, adaptiveExecutionContext, preprocessingRules, isSubquery)
        } catch {
          case SubqueryAdaptiveNotSupportedException(subquery) =>
            logWarning(s"${SQLConf.ADAPTIVE_EXECUTION_ENABLED.key} is enabled " +
              s"but is not supported for sub-query: $subquery.")
            plan
        }
      } else {
        logDebug(s"${SQLConf.ADAPTIVE_EXECUTION_ENABLED.key} is enabled " +
          s"but is not supported for query: $plan.")
        plan
      }

    case _ => plan
  }

  // AQE 仅在查询包含交换或子查询时才有用。如果满足以下任一条件，则此方法返回 true：
  //   - 配置 ADAPTIVE_EXECUTION_FORCE_APPLY 为 true。
  //   - 输入查询来自子查询。当发生这种情况时，意味着我们已经决定对主查询应用 AQE，并且必须继续执行。
  //   - 查询包含交换。
  //   - 查询可能需要添加交换。运行 `EnsureRequirements` 在这里过于复杂，因此我们只检查 `SparkPlan.requiredChildDistribution`，
  //     看是否有可能查询稍后需要添加交换。
  //   - 查询包含子查询。
  private def shouldApplyAQE(plan: SparkPlan, isSubquery: Boolean): Boolean = {
    conf.getConf(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY) || isSubquery || {
      plan.exists {
        case _: Exchange => true
        case p if !p.requiredChildDistribution.forall(_ == UnspecifiedDistribution) => true
        case p => p.expressions.exists(_.exists {
          case _: SubqueryExpression => true
          case _ => false
        })
      }
    }
  }

  private def supportAdaptive(plan: SparkPlan): Boolean = {
    sanityCheck(plan) &&
      !plan.logicalLink.exists(_.isStreaming) &&
    plan.children.forall(supportAdaptive)
  }

  private def sanityCheck(plan: SparkPlan): Boolean =
    plan.logicalLink.isDefined

  /**
   * 返回所有子查询的表达式 ID 到执行计划的映射。
   * 对于每个子查询，通过应用此规则生成自适应执行计划，或者在可能的情况下重用具有相同语义的其他子查询的执行计划。
   */
  private def buildSubqueryMap(plan: SparkPlan): Map[Long, BaseSubqueryExec] = {
    val subqueryMap = mutable.HashMap.empty[Long, BaseSubqueryExec]
    if (!plan.containsAnyPattern(SCALAR_SUBQUERY, IN_SUBQUERY, DYNAMIC_PRUNING_SUBQUERY)) {
      return subqueryMap.toMap
    }
    plan.foreach(_.expressions.filter(_.containsPattern(PLAN_EXPRESSION)).foreach(_.foreach {
      case expressions.ScalarSubquery(p, _, exprId, _)    //
          if !subqueryMap.contains(exprId.id) =>
        val executedPlan = compileSubquery(p)
        verifyAdaptivePlan(executedPlan, p)
        val subquery = SubqueryExec.createForScalarSubquery(
          s"subquery#${exprId.id}", executedPlan)
        subqueryMap.put(exprId.id, subquery)
      case expressions.InSubquery(_, ListQuery(query, _, exprId, _, _))
          if !subqueryMap.contains(exprId.id) =>
        val executedPlan = compileSubquery(query)
        verifyAdaptivePlan(executedPlan, query)
        val subquery = SubqueryExec(s"subquery#${exprId.id}", executedPlan)
        subqueryMap.put(exprId.id, subquery)
      case expressions.DynamicPruningSubquery(value, buildPlan,
          buildKeys, broadcastKeyIndex, onlyInBroadcast, exprId)
          if !subqueryMap.contains(exprId.id) =>
        val executedPlan = compileSubquery(buildPlan)
        verifyAdaptivePlan(executedPlan, buildPlan)

        val name = s"dynamicpruning#${exprId.id}"
        val subquery = SubqueryAdaptiveBroadcastExec(
          name, broadcastKeyIndex, onlyInBroadcast,
          buildPlan, buildKeys, executedPlan)
        subqueryMap.put(exprId.id, subquery)
      case _ =>
    }))

    subqueryMap.toMap
  }

  def compileSubquery(plan: LogicalPlan): SparkPlan = {
    // Apply the same instance of this rule to sub-queries so that sub-queries all share the
    // same `stageCache` for Exchange reuse.
    this.applyInternal(
      QueryExecution.createSparkPlan(adaptiveExecutionContext.session,
        adaptiveExecutionContext.session.sessionState.planner, plan.clone()), true)
  }

  private def verifyAdaptivePlan(plan: SparkPlan, logicalPlan: LogicalPlan): Unit = {
    if (!plan.isInstanceOf[AdaptiveSparkPlanExec]) {
      throw SubqueryAdaptiveNotSupportedException(logicalPlan)
    }
  }
}

private case class SubqueryAdaptiveNotSupportedException(plan: LogicalPlan) extends Exception {}
