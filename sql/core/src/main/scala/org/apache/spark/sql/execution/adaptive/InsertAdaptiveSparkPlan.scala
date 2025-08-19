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
 * 1. InsertAdaptiveSparkPlan 规则插入 AdaptiveSparkPlanExec
 * 2. 将查询计划拆分为 QueryStage
 * 3. 异步执行各个阶段
 * 4. 收集运行时统计信息
 * 5. 基于统计信息重新优化剩余计划
 * 6. 重复步骤 2-5 直到所有阶段完成
 * 此规则将查询计划包装在 [[AdaptiveSparkPlanExec]] 中，该执行计划根据运行时数据统计在执行过程中重新优化计划。
 *
 * 注意，此规则是有状态的，因此不应在多次查询执行中重用。
 */
case class InsertAdaptiveSparkPlan(
    adaptiveExecutionContext: AdaptiveExecutionContext) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = applyInternal(plan, false)

  private def applyInternal(plan: SparkPlan, isSubquery: Boolean): SparkPlan = plan match {

    // 没有开启自适应
    case _ if !conf.adaptiveExecutionEnabled => plan
    // DDL/DML 命令（如 CREATE TABLE, DROP TABLE）
    case _: ExecutedCommandExec => plan
    case _: CommandResultExec => plan
    case c: DataWritingCommandExec => c.copy(child = apply(c.child))     // insert into 类型对其子查询进行执行优化
    case c: V2CommandExec => c.withNewChildren(c.children.map(apply))    // DataSource V2 API 的命令节点, 对所有子节点递归应用 AQE

    case _ if shouldApplyAQE(plan, isSubquery) =>  // AQE 优化
      if (supportAdaptive(plan)) {
        try {

          // 首先先校验检查子查询， 如果存在子查询先优化子查询
          val subqueryMap = buildSubqueryMap(plan)                           // 子查询处理
          val planSubqueriesRule = PlanAdaptiveSubqueries(subqueryMap)      // 步骤2：创建子查询规划规则
          val preprocessingRules = Seq(
            planSubqueriesRule)
          // Run pre-processing rules.
          // 利用子查询
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
  //   - 配置 spark.sql.adaptive.forceApply 为 true 无条件启用 AQE
  //   - 如果当前计划是子查询，必须启用 AQE
  //   - 查询包含 Exchange(ShuffleExchangeExec, BroadcastExchangeExec) 启动AQE。
  //   - 查询包含子查询。
  //
  //   -- 会触发 AQE（包含 Join，需要 Exchange）
  //   SELECT * FROM t1 JOIN t2 ON t1.id = t2.id
  //
  //   -- 会触发 AQE（包含子查询）
  //   SELECT * FROM t1 WHERE id IN (SELECT id FROM t2)
  //
  //   -- 可能不触发 AQE（简单查询）
  //   SELECT * FROM t1 WHERE col > 100
  //
  //   -- 会触发 AQE（包含聚合，可能需要 Exchange）
  //   SELECT col, COUNT(*) FROM t1 GROUP BY col
  private def shouldApplyAQE(plan: SparkPlan, isSubquery: Boolean): Boolean = {
    // spark.sql.adaptive.forceApply (如果设置为 true, 则无条件启用 AQE)
    conf.getConf(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY) || isSubquery || {
      plan.exists {
        case _: Exchange => true
        case p if !p.requiredChildDistribution.forall(_ == UnspecifiedDistribution) => true   // 潜在 Exchange 需求
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

  // 每个 SparkPlan 节点都应该能追溯到对应的 LogicalPlan
  private def sanityCheck(plan: SparkPlan): Boolean =
    plan.logicalLink.isDefined

  /**
   *  构建子查询映射表
   *
   * 返回所有子查询的表达式 ID 到执行计划的映射。
   * 对于每个子查询，通过应用此规则生成自适应执行计划，
   * 或者在可能的情况下重用具有相同语义的其他子查询的执行计划。
   *
   * 处理三种类型的子查询：
   * 1. ScalarSubquery：标量子查询（返回单个值）
   * 2. InSubquery：IN 子查询（用于 IN 条件）
   * 3. DynamicPruningSubquery：动态分区裁剪子查询（用于优化分区表查询）
   */
  private def buildSubqueryMap(plan: SparkPlan): Map[Long, BaseSubqueryExec] = {
    val subqueryMap = mutable.HashMap.empty[Long, BaseSubqueryExec]

    // 如果计划中不包含任何子查询模式，直接返回空映射
    if (!plan.containsAnyPattern(SCALAR_SUBQUERY, IN_SUBQUERY, DYNAMIC_PRUNING_SUBQUERY)) {
      return subqueryMap.toMap
    }

    // 遍历计划中的所有表达式，查找子查询
    plan.foreach(_.expressions.filter(_.containsPattern(PLAN_EXPRESSION)).foreach(_.foreach {

      // 处理标量子查询
      case expressions.ScalarSubquery(p, _, exprId, _)  if !subqueryMap.contains(exprId.id) =>  // 表示未经处理的标量子查询
        val executedPlan = compileSubquery(p)
        verifyAdaptivePlan(executedPlan, p)
        val subquery = SubqueryExec.createForScalarSubquery(s"subquery#${exprId.id}", executedPlan)
        subqueryMap.put(exprId.id, subquery)

      // 处理 IN 子查询
      case expressions.InSubquery(_, ListQuery(query, _, exprId, _, _))
          if !subqueryMap.contains(exprId.id) =>
        val executedPlan = compileSubquery(query)
        verifyAdaptivePlan(executedPlan, query)
        val subquery = SubqueryExec(s"subquery#${exprId.id}", executedPlan)
        subqueryMap.put(exprId.id, subquery)

      // 处理动态分区裁剪子查询
      case expressions.DynamicPruningSubquery(value, buildPlan, buildKeys, broadcastKeyIndex, onlyInBroadcast, exprId)
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

  /***
   * 编译子查询为执行计划
   * 对子查询应用相同的 AQE 规则实例，以便所有子查询共享相同的 `stageCache`  来实现 Exchange 重用优化。
   */
  def compileSubquery(plan: LogicalPlan): SparkPlan = {
    // 对子查询递归应用当前规则实例
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
