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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{Command, CTERelationDef, CTERelationRef, InsertIntoDir, LogicalPlan, ParsedStatement, SubqueryAlias, UnresolvedWith, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf.{LEGACY_CTE_PRECEDENCE_POLICY, LegacyBehaviorPolicy}

/**
 * 分析 WITH 节点，并根据以下条件用 CTE 引用或 CTE 定义替换子计划：
 *  1. 如果处于兼容模式（legacy mode），或者查询是一个 SQL 命令或 DML 语句，则用 CTE 定义替换，即内联 CTE；
 *  2. 否则，替换为 CTE 引用 CTERelationRef。是否内联的决定将在查询分析之后，由规则 InlineCTE 来做出。
 *
 * 所有在该替换过程中未被内联的 CTE 定义，将会被统一归入一个 WithCTE 节点中，无论是在主查询中，还是在子查询中。
 * 任何不包含 CTE，或其所有 CTE 均已被内联的主查询或子查询，显然将不会包含 WithCTE 节点。但如果有，它们的 WithCTE 节点会处于原来最外层 With 节点所在的位置。
 *
 * WithCTE 节点中的 CTE 定义将按照它们被解析的顺序保存。
 * 这意味着，对于任何合法的 CTE 查询，这些定义一定是按依赖关系的拓扑顺序排列的（例如，若有两个 CTE 定义 A 和 B，且 B 依赖于 A，则 A 一定出现在 B 之前）。
 * 否则，这将是一个非法的用户查询，稍后在解析关系时会抛出分析异常。
 */
object CTESubstitution extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.containsPattern(UNRESOLVED_WITH)) {
      return plan
    }

    val isCommand = plan.exists {   // insert 类型返回 true
      case _: Command | _: ParsedStatement | _: InsertIntoDir => true
      case _ => false
    }

    val cteDefs = ArrayBuffer.empty[CTERelationDef]
    val (substituted, firstSubstituted) =
      LegacyBehaviorPolicy.withName(conf.getConf(LEGACY_CTE_PRECEDENCE_POLICY)) match {
        case LegacyBehaviorPolicy.EXCEPTION =>
          assertNoNameConflictsInCTE(plan)
          traverseAndSubstituteCTE(plan, isCommand, Seq.empty, cteDefs)
        case LegacyBehaviorPolicy.LEGACY =>
          (legacyTraverseAndSubstituteCTE(plan, cteDefs), None)
        case LegacyBehaviorPolicy.CORRECTED =>
          traverseAndSubstituteCTE(plan, isCommand, Seq.empty, cteDefs)
    }
    if (cteDefs.isEmpty) {
      substituted
    } else if (substituted eq firstSubstituted.get) {
      WithCTE(substituted, cteDefs.toSeq)
    } else {
      var done = false
      substituted.resolveOperatorsWithPruning(_ => !done) {
        case p if p eq firstSubstituted.get =>
          // `firstSubstituted` 是所有其他 CTE 的父节点（如果有的话）。
          done = true
          WithCTE(p, cteDefs.toSeq)
        case p if p.children.count(_.containsPattern(CTE)) > 1 =>
          // 这是所有 CTE 的第一个公共父节点。
          done = true
          WithCTE(p, cteDefs.toSeq)
      }
    }
  }

  /**
   * Spark 3.0 改变了 CTE 关系的解析方式，内部关系优先。这是正确的，但在 EXCEPTION 模式下，
   * 当我们看到具有冲突名称的 CTE 关系时，需要警告用户这种行为变化。
   *
   * 注意，在 Spark 3.0 之前，解析器不支持 FROM 子句中的 CTE。例如
   * `WITH ... SELECT * FROM (WITH ... SELECT ...)` 不被支持。我们不应该为这种情况失败，
   *  因为 3.0 之前的 Spark 版本无论如何都无法运行它。参数 `startOfQuery` 用于指示在 Spark 3.0 之前我们可以定义 CTE 关系的位置，
   *  我们只应该在 `startOfQuery` 为 true 时检查名称冲突。
   */
  private def assertNoNameConflictsInCTE(
      plan: LogicalPlan,
      outerCTERelationNames: Seq[String] = Nil,
      startOfQuery: Boolean = true): Unit = {
    val resolver = conf.resolver
    plan match {
      case UnresolvedWith(child, relations) =>
        val newNames = ArrayBuffer.empty[String]
        newNames ++= outerCTERelationNames
        relations.foreach {
          case (name, relation) =>
            if (startOfQuery && outerCTERelationNames.exists(resolver(_, name))) {
              throw QueryCompilationErrors.ambiguousRelationAliasNameInNestedCTEError(name)
            }
            // CTE 关系定义为 `SubqueryAlias`。这里我们跳过它并直接检查子节点，
            // 以便正确设置 `startOfQuery`。
            assertNoNameConflictsInCTE(relation.child, newNames.toSeq)
            newNames += name
        }
        assertNoNameConflictsInCTE(child, newNames.toSeq, startOfQuery = false)

      case other =>
        other.subqueries.foreach(assertNoNameConflictsInCTE(_, outerCTERelationNames))
        other.children.foreach(
          assertNoNameConflictsInCTE(_, outerCTERelationNames, startOfQuery = false))
    }
  }

  private def legacyTraverseAndSubstituteCTE(
      plan: LogicalPlan,
      cteDefs: ArrayBuffer[CTERelationDef]): LogicalPlan = {
    plan.resolveOperatorsUp {
      case UnresolvedWith(child, relations) =>
        val resolvedCTERelations =
          resolveCTERelations(relations, isLegacy = true, isCommand = false, Seq.empty, cteDefs)
        substituteCTE(child, alwaysInline = true, resolvedCTERelations)
    }
  }

  /**
   * 遍历计划和表达式节点作为树，如果 `isCommand` 为 false，则用 CTE 引用替换匹配的引用，否则用相应 CTE 定义的查询计划替换。
   * - 如果规则遇到 WITH 节点，则用该节点的 CTE 定义从右到左的顺序替换该节点的子节点，
   *   因为定义可以引用前一个定义。
   *   例如，以下查询是有效的：
   *   WITH
   *     t AS (SELECT 1),
   *     t2 AS (SELECT * FROM t)
   *   SELECT * FROM t2
   * - 如果 CTE 定义包含内部 WITH 节点，则内部的替换应该优先，因为它可以遮蔽外部 CTE 定义。
   *   例如，以下查询应该返回 2：
   *   For example the following query should return 2:
   *   WITH
   *     t AS (SELECT 1),
   *     t2 AS (
   *       WITH t AS (SELECT 2)
   *       SELECT * FROM t
   *     )
   *   SELECT * FROM t2
   * - 如果 CTE 定义包含一个包含内部 WITH 节点的子查询，则内部的替换应该优先，
   *   因为它可以遮蔽外部 CTE 定义。
   *   例如，以下查询应该返回 2：
   *   WITH t AS (SELECT 1 AS c)
   *   SELECT max(c) FROM (
   *     WITH t AS (SELECT 2 AS c)
   *     SELECT * FROM t
   *   )
   * - 如果 CTE 定义包含一个包含内部 WITH 节点的子查询表达式，则内部的替换应该优先，
   *   因为它可以遮蔽外部 CTE 定义。
   *   例如，以下查询应该返回 2:
   *   WITH t AS (SELECT 1)
   *   SELECT (
   *     WITH t AS (SELECT 2)
   *     SELECT * FROM t
   *   )
   * @param plan 要遍历的计划
   * @param isCommand 是否为命令
   * @param outerCTEDefs 已解析的外部 CTE 定义及其名称
   * @param cteDefs 所有累积的 CTE 定义
   * @return 应用 CTE 替换的计划，以及可选的最后替换的 `With`，CTE 定义将被收集到该处
   */
  private def traverseAndSubstituteCTE(
      plan: LogicalPlan,                     // sh
      isCommand: Boolean,
      outerCTEDefs: Seq[(String, CTERelationDef)],
      cteDefs: ArrayBuffer[CTERelationDef]): (LogicalPlan, Option[LogicalPlan]) = {

    var firstSubstituted: Option[LogicalPlan] = None

    // 从最外层开始向内查找  with 语句
    val newPlan = plan.resolveOperatorsDownWithPruning(
        _.containsAnyPattern(UNRESOLVED_WITH, PLAN_EXPRESSION)) {

      case UnresolvedWith(child: LogicalPlan, relations) =>
        // 解决所有的 CTE - relation
        val resolvedCTERelations =
          resolveCTERelations(relations, isLegacy = false, isCommand, outerCTEDefs, cteDefs) ++
            outerCTEDefs

        val substituted = substituteCTE(
          traverseAndSubstituteCTE(child, isCommand, resolvedCTERelations, cteDefs)._1,   // 解决子节点的CTE 问题
          isCommand,
          resolvedCTERelations)
        if (firstSubstituted.isEmpty) {
          firstSubstituted = Some(substituted)
        }
        substituted

      case other =>
        other.transformExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
          case e: SubqueryExpression => e.withNewPlan(apply(e.plan))
        }
    }
    (newPlan, firstSubstituted)
  }

  private def resolveCTERelations(
      relations: Seq[(String, SubqueryAlias)],     // 当前
      isLegacy: Boolean,
      isCommand: Boolean,
      outerCTEDefs: Seq[(String, CTERelationDef)],
      cteDefs: ArrayBuffer[CTERelationDef]): Seq[(String, CTERelationDef)] = {
    var resolvedCTERelations = if (isLegacy || isCommand) {
      Seq.empty
    } else {
      outerCTEDefs
    }
    for ((name, relation) <- relations) {
      val innerCTEResolved = if (isLegacy) {
        // 在兼容模式下，外部 CTE 关系优先。这里我们不解析内部的 `With` 节点，
        // 稍后我们将用外部 CTE 关系替换 `UnresolvedRelation`。
        // 分析器将多次运行此规则，直到所有 `With` 节点都被解析。
        relation
      } else {
        // CTE 定义可能包含具有更高优先级的内部 CTE，因此首先遍历并替换 `relation` 中定义的 CTE。
        // 注意：我们必须在 `substituteCTE` 之前调用 `traverseAndSubstituteCTE`，因为在解析内部 CTE 关系时，
        // 内部 CTE 中的关系比外部 CTE 中的关系具有更高的优先级。例如：
        //
        // WITH t1 AS (SELECT 1)
        // t2 AS (
        //   WITH t1 AS (SELECT 2)
        //   WITH t3 AS (SELECT * FROM t1)
        // )
        // t3 应该将 t1 解析为 `SELECT 2` 而不是 `SELECT 1`。
        traverseAndSubstituteCTE(relation, isCommand, resolvedCTERelations, cteDefs)._1
      }
      // CTE 定义可以引用前一个定义
      val substituted = substituteCTE(innerCTEResolved, isLegacy || isCommand, resolvedCTERelations)
      val cteRelation = CTERelationDef(substituted)
      if (!(isLegacy || isCommand)) {
        cteDefs += cteRelation
      }
      // 前置新的 CTE 确保它们比外部的 CTE 具有更高的优先级。
      resolvedCTERelations +:= (name -> cteRelation)
    }
    resolvedCTERelations
  }

  // 这个方法是CTE替换的核心执行器，负责将UnresolvedRelation替换为具体的CTE实现
  private def substituteCTE(plan: LogicalPlan, alwaysInline: Boolean, cteRelations: Seq[(String, CTERelationDef)]): LogicalPlan =

    plan.resolveOperatorsUpWithPruning(
        _.containsAnyPattern(RELATION_TIME_TRAVEL, UNRESOLVED_RELATION, PLAN_EXPRESSION)) {

      case RelationTimeTravel(UnresolvedRelation(Seq(table), _, _), _, _)
        if cteRelations.exists(r => plan.conf.resolver(r._1, table)) =>
        throw QueryCompilationErrors.timeTravelUnsupportedError("subqueries from WITH clause")

      case u @ UnresolvedRelation(Seq(table), _, _) =>
        cteRelations.find(r => plan.conf.resolver(r._1, table)).map { case (_, d) =>
          if (alwaysInline) {
            d.child
          } else {
            // 为提示解析规则添加 `SubqueryAlias` 以匹配关系名称。
            SubqueryAlias(table, CTERelationRef(d.id, d.resolved, d.output))
          }
        }.getOrElse(u)

      case other =>
        // 这不能在 ResolveSubquery 中完成，因为 ResolveSubquery 不知道 CTE。
        other.transformExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
          case e: SubqueryExpression =>
            e.withNewPlan(apply(substituteCTE(e.plan, alwaysInline, cteRelations)))
        }
    }
}
