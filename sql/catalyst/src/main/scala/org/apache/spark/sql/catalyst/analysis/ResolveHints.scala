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

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Ascending, Expression, IntegerLiteral, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_HINT
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf


/**
 * Collection of rules related to hints. The only hint currently available is join strategy hint.
 *
 * Note that this is separately into two rules because in the future we might introduce new hint
 * rules that have different ordering requirements from join strategies.
 */
object ResolveHints {

  /**
   * 允许的连接策略提示（join strategy hint）列表在 [[JoinStrategyHint.strategies]] 中定义，
   * 可以使用连接策略提示指定一组关系别名，例如："MERGE(a, c)"、"BROADCAST(a)"。
   * 当匹配指定名称的关系（未被重新命名）、子查询或公共表表达式（CTE）时，
   * 将在其上方插入一个连接策略提示的计划节点（join strategy hint plan node）。
   */
  object ResolveJoinStrategyHints extends Rule[LogicalPlan] {
    private val STRATEGY_HINT_NAMES = JoinStrategyHint.strategies.flatMap(_.hintAliases)

    private def hintErrorHandler = conf.hintErrorHandler

    def resolver: Resolver = conf.resolver

    private def createHintInfo(hintName: String): HintInfo = {
      HintInfo(strategy =
        JoinStrategyHint.strategies.find(
          _.hintAliases.map(
            _.toUpperCase(Locale.ROOT)).contains(hintName.toUpperCase(Locale.ROOT))))
    }

    // This method checks if given multi-part identifiers are matched with each other.
    // The [[ResolveJoinStrategyHints]] rule is applied before the resolution batch
    // in the analyzer and we cannot semantically compare them at this stage.
    // Therefore, we follow a simple rule; they match if an identifier in a hint
    // is a tail of an identifier in a relation. This process is independent of a session
    // catalog (`currentDb` in [[SessionCatalog]]) and it just compares them literally.
    //
    // For example,
    //  * in a query `SELECT /*+ BROADCAST(t) */ * FROM db1.t JOIN t`,
    //    the broadcast hint will match both tables, `db1.t` and `t`,
    //    even when the current db is `db2`.
    //  * in a query `SELECT /*+ BROADCAST(default.t) */ * FROM default.t JOIN t`,
    //    the broadcast hint will match the left-side table only, `default.t`.
    private def matchedIdentifier(identInHint: Seq[String], identInQuery: Seq[String]): Boolean = {
      if (identInHint.length <= identInQuery.length) {
        identInHint.zip(identInQuery.takeRight(identInHint.length))
          .forall { case (i1, i2) => resolver(i1, i2) }
      } else {
        false
      }
    }

    private def extractIdentifier(r: SubqueryAlias): Seq[String] = {
      r.identifier.qualifier :+ r.identifier.name
    }

    private def applyJoinStrategyHint(
        plan: LogicalPlan,
        relationsInHint: Set[Seq[String]],
        relationsInHintWithMatch: mutable.HashSet[Seq[String]],
        hintName: String): LogicalPlan = {
      // Whether to continue recursing down the tree
      var recurse = true

      def matchedIdentifierInHint(identInQuery: Seq[String]): Boolean = {
        relationsInHint.find(matchedIdentifier(_, identInQuery))
          .map(relationsInHintWithMatch.add).nonEmpty
      }

      val newNode = CurrentOrigin.withOrigin(plan.origin) {
        plan match {
          case ResolvedHint(u @ UnresolvedRelation(ident, _, _), hint)
              if matchedIdentifierInHint(ident) =>
            ResolvedHint(u, createHintInfo(hintName).merge(hint, hintErrorHandler))

          case ResolvedHint(r: SubqueryAlias, hint)
              if matchedIdentifierInHint(extractIdentifier(r)) =>
            ResolvedHint(r, createHintInfo(hintName).merge(hint, hintErrorHandler))

          case UnresolvedRelation(ident, _, _) if matchedIdentifierInHint(ident) =>
            ResolvedHint(plan, createHintInfo(hintName))

          case r: SubqueryAlias if matchedIdentifierInHint(extractIdentifier(r)) =>
            ResolvedHint(plan, createHintInfo(hintName))

          case _: ResolvedHint | _: View | _: UnresolvedWith | _: SubqueryAlias =>
            // Don't traverse down these nodes.
            // For an existing strategy hint, there is no chance for a match from this point down.
            // The rest (view, with, subquery) indicates different scopes that we shouldn't traverse
            // down. Note that technically when this rule is executed, we haven't completed view
            // resolution yet and as a result the view part should be deadcode. I'm leaving it here
            // to be more future proof in case we change the view we do view resolution.
            recurse = false
            plan

          case _ =>
            plan
        }
      }

      if ((plan fastEquals newNode) && recurse) {
        newNode.mapChildren { child =>
          applyJoinStrategyHint(child, relationsInHint, relationsInHintWithMatch, hintName)
        }
      } else {
        newNode
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
      _.containsPattern(UNRESOLVED_HINT), ruleId) {
      case h: UnresolvedHint if STRATEGY_HINT_NAMES.contains(h.name.toUpperCase(Locale.ROOT)) =>
        if (h.parameters.isEmpty) {
          // If there is no table alias specified, apply the hint on the entire subtree.
          ResolvedHint(h.child, createHintInfo(h.name))
        } else {
          // Otherwise, find within the subtree query plans to apply the hint.
          val relationNamesInHint = h.parameters.map {
            case tableName: String => UnresolvedAttribute.parseAttributeName(tableName)
            case tableId: UnresolvedAttribute => tableId.nameParts
            case unsupported =>
              throw QueryCompilationErrors.joinStrategyHintParameterNotSupportedError(unsupported)
          }.toSet
          val relationsInHintWithMatch = new mutable.HashSet[Seq[String]]
          val applied = applyJoinStrategyHint(
            h.child, relationNamesInHint, relationsInHintWithMatch, h.name)

          // Filters unmatched relation identifiers in the hint
          val unmatchedIdents = relationNamesInHint -- relationsInHintWithMatch
          hintErrorHandler.hintRelationsNotFound(h.name, h.parameters, unmatchedIdents)
          applied
        }
    }
  }

  /**
   * COALESCE Hint accepts names "COALESCE", "REPARTITION", and "REPARTITION_BY_RANGE".
   */
  object ResolveCoalesceHints extends Rule[LogicalPlan] {

    val COALESCE_HINT_NAMES: Set[String] =
      Set("COALESCE", "REPARTITION", "REPARTITION_BY_RANGE", "REBALANCE")

    /**
     * This function handles hints for "COALESCE" and "REPARTITION".
     * The "COALESCE" hint only has a partition number as a parameter. The "REPARTITION" hint
     * has a partition number, columns, or both of them as parameters.
     */
    private def createRepartition(
        shuffle: Boolean, hint: UnresolvedHint): LogicalPlan = {
      val hintName = hint.name.toUpperCase(Locale.ROOT)

      def createRepartitionByExpression(
          numPartitions: Option[Int], partitionExprs: Seq[Any]): RepartitionByExpression = {
        val sortOrders = partitionExprs.filter(_.isInstanceOf[SortOrder])
        if (sortOrders.nonEmpty) {
          throw QueryCompilationErrors.invalidRepartitionExpressionsError(sortOrders)
        }
        val invalidParams = partitionExprs.filter(!_.isInstanceOf[UnresolvedAttribute])
        if (invalidParams.nonEmpty) {
          throw QueryCompilationErrors.invalidHintParameterError(hintName, invalidParams)
        }
        RepartitionByExpression(
          partitionExprs.map(_.asInstanceOf[Expression]), hint.child, numPartitions)
      }

      hint.parameters match {
        case Seq(IntegerLiteral(numPartitions)) =>
          Repartition(numPartitions, shuffle, hint.child)
        case Seq(numPartitions: Int) =>
          Repartition(numPartitions, shuffle, hint.child)
        // The "COALESCE" hint (shuffle = false) must have a partition number only
        case _ if !shuffle =>
          throw QueryCompilationErrors.invalidCoalesceHintParameterError(hintName)

        case param @ Seq(IntegerLiteral(numPartitions), _*) if shuffle =>
          createRepartitionByExpression(Some(numPartitions), param.tail)
        case param @ Seq(numPartitions: Int, _*) if shuffle =>
          createRepartitionByExpression(Some(numPartitions), param.tail)
        case param @ Seq(_*) if shuffle =>
          createRepartitionByExpression(None, param)
      }
    }

    /**
     * This function handles hints for "REPARTITION_BY_RANGE".
     * The "REPARTITION_BY_RANGE" hint must have column names and a partition number is optional.
     */
    private def createRepartitionByRange(hint: UnresolvedHint): RepartitionByExpression = {
      val hintName = hint.name.toUpperCase(Locale.ROOT)

      def createRepartitionByExpression(
          numPartitions: Option[Int], partitionExprs: Seq[Any]): RepartitionByExpression = {
        val invalidParams = partitionExprs.filter(!_.isInstanceOf[UnresolvedAttribute])
        if (invalidParams.nonEmpty) {
          throw QueryCompilationErrors.invalidHintParameterError(hintName, invalidParams)
        }
        val sortOrder = partitionExprs.map {
          case expr: SortOrder => expr
          case expr: Expression => SortOrder(expr, Ascending)
        }
        RepartitionByExpression(sortOrder, hint.child, numPartitions)
      }

      hint.parameters match {
        case param @ Seq(IntegerLiteral(numPartitions), _*) =>
          createRepartitionByExpression(Some(numPartitions), param.tail)
        case param @ Seq(numPartitions: Int, _*) =>
          createRepartitionByExpression(Some(numPartitions), param.tail)
        case param @ Seq(_*) =>
          createRepartitionByExpression(None, param)
      }
    }

    private def createRebalance(hint: UnresolvedHint): LogicalPlan = {
      def createRebalancePartitions(
          partitionExprs: Seq[Any], initialNumPartitions: Option[Int]): RebalancePartitions = {
        val invalidParams = partitionExprs.filter(!_.isInstanceOf[UnresolvedAttribute])
        if (invalidParams.nonEmpty) {
          val hintName = hint.name.toUpperCase(Locale.ROOT)
          throw QueryCompilationErrors.invalidHintParameterError(hintName, invalidParams)
        }
        RebalancePartitions(
          partitionExprs.map(_.asInstanceOf[Expression]),
          hint.child,
          initialNumPartitions)
      }

      hint.parameters match {
        case param @ Seq(IntegerLiteral(numPartitions), _*) =>
          createRebalancePartitions(param.tail, Some(numPartitions))
        case param @ Seq(numPartitions: Int, _*) =>
          createRebalancePartitions(param.tail, Some(numPartitions))
        case partitionExprs @ Seq(_*) =>
          createRebalancePartitions(partitionExprs, None)
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
      _.containsPattern(UNRESOLVED_HINT), ruleId) {
      case hint @ UnresolvedHint(hintName, _, _) => hintName.toUpperCase(Locale.ROOT) match {
          case "REPARTITION" =>
            createRepartition(shuffle = true, hint)
          case "COALESCE" =>
            createRepartition(shuffle = false, hint)
          case "REPARTITION_BY_RANGE" =>
            createRepartitionByRange(hint)
          case "REBALANCE" if conf.adaptiveExecutionEnabled =>
            createRebalance(hint)
          case _ => hint
        }
    }
  }

  /**
   * Removes all the hints, used to remove invalid hints provided by the user.
   * This must be executed after all the other hint rules are executed.
   */
  class RemoveAllHints extends Rule[LogicalPlan] {

    private def hintErrorHandler = conf.hintErrorHandler

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
      _.containsPattern(UNRESOLVED_HINT)) {
      case h: UnresolvedHint =>
        hintErrorHandler.hintNotRecognized(h.name, h.parameters)
        h.child
    }
  }

  /**
   * 当设置了 spark.sql.optimizer.disableHints 时，移除所有的 hint（提示）。
   * 这将在 Analyzer 的最开始阶段执行，以禁用 hint 功能。
   */
  class DisableHints extends RemoveAllHints {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      if (conf.getConf(SQLConf.DISABLE_HINTS)) super.apply(plan) else plan
    }
  }
}
