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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.expressions.{Expression, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, DeleteFromTableWithFilters, LogicalPlan, ReplaceData, RowLevelWrite}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{SupportsDelete, TruncatableTable}
import org.apache.spark.sql.connector.write.RowLevelOperation
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.DELETE
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources

/**
 * 该规则用于将重写后的DELETE操作替换为使用过滤条件的删除操作，
 * 前提是数据源能够在不执行针对单行或行组的操作计划的情况下处理此DELETE命令。
 * 注意：此规则必须在表达式优化之后、扫描计划生成之前执行。
 */
object OptimizeMetadataOnlyDeleteFromTable extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case RewrittenRowLevelCommand(rowLevelPlan, DELETE, cond, relation: DataSourceV2Relation) =>
      relation.table match {
        case table: SupportsDelete if !SubqueryExpression.hasSubquery(cond) =>
          val predicates = splitConjunctivePredicates(cond)
          val normalizedPredicates = DataSourceStrategy.normalizeExprs(predicates, relation.output)
          val filters = toDataSourceFilters(normalizedPredicates)
          val allPredicatesTranslated = normalizedPredicates.size == filters.length
          if (allPredicatesTranslated && table.canDeleteWhere(filters)) {
            logDebug(s"Switching to delete with filters: ${filters.mkString("[", ", ", "]")}")
            DeleteFromTableWithFilters(relation, filters)
          } else {
            rowLevelPlan
          }

        case _: TruncatableTable if cond == TrueLiteral =>
          DeleteFromTable(relation, cond)

        case _ =>
          rowLevelPlan
      }
  }

  private def toDataSourceFilters(predicates: Seq[Expression]): Array[sources.Filter] = {
    predicates.flatMap { p =>
      val filter = DataSourceStrategy.translateFilter(p, supportNestedPredicatePushdown = true)
      if (filter.isEmpty) {
        logDebug(s"Cannot translate expression to data source filter: $p")
      }
      filter
    }.toArray
  }

  private object RewrittenRowLevelCommand {
    type ReturnType = (RowLevelWrite, RowLevelOperation.Command, Expression, LogicalPlan)

    def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
      case rd @ ReplaceData(_, cond, _, originalTable, _) =>
        val command = rd.operation.command
        Some(rd, command, cond, originalTable)

      case _ =>
        None
    }
  }
}
