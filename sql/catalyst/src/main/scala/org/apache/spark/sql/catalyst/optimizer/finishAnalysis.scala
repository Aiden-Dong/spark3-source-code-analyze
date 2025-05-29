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

import java.time.{Instant, LocalDateTime}

import org.apache.spark.sql.catalyst.CurrentUserContext.CURRENT_USER
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.trees.TreePatternBits
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{convertSpecialDate, convertSpecialTimestamp, convertSpecialTimestampNTZ, instantToMicros, localDateTimeToMicros}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils



/**
 * 查找并替换所有无法直接求值的[[RuntimeReplaceable]]表达式，
 * 将其转换为语义等效且可求值的表达式。
 * 该转换主要提供与其他数据库的兼容支持，典型场景包括：
 * 将"left"函数替换为"substring"实现
 * 分别用Min和Max替换Every和Any函数
 */
object ReplaceExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsAnyPattern(RUNTIME_REPLACEABLE)) {
    case p => p.mapExpressions(replace)
  }

  private def replace(e: Expression): Expression = e match {
    case r: RuntimeReplaceable => replace(r.replacement)
    case _ => e.mapChildren(replace)
  }
}


/**
 * 将非关联的EXISTS子查询重写为使用标量子查询(ScalarSubquery)
 * 示例转换：
 * WHERE EXISTS (SELECT A FROM TABLE B WHERE COL1 > 10)
 * 将被重写为
 * WHERE (SELECT 1 FROM (SELECT A FROM TABLE B WHERE COL1 > 10) LIMIT 1) IS NOT NULL
 */
object RewriteNonCorrelatedExists extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressionsWithPruning(
    _.containsPattern(EXISTS_SUBQUERY)) {
    case exists: Exists if exists.children.isEmpty =>
      IsNotNull(
        ScalarSubquery(
          plan = Limit(Literal(1), Project(Seq(Alias(Literal(1), "col")()), exists.plan)),
          exprId = exists.exprId))
  }
}

/**
 * 计算当前日期和时间，以确保在同一个查询中返回一致的结果。
 */
object ComputeCurrentTime extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val instant = Instant.now()
    val currentTimestampMicros = instantToMicros(instant)
    val currentTime = Literal.create(currentTimestampMicros, TimestampType)
    val timezone = Literal.create(conf.sessionLocalTimeZone, StringType)

    def transformCondition(treePatternbits: TreePatternBits): Boolean = {
      treePatternbits.containsPattern(CURRENT_LIKE)
    }

    plan.transformDownWithSubqueriesAndPruning(transformCondition) {
      case subQuery =>
        subQuery.transformAllExpressionsWithPruning(transformCondition) {
          case cd: CurrentDate =>
            Literal.create(DateTimeUtils.microsToDays(currentTimestampMicros, cd.zoneId), DateType)
          case CurrentTimestamp() | Now() => currentTime
          case CurrentTimeZone() => timezone
          case localTimestamp: LocalTimestamp =>
            val asDateTime = LocalDateTime.ofInstant(instant, localTimestamp.zoneId)
            Literal.create(localDateTimeToMicros(asDateTime), TimestampNTZType)
        }
    }
  }
}

/**
 * 将CurrentDatabase表达式替换为当前数据库名
 * 将CurrentCatalog表达式替换为当前目录名称
 */
case class ReplaceCurrentLike(catalogManager: CatalogManager) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    val currentNamespace = catalogManager.currentNamespace.quoted
    val currentCatalog = catalogManager.currentCatalog.name()
    val currentUser = Option(CURRENT_USER.get()).getOrElse(Utils.getCurrentUserName())

    plan.transformAllExpressionsWithPruning(_.containsPattern(CURRENT_LIKE)) {
      case CurrentDatabase() =>
        Literal.create(currentNamespace, StringType)
      case CurrentCatalog() =>
        Literal.create(currentCatalog, StringType)
      case CurrentUser() =>
        Literal.create(currentUser, StringType)
    }
  }
}

/**
 * 当输入字符串可折叠时，将特殊日期时间字符串的类型转换替换为对应的日期/时间戳值。
 */
object SpecialDatetimeValues extends Rule[LogicalPlan] {
  private val conv = Map[DataType, (String, java.time.ZoneId) => Option[Any]](
    DateType -> convertSpecialDate,
    TimestampType -> convertSpecialTimestamp,
    TimestampNTZType -> convertSpecialTimestampNTZ)
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformAllExpressionsWithPruning(_.containsPattern(CAST)) {
      case cast @ Cast(e, dt @ (DateType | TimestampType | TimestampNTZType), _, _)
        if e.foldable && e.dataType == StringType =>
        Option(e.eval())
          .flatMap(s => conv(dt)(s.toString, cast.zoneId))
          .map(Literal(_, dt))
          .getOrElse(cast)
    }
  }
}
