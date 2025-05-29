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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE
import org.apache.spark.sql.types.IntegerType

/**
 * 本规则将带有去重聚合的聚合查询重写为扩展的双重聚合*
 * 其中常规聚合表达式和每个去重子句分别在独立的组中进行聚合*
 * 最终在第二次聚合中将结果合并*
 *
 * First example: query without filter clauses (in scala):
 * {{{
 *   val data = Seq(
 *     ("a", "ca1", "cb1", 10),
 *     ("a", "ca1", "cb2", 5),
 *     ("b", "ca1", "cb1", 13))
 *     .toDF("key", "cat1", "cat2", "value")
 *   data.createOrReplaceTempView("data")
 *
 *   val agg = data.groupBy($"key")
 *     .agg(
 *       count_distinct($"cat1").as("cat1_cnt"),
 *       count_distinct($"cat2").as("cat2_cnt"),
 *       sum($"value").as("total"))
 * }}}
 *
 * This translates to the following (pseudo) logical plan:
 * {{{
 * Aggregate(
 *    key = ['key]
 *    functions = [COUNT(DISTINCT 'cat1),
 *                 COUNT(DISTINCT 'cat2),
 *                 sum('value)]
 *    output = ['key, 'cat1_cnt, 'cat2_cnt, 'total])
 *   LocalTableScan [...]
 * }}}
 *
 * This rule rewrites this logical plan to the following (pseudo) logical plan:
 * {{{
 * Aggregate(
 *    key = ['key]
 *    functions = [count('cat1) FILTER (WHERE 'gid = 1),
 *                 count('cat2) FILTER (WHERE 'gid = 2),
 *                 first('total) ignore nulls FILTER (WHERE 'gid = 0)]
 *    output = ['key, 'cat1_cnt, 'cat2_cnt, 'total])
 *   Aggregate(
 *      key = ['key, 'cat1, 'cat2, 'gid]
 *      functions = [sum('value)]
 *      output = ['key, 'cat1, 'cat2, 'gid, 'total])
 *     Expand(
 *        projections = [('key, null, null, 0, cast('value as bigint)),
 *                       ('key, 'cat1, null, 1, null),
 *                       ('key, null, 'cat2, 2, null)]
 *        output = ['key, 'cat1, 'cat2, 'gid, 'value])
 *       LocalTableScan [...]
 * }}}
 *
 * Second example: aggregate function without distinct and with filter clauses (in sql):
 * {{{
 *   SELECT
 *     COUNT(DISTINCT cat1) as cat1_cnt,
 *     COUNT(DISTINCT cat2) as cat2_cnt,
 *     SUM(value) FILTER (WHERE id > 1) AS total
 *   FROM
 *     data
 *   GROUP BY
 *     key
 * }}}
 *
 * This translates to the following (pseudo) logical plan:
 * {{{
 * Aggregate(
 *    key = ['key]
 *    functions = [COUNT(DISTINCT 'cat1),
 *                 COUNT(DISTINCT 'cat2),
 *                 sum('value) FILTER (WHERE 'id > 1)]
 *    output = ['key, 'cat1_cnt, 'cat2_cnt, 'total])
 *   LocalTableScan [...]
 * }}}
 *
 * This rule rewrites this logical plan to the following (pseudo) logical plan:
 * {{{
 * Aggregate(
 *    key = ['key]
 *    functions = [count('cat1) FILTER (WHERE 'gid = 1),
 *                 count('cat2) FILTER (WHERE 'gid = 2),
 *                 first('total) ignore nulls FILTER (WHERE 'gid = 0)]
 *    output = ['key, 'cat1_cnt, 'cat2_cnt, 'total])
 *   Aggregate(
 *      key = ['key, 'cat1, 'cat2, 'gid]
 *      functions = [sum('value) FILTER (WHERE 'id > 1)]
 *      output = ['key, 'cat1, 'cat2, 'gid, 'total])
 *     Expand(
 *        projections = [('key, null, null, 0, cast('value as bigint), 'id),
 *                       ('key, 'cat1, null, 1, null, null),
 *                       ('key, null, 'cat2, 2, null, null)]
 *        output = ['key, 'cat1, 'cat2, 'gid, 'value, 'id])
 *       LocalTableScan [...]
 * }}}
 *
 * Third example: aggregate function with distinct and filter clauses (in sql):
 * {{{
 *   SELECT
 *     COUNT(DISTINCT cat1) FILTER (WHERE id > 1) as cat1_cnt,
 *     COUNT(DISTINCT cat2) FILTER (WHERE id > 2) as cat2_cnt,
 *     SUM(value) FILTER (WHERE id > 3) AS total
 *   FROM
 *     data
 *   GROUP BY
 *     key
 * }}}
 *
 * This translates to the following (pseudo) logical plan:
 * {{{
 * Aggregate(
 *    key = ['key]
 *    functions = [COUNT(DISTINCT 'cat1) FILTER (WHERE 'id > 1),
 *                 COUNT(DISTINCT 'cat2) FILTER (WHERE 'id > 2),
 *                 sum('value) FILTER (WHERE 'id > 3)]
 *    output = ['key, 'cat1_cnt, 'cat2_cnt, 'total])
 *   LocalTableScan [...]
 * }}}
 *
 * This rule rewrites this logical plan to the following (pseudo) logical plan:
 * {{{
 * Aggregate(
 *    key = ['key]
 *    functions = [count('cat1) FILTER (WHERE 'gid = 1 and 'max_cond1),
 *                 count('cat2) FILTER (WHERE 'gid = 2 and 'max_cond2),
 *                 first('total) ignore nulls FILTER (WHERE 'gid = 0)]
 *    output = ['key, 'cat1_cnt, 'cat2_cnt, 'total])
 *   Aggregate(
 *      key = ['key, 'cat1, 'cat2, 'gid]
 *      functions = [max('cond1), max('cond2), sum('value) FILTER (WHERE 'id > 3)]
 *      output = ['key, 'cat1, 'cat2, 'gid, 'max_cond1, 'max_cond2, 'total])
 *     Expand(
 *        projections = [('key, null, null, 0, null, null, cast('value as bigint), 'id),
 *                       ('key, 'cat1, null, 1, 'id > 1, null, null, null),
 *                       ('key, null, 'cat2, 2, null, 'id > 2, null, null)]
 *        output = ['key, 'cat1, 'cat2, 'gid, 'cond1, 'cond2, 'value, 'id])
 *       LocalTableScan [...]
 * }}}
 *
 * 本规则实现步骤如下：
 * 数据扩展阶段：
 * 查询中存在三个聚合组：
 *   i. 非去重组
 *   ii. 'cat1去重组
 *   iii. 'cat2去重组
 *
 *  通过插入扩展运算符(expand)为每个组复制子数据。扩展操作会将当前组未使用的列置为NULL，
 *  这对后续计算的正确性至关重要。扩展运算符会添加组标识列(gid)来区分不同组。
 *  若去重组包含过滤条件，扩展操作会计算过滤结果（如cond1），该结果将用于后续计算与过滤子句等效的全局条件（如max_cond1）。
 *
 *  - 去重与非聚合路径处理：
 *  本阶段聚合的GROUP BY子句包含：原始分组列、所有去重列及组ID(gid)。
 *  通过利用gid分组和NULL化非相关列的特性，同时实现去重列的去重和非去重组的聚合。
 *  若去重组存在过滤条件，使用max聚合上一步的过滤输出结果（如cond1），生成与过滤子句等效的全局条件（如max_cond1）。
 *
 *  - 合并聚合结果：
 *  使用组ID和全局条件筛选聚合函数的输入数据。当全局条件（如max_cond1）为真时，
 *  表示至少有一条去重值记录满足过滤条件，该去重值应纳入聚合计算。
 *  非去重组的聚合结果通过first运算符合并，未来可采用更优雅的原生UDAF合并机制。
 *
 *  注意事项：
 *  本规则会使输入数据复制2次或更多（去重组数量+可选的非去重组），
 *  这将给聚合运算符和交换运算符带来较大内存压力。应尽可能减少去重组的数量，
 *  当前规则可通过应用更高级的表达式规范化技术进行优化。
 */
object RewriteDistinctAggregates extends Rule[LogicalPlan] {

  private def mayNeedtoRewrite(a: Aggregate): Boolean = {
    val aggExpressions = collectAggregateExprs(a)
    val distinctAggs = aggExpressions.filter(_.isDistinct)
    // We need at least two distinct aggregates or the single distinct aggregate group exists filter
    // clause for this rule because aggregation strategy can handle a single distinct aggregate
    // group without filter clause.
    // This check can produce false-positives, e.g., SUM(DISTINCT a) & COUNT(DISTINCT a).
    distinctAggs.size > 1 || distinctAggs.exists(_.filter.isDefined)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsPattern(AGGREGATE)) {
    case a: Aggregate if mayNeedtoRewrite(a) => rewrite(a)
  }

  def rewrite(a: Aggregate): Aggregate = {

    val aggExpressions = collectAggregateExprs(a)
    val distinctAggs = aggExpressions.filter(_.isDistinct)

    // Extract distinct aggregate expressions.
    val distinctAggGroups = aggExpressions.filter(_.isDistinct).groupBy { e =>
        val unfoldableChildren = e.aggregateFunction.children.filter(!_.foldable).toSet
        if (unfoldableChildren.nonEmpty) {
          // Only expand the unfoldable children
          unfoldableChildren
        } else {
          // If aggregateFunction's children are all foldable
          // we must expand at least one of the children (here we take the first child),
          // or If we don't, we will get the wrong result, for example:
          // count(distinct 1) will be explained to count(1) after the rewrite function.
          // Generally, the distinct aggregateFunction should not run
          // foldable TypeCheck for the first child.
          e.aggregateFunction.children.take(1).toSet
        }
    }

    // Aggregation strategy can handle queries with a single distinct group without filter clause.
    if (distinctAggGroups.size > 1 || distinctAggs.exists(_.filter.isDefined)) {
      // Create the attributes for the grouping id and the group by clause.
      val gid = AttributeReference("gid", IntegerType, nullable = false)()
      val groupByMap = a.groupingExpressions.collect {
        case ne: NamedExpression => ne -> ne.toAttribute
        case e => e -> AttributeReference(e.sql, e.dataType, e.nullable)()
      }
      val groupByAttrs = groupByMap.map(_._2)

      def patchAggregateFunctionChildren(
          af: AggregateFunction)(
          attrs: Expression => Option[Expression]): AggregateFunction = {
        val newChildren = af.children.map(c => attrs(c).getOrElse(c))
        af.withNewChildren(newChildren).asInstanceOf[AggregateFunction]
      }

      // Setup unique distinct aggregate children.
      val distinctAggChildren = distinctAggGroups.keySet.flatten.toSeq.distinct
      val distinctAggChildAttrMap = distinctAggChildren.map(expressionAttributePair)
      val distinctAggChildAttrs = distinctAggChildAttrMap.map(_._2)
      // Setup all the filters in distinct aggregate.
      val (distinctAggFilters, distinctAggFilterAttrs, maxConds) = distinctAggs.collect {
        case AggregateExpression(_, _, _, filter, _) if filter.isDefined =>
          val (e, attr) = expressionAttributePair(filter.get)
          val aggregateExp = Max(attr).toAggregateExpression()
          (e, attr, Alias(aggregateExp, attr.name)())
      }.unzip3

      // Setup expand & aggregate operators for distinct aggregate expressions.
      val distinctAggChildAttrLookup = distinctAggChildAttrMap.toMap
      val distinctAggFilterAttrLookup = distinctAggFilters.zip(maxConds.map(_.toAttribute)).toMap
      val distinctAggOperatorMap = distinctAggGroups.toSeq.zipWithIndex.map {
        case ((group, expressions), i) =>
          val id = Literal(i + 1)

          // Expand projection for filter
          val filters = expressions.filter(_.filter.isDefined).map(_.filter.get)
          val filterProjection = distinctAggFilters.map {
            case e if filters.contains(e) => e
            case e => nullify(e)
          }

          // Expand projection
          val projection = distinctAggChildren.map {
            case e if group.contains(e) => e
            case e => nullify(e)
          } :+ id

          // Final aggregate
          val operators = expressions.map { e =>
            val af = e.aggregateFunction
            val condition = e.filter.flatMap(distinctAggFilterAttrLookup.get)
            val naf = if (af.children.forall(_.foldable)) {
              af
            } else {
              patchAggregateFunctionChildren(af) { x =>
                distinctAggChildAttrLookup.get(x)
              }
            }
            val newCondition = if (condition.isDefined) {
              And(EqualTo(gid, id), condition.get)
            } else {
              EqualTo(gid, id)
            }

            (e, e.copy(aggregateFunction = naf, isDistinct = false, filter = Some(newCondition)))
          }

          (projection ++ filterProjection, operators)
      }

      // Setup expand for the 'regular' aggregate expressions.
      // only expand unfoldable children
      val regularAggExprs = aggExpressions
        .filter(e => !e.isDistinct && e.children.exists(!_.foldable))
      val regularAggFunChildren = regularAggExprs
        .flatMap(_.aggregateFunction.children.filter(!_.foldable))
      val regularAggFilterAttrs = regularAggExprs.flatMap(_.filterAttributes)
      val regularAggChildren = (regularAggFunChildren ++ regularAggFilterAttrs).distinct
      val regularAggChildAttrMap = regularAggChildren.map(expressionAttributePair)

      // Setup aggregates for 'regular' aggregate expressions.
      val regularGroupId = Literal(0)
      val regularAggChildAttrLookup = regularAggChildAttrMap.toMap
      val regularAggOperatorMap = regularAggExprs.map { e =>
        // Perform the actual aggregation in the initial aggregate.
        val af = patchAggregateFunctionChildren(e.aggregateFunction)(regularAggChildAttrLookup.get)
        // We changed the attributes in the [[Expand]] output using expressionAttributePair.
        // So we need to replace the attributes in FILTER expression with new ones.
        val filterOpt = e.filter.map(_.transform {
          case a: Attribute => regularAggChildAttrLookup.getOrElse(a, a)
        })
        val operator = Alias(e.copy(aggregateFunction = af, filter = filterOpt), e.sql)()

        // Select the result of the first aggregate in the last aggregate.
        val result = AggregateExpression(
          aggregate.First(operator.toAttribute, ignoreNulls = true),
          mode = Complete,
          isDistinct = false,
          filter = Some(EqualTo(gid, regularGroupId)))

        // Some aggregate functions (COUNT) have the special property that they can return a
        // non-null result without any input. We need to make sure we return a result in this case.
        val resultWithDefault = af.defaultResult match {
          case Some(lit) => Coalesce(Seq(result, lit))
          case None => result
        }

        // Return a Tuple3 containing:
        // i. The original aggregate expression (used for look ups).
        // ii. The actual aggregation operator (used in the first aggregate).
        // iii. The operator that selects and returns the result (used in the second aggregate).
        (e, operator, resultWithDefault)
      }

      // Construct the regular aggregate input projection only if we need one.
      val regularAggProjection = if (regularAggExprs.nonEmpty) {
        Seq(a.groupingExpressions ++
          distinctAggChildren.map(nullify) ++
          Seq(regularGroupId) ++
          distinctAggFilters.map(nullify) ++
          regularAggChildren)
      } else {
        Seq.empty[Seq[Expression]]
      }

      // Construct the distinct aggregate input projections.
      val regularAggNulls = regularAggChildren.map(nullify)
      val distinctAggProjections = distinctAggOperatorMap.map {
        case (projection, _) =>
          a.groupingExpressions ++
            projection ++
            regularAggNulls
      }

      // Construct the expand operator.
      val expand = Expand(
        regularAggProjection ++ distinctAggProjections,
        groupByAttrs ++ distinctAggChildAttrs ++ Seq(gid) ++ distinctAggFilterAttrs ++
          regularAggChildAttrMap.map(_._2),
        a.child)

      // Construct the first aggregate operator. This de-duplicates all the children of
      // distinct operators, and applies the regular aggregate operators.
      val firstAggregateGroupBy = groupByAttrs ++ distinctAggChildAttrs :+ gid
      val firstAggregate = Aggregate(
        firstAggregateGroupBy,
        firstAggregateGroupBy ++ maxConds ++ regularAggOperatorMap.map(_._2),
        expand)

      // Construct the second aggregate
      val transformations: Map[Expression, Expression] =
        (distinctAggOperatorMap.flatMap(_._2) ++
          regularAggOperatorMap.map(e => (e._1, e._3))).toMap

      val patchedAggExpressions = a.aggregateExpressions.map { e =>
        e.transformDown {
          case e: Expression =>
            // The same GROUP BY clauses can have different forms (different names for instance) in
            // the groupBy and aggregate expressions of an aggregate. This makes a map lookup
            // tricky. So we do a linear search for a semantically equal group by expression.
            groupByMap
              .find(ge => e.semanticEquals(ge._1))
              .map(_._2)
              .getOrElse(transformations.getOrElse(e, e))
        }.asInstanceOf[NamedExpression]
      }
      Aggregate(groupByAttrs, patchedAggExpressions, firstAggregate)
    } else {
      a
    }
  }

  private def collectAggregateExprs(a: Aggregate): Seq[AggregateExpression] = {
    // Collect all aggregate expressions.
    a.aggregateExpressions.flatMap { _.collect {
        case ae: AggregateExpression => ae
    }}
  }

  private def nullify(e: Expression) = Literal.create(null, e.dataType)

  private def expressionAttributePair(e: Expression) =
    // We are creating a new reference here instead of reusing the attribute in case of a
    // NamedExpression. This is done to prevent collisions between distinct and regular aggregate
    // children, in this case attribute reuse causes the input of the regular aggregate to bound to
    // the (nulled out) input of the distinct aggregate.
    e -> AttributeReference(e.sql, e.dataType, nullable = true)()
}
