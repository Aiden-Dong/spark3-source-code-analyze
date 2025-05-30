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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.analysis.DeduplicateRelations
import org.apache.spark.sql.catalyst.expressions.{Alias, OuterReference, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, CTERelationRef, Join, JoinHint, LogicalPlan, Project, Subquery, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, PLAN_EXPRESSION}

/**
 * 如果满足以下任一条件，将CTE定义内联到对应的引用中：
 *
 *  - CTE 定义不包含任何非确定性表达式，或包含对外部查询的属性引用。
 *  - 即使该 CTE 定义引用了另一个含有非确定性表达式的 CTE 定义，仍然可以内联当前CTE定义。
 *
 *  CTE定义在整个主查询及所有子查询中仅被引用一次。出现在子查询中且未被内联的CTE定义将被提升到主查询层级。
 *
 *  @param alwaysInline 如果为true，则内联查询计划中的所有CTE。
 */

case class InlineCTE(alwaysInline: Boolean = false) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.isInstanceOf[Subquery] && plan.containsPattern(CTE)) {
      val cteMap = mutable.HashMap.empty[Long, (CTERelationDef, Int)]
      buildCTEMap(plan, cteMap)
      val notInlined = mutable.ArrayBuffer.empty[CTERelationDef]
      val inlined = inlineCTE(plan, cteMap, notInlined)
      // CTEs in SQL Commands have been inlined by `CTESubstitution` already, so it is safe to add
      // WithCTE as top node here.
      if (notInlined.isEmpty) {
        inlined
      } else {
        WithCTE(inlined, notInlined.toSeq)
      }
    } else {
      plan
    }
  }

  private def shouldInline(cteDef: CTERelationDef, refCount: Int): Boolean = alwaysInline || {
    // We do not need to check enclosed `CTERelationRef`s for `deterministic` or `OuterReference`,
    // because:
    // 1) It is fine to inline a CTE if it references another CTE that is non-deterministic;
    // 2) Any `CTERelationRef` that contains `OuterReference` would have been inlined first.
    refCount == 1 ||
      cteDef.deterministic ||
      cteDef.child.exists(_.expressions.exists(_.isInstanceOf[OuterReference]))
  }

  private def buildCTEMap(
      plan: LogicalPlan,
      cteMap: mutable.HashMap[Long, (CTERelationDef, Int)]): Unit = {
    plan match {
      case WithCTE(_, cteDefs) =>
        cteDefs.foreach { cteDef =>
          cteMap.put(cteDef.id, (cteDef, 0))
        }

      case ref: CTERelationRef =>
        val (cteDef, refCount) = cteMap(ref.cteId)
        cteMap.update(ref.cteId, (cteDef, refCount + 1))

      case _ =>
    }

    if (plan.containsPattern(CTE)) {
      plan.children.foreach { child =>
        buildCTEMap(child, cteMap)
      }

      plan.expressions.foreach { expr =>
        if (expr.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
          expr.foreach {
            case e: SubqueryExpression =>
              buildCTEMap(e.plan, cteMap)
            case _ =>
          }
        }
      }
    }
  }

  private def inlineCTE(
      plan: LogicalPlan,
      cteMap: mutable.HashMap[Long, (CTERelationDef, Int)],
      notInlined: mutable.ArrayBuffer[CTERelationDef]): LogicalPlan = {
    plan match {
      case WithCTE(child, cteDefs) =>
        cteDefs.foreach { cteDef =>
          val (cte, refCount) = cteMap(cteDef.id)
          if (refCount > 0) {
            val inlined = cte.copy(child = inlineCTE(cte.child, cteMap, notInlined))
            cteMap.update(cteDef.id, (inlined, refCount))
            if (!shouldInline(inlined, refCount)) {
              notInlined.append(inlined)
            }
          }
        }
        inlineCTE(child, cteMap, notInlined)

      case ref: CTERelationRef =>
        val (cteDef, refCount) = cteMap(ref.cteId)
        if (shouldInline(cteDef, refCount)) {
          if (ref.outputSet == cteDef.outputSet) {
            cteDef.child
          } else {
            val ctePlan = DeduplicateRelations(
              Join(cteDef.child, cteDef.child, Inner, None, JoinHint(None, None))).children(1)
            val projectList = ref.output.zip(ctePlan.output).map { case (tgtAttr, srcAttr) =>
              Alias(srcAttr, tgtAttr.name)(exprId = tgtAttr.exprId)
            }
            Project(projectList, ctePlan)
          }
        } else {
          ref
        }

      case _ if plan.containsPattern(CTE) =>
        plan
          .withNewChildren(plan.children.map(child => inlineCTE(child, cteMap, notInlined)))
          .transformExpressionsWithPruning(_.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
            case e: SubqueryExpression =>
              e.withNewPlan(inlineCTE(e.plan, cteMap, notInlined))
          }

      case _ => plan
    }
  }
}
