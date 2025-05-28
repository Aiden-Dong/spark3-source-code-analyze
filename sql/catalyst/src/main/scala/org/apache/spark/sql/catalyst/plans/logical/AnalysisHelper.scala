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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Expression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.rules.RuleId
import org.apache.spark.sql.catalyst.rules.UnknownRuleId
import org.apache.spark.sql.catalyst.trees.{AlwaysProcess, CurrentOrigin, TreePatternBits}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.util.Utils

/**
 * [[AnalysisHelper]] 为查询分析器定义了一些基础设施。特别地，在查询分析过程中，我们不希望重复分析已被分析过的子计划（sub-plans）。
 *
 * 该特质（trait）定义了一个标志位 `analyzed`，当树形结构完成分析后可将其设为true。
 * 同时提供了一组解析方法（resolve methods），这些方法不会递归处理已标记为`analyzed=true`的子计划。
 *
 * 分析器规则应使用这些解析方法，而非直接使用定义在
 * [[org.apache.spark.sql.catalyst.trees.TreeNode]] 和 [[QueryPlan]] 中的转换方法（transform methods）。
 *
 * 为防止在分析器中意外使用转换方法，本特质在测试模式下会通过重写transform方法抛出异常。
 */
trait AnalysisHelper extends QueryPlan[LogicalPlan] { self: LogicalPlan =>

  private var _analyzed: Boolean = false

  /**
   * 递归地将本计划树中的所有节点标记为已分析状态。
   * 该方法应仅由 [[org.apache.spark.sql.catalyst.analysis.CheckAnalysis]] 调用。
   */
  private[sql] def setAnalyzed(): Unit = {
    if (!_analyzed) {
      _analyzed = true
      children.foreach(_.setAnalyzed())
    }
  }

  /**
   * 返回该节点及其子节点是否已完成分析和验证。
   * 注意：这仅作为优化手段用于避免重复分析已分析过的树，且可能被转换操作重置。
   */
  def analyzed: Boolean = _analyzed

  /**
   * 返回此节点的副本，其中`rule`已被递归地应用到树上。
   * 当`rule`不适用于某个节点时，该节点保持不变。
   * 此方法与`transform`类似，但会跳过已标记为分析完成的子树。
   * 用户不应期望特定的遍历方向（自上而下或自下而上），
   * 如果需要特定方向，应使用[[resolveOperatorsUp]]或[[resolveOperatorsDown]]方法。
   *
   * @param rule 用于转换此节点子节点的函数
   */
  def resolveOperators(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = {
    resolveOperatorsWithPruning(AlwaysProcess.fn, UnknownRuleId)(rule)
  }

  /**
   * 返回此节点的副本，其中对树递归应用了`rule`。
   * 当`rule`不适用于某个节点时，该节点保持不变。
   * 此函数类似于`transform`，但会跳过已标记为分析完成的子树。
   *
   * 用户不应期望特定的遍历方向。如果需要特定方向，
   * 应该使用[[resolveOperatorsUpWithPruning]]或[[resolveOperatorsDownWithPruning]]方法。
   *
   * @param rule   用于转换此节点子节点的函数
   * @param cond   用于剪枝树遍历的Lambda表达式。如果在运算符T上`cond.apply`返回false，
   *               则跳过处理T及其子树；否则，递归处理T及其子树
   * @param ruleId `rule`的唯一ID，用于剪枝不必要的树遍历。当为UnknownRuleId时不进行剪枝。
   *               如果`rule`(带有ID`ruleId`)已被标记为对运算符T无效，则跳过处理T及其子树。
   *               如果规则不是纯函数且每次调用会读取不同的初始状态，则不要传递此参数
   */
  def resolveOperatorsWithPruning(cond: TreePatternBits => Boolean,
    ruleId: RuleId = UnknownRuleId)(rule: PartialFunction[LogicalPlan, LogicalPlan])
  : LogicalPlan = {
    resolveOperatorsDownWithPruning(cond, ruleId)(rule)
  }

  /**
   * 返回此节点的副本，其中`rule`会先递归地应用于所有子节点，然后再应用于节点本身（后序遍历，自底向上）。
   * 当`rule`不适用于某个节点时，该节点保持不变。此方法与`transformUp`类似，
   * 但会跳过已被标记为已分析的子树。
   *
   * @param rule 用于转换此节点子节点的函数
   */
  def resolveOperatorsUp(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = {
    resolveOperatorsUpWithPruning(AlwaysProcess.fn, UnknownRuleId)(rule)
  }

  /**
   * 返回此节点的副本，其中会先递归地对所有子节点应用`rule`，再对节点本身应用（后序遍历，自底向上）。
   * 当`rule`不适用于某节点时，该节点保持不变。此方法与`transformUp`类似，
   * 但会跳过已标记为分析过的子树。
   *
   * @param rule   用于转换此节点子节点的函数
   * @param cond   用于剪枝遍历的Lambda表达式。如果在运算符 T 上`cond.apply`返回false，则跳过处理T及其子树；否则递归处理T及其子树
   * @param ruleId `rule`的唯一标识符，用于剪枝不必要的树遍历。当为UnknownRuleId时不进行剪枝。
   *               若运算符T已标记`rule`（具有`ruleId`）为无效，则跳过处理T及其子树。
   *               如果规则不是纯函数且每次调用读取不同初始状态，则不应传递此参数
   */
  def resolveOperatorsUpWithPruning(cond: TreePatternBits => Boolean,
    ruleId: RuleId = UnknownRuleId)(rule: PartialFunction[LogicalPlan, LogicalPlan])
  : LogicalPlan = {
    if (!analyzed && cond.apply(self) && !isRuleIneffective(ruleId)) {
      AnalysisHelper.allowInvokingTransformsInAnalyzer {
        val afterRuleOnChildren = mapChildren(_.resolveOperatorsUpWithPruning(cond, ruleId)(rule))
        val afterRule = if (self fastEquals afterRuleOnChildren) {
          CurrentOrigin.withOrigin(origin) {
            rule.applyOrElse(self, identity[LogicalPlan])
          }
        } else {
          CurrentOrigin.withOrigin(origin) {
            rule.applyOrElse(afterRuleOnChildren, identity[LogicalPlan])
          }
        }
        if (self eq afterRule) {
          self.markRuleAsIneffective(ruleId)
          self
        } else {
          afterRule.copyTagsFrom(self)
          afterRule
        }
      }
    } else {
      self
    }
  }

  /**
   * 与[[resolveOperatorsUp]]类似，但采用自顶向下的处理方式。
   */
  def resolveOperatorsDown(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = {
    resolveOperatorsDownWithPruning(AlwaysProcess.fn, UnknownRuleId)(rule)
  }

  /**
   * 类似于[[resolveOperatorsUpWithPruning]]，但采用自顶向下的方式处理。
   */
  def resolveOperatorsDownWithPruning(cond: TreePatternBits => Boolean,
    ruleId: RuleId = UnknownRuleId)(rule: PartialFunction[LogicalPlan, LogicalPlan])
  : LogicalPlan = {
    if (!analyzed && cond.apply(self) && !isRuleIneffective(ruleId)) {
      AnalysisHelper.allowInvokingTransformsInAnalyzer {
        val afterRule = CurrentOrigin.withOrigin(origin) {
          rule.applyOrElse(self, identity[LogicalPlan])
        }

        // Check if unchanged and then possibly return old copy to avoid gc churn.
        if (self fastEquals afterRule) {
          val rewritten_plan = mapChildren(_.resolveOperatorsDownWithPruning(cond, ruleId)(rule))
          if (self eq rewritten_plan) {
            self.markRuleAsIneffective(ruleId)
            self
          } else {
            rewritten_plan
          }
        } else {
          afterRule.mapChildren(_.resolveOperatorsDownWithPruning(cond, ruleId)(rule))
        }
      }
    } else {
      self
    }
  }

  /**
   * `transformUpWithNewOutput`的一个变体，跳过已分析过的执行计划。
   */
  def resolveOperatorsUpWithNewOutput(
      rule: PartialFunction[LogicalPlan, (LogicalPlan, Seq[(Attribute, Attribute)])])
  : LogicalPlan = {
    if (!analyzed) {
      transformUpWithNewOutput(rule, skipCond = _.analyzed, canGetOutput = _.resolved)
    } else {
      self
    }
  }

  override def transformUpWithNewOutput(
      rule: PartialFunction[LogicalPlan, (LogicalPlan, Seq[(Attribute, Attribute)])],
      skipCond: LogicalPlan => Boolean,
      canGetOutput: LogicalPlan => Boolean): LogicalPlan = {
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      super.transformUpWithNewOutput(rule, skipCond, canGetOutput)
    }
  }

  override def updateOuterReferencesInSubquery(plan: LogicalPlan, attrMap: AttributeMap[Attribute])
    : LogicalPlan = {
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      super.updateOuterReferencesInSubquery(plan, attrMap)
    }
  }

  /**
   * `transformUpWithNewOutput`的一个变体，跳过已分析的计划节点。
   */
  def resolveExpressions(r: PartialFunction[Expression, Expression]): LogicalPlan = {
    resolveExpressionsWithPruning(AlwaysProcess.fn, UnknownRuleId)(r)
  }

  /**
   * 递归转换树结构的表达式，跳过已分析的节点。
   *
   * @param rule   用于转换子节点的函数
   * @param cond   用于剪枝的Lambda表达式。如果`cond.apply`在树节点T上返回false，
   *               则跳过T及其子树的处理；否则递归处理T及其子树
   * @param ruleId 规则的唯一标识符，用于剪枝不必要的遍历。当为UnknownRuleId时不进行剪枝。
   *               如果规则(带ruleId)已被标记为对树节点T无效，则跳过T及其子树的处理。
   *               如果规则不是纯函数式且每次调用读取不同初始状态，则不应传递此参数
   */
  def resolveExpressionsWithPruning(cond: TreePatternBits => Boolean,
    ruleId: RuleId = UnknownRuleId)(rule: PartialFunction[Expression, Expression]): LogicalPlan = {
    resolveOperatorsWithPruning(cond, ruleId) {
      case p => p.transformExpressionsWithPruning(cond, ruleId)(rule)
    }
  }

  protected def assertNotAnalysisRule(): Unit = {
    if (Utils.isTesting &&
        AnalysisHelper.inAnalyzer.get > 0 &&
        AnalysisHelper.resolveOperatorDepth.get == 0) {
      throw QueryExecutionErrors.methodCalledInAnalyzerNotAllowedError()
    }
  }

  /**
   * 在分析器中，应使用[[resolveOperatorsDown()]]替代本方法。若在分析器中调用本方法，
   * 测试模式下将抛出异常。但在[[resolveOperatorsDown()]]调用范围内使用本方法是允许的。
   *
   * @see [[org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning()]]
   */
  override def transformDownWithPruning(cond: TreePatternBits => Boolean,
    ruleId: RuleId = UnknownRuleId)(rule: PartialFunction[LogicalPlan, LogicalPlan])
  : LogicalPlan = {
    assertNotAnalysisRule()
    super.transformDownWithPruning(cond, ruleId)(rule)
  }

  /**
   * Use [[resolveOperators()]] in the analyzer.
   *
   * @see [[org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning()]]
   */
  override def transformUpWithPruning(cond: TreePatternBits => Boolean,
    ruleId: RuleId = UnknownRuleId)(rule: PartialFunction[LogicalPlan, LogicalPlan])
  : LogicalPlan = {
    assertNotAnalysisRule()
    super.transformUpWithPruning(cond, ruleId)(rule)
  }

  /**
   * Use [[resolveExpressions()]] in the analyzer.
   * @see [[QueryPlan.transformAllExpressionsWithPruning()]]
   */
  override def transformAllExpressionsWithPruning(
    cond: TreePatternBits => Boolean,
    ruleId: RuleId = UnknownRuleId)(rule: PartialFunction[Expression, Expression])
  : this.type = {
    assertNotAnalysisRule()
    super.transformAllExpressionsWithPruning(cond, ruleId)(rule)
  }

  override def clone(): LogicalPlan = {
    val cloned = super.clone()
    if (analyzed) cloned.setAnalyzed()
    cloned
  }
}


object AnalysisHelper {

  /**
   * A thread local to track whether we are in a resolveOperator call (for the purpose of analysis).
   * This is an int because resolve* calls might be be nested (e.g. a rule might trigger another
   * query compilation within the rule itself), so we are tracking the depth here.
   */
  private val resolveOperatorDepth: ThreadLocal[Int] = new ThreadLocal[Int] {
    override def initialValue(): Int = 0
  }

  /**
   * A thread local to track whether we are in the analysis phase of query compilation. This is an
   * int rather than a boolean in case our analyzer recursively calls itself.
   */
  private val inAnalyzer: ThreadLocal[Int] = new ThreadLocal[Int] {
    override def initialValue(): Int = 0
  }

  def allowInvokingTransformsInAnalyzer[T](f: => T): T = {
    resolveOperatorDepth.set(resolveOperatorDepth.get + 1)
    try f finally {
      resolveOperatorDepth.set(resolveOperatorDepth.get - 1)
    }
  }

  def markInAnalyzer[T](f: => T): T = {
    inAnalyzer.set(inAnalyzer.get + 1)
    try f finally {
      inAnalyzer.set(inAnalyzer.get - 1)
    }
  }
}
