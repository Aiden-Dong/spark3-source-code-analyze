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

package org.apache.spark.sql

import scala.collection.mutable

import org.apache.spark.annotation.{DeveloperApi, Experimental, Unstable}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TableFunctionRegistry}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.analysis.TableFunctionRegistry.TableFunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

/**
 * :: 实验性 ::
 * [[SparkSession]] 的注入点持有者。我们不保证此处方法的二进制兼容性和源代码兼容性的稳定性。
 *
 * 目前提供以下扩展点：
 *
 * <ul>
 *  <li>Analyzer Rules (解析器规则)</li>
 *  <li>Check Analysis Rules (检查解析规则)</li>
 *  <li>Optimizer Rules (优化器规则)</li>
 *  <li>Pre CBO Rules (预成本预算优化CBO规则)</li>
 *  <li>Planning Strategies (计划策略)</li>
 *  <li>Customized Parser (自定义解析器)</li>
 *  <li>(External) Catalog listeners (外部目录监听器)</li>
 *  <li>Columnar Rules (列式规则)</li>
 *  <li>Adaptive Query Stage Preparation Rules (自适应查询阶段准备规则)</li>
 * </ul>
 *
 * 扩展可以通过在 [[SparkSession.Builder]] 上调用 `withExtensions` 方法来使用，例如：
 * {{{
 *   SparkSession.builder()
 *     .master("...")
 *     .config("...", true)
 *     .withExtensions { extensions =>
 *       extensions.injectResolutionRule { session =>
 *         ...
 *       }
 *       extensions.injectParser { (session, parser) =>
 *         ...
 *       }
 *     }
 *     .getOrCreate()
 * }}}
 *
 * 扩展还可以通过设置 Spark SQL 配置属性 `spark.sql.extensions` 来使用。
 * 多个扩展可以使用逗号分隔的列表进行设置。例如：
 * {{{
 *   SparkSession.builder()
 *     .master("...")
 *     .config("spark.sql.extensions", "org.example.MyExtensions,org.example.YourExtensions")
 *     .getOrCreate()
 *
 *   class MyExtensions extends Function1[SparkSessionExtensions, Unit] {
 *     override def apply(extensions: SparkSessionExtensions): Unit = {
 *       extensions.injectResolutionRule { session =>
 *         ...
 *       }
 *       extensions.injectParser { (session, parser) =>
 *         ...
 *       }
 *     }
 *   }
 *
 *   class YourExtensions extends SparkSessionExtensionsProvider {
 *     override def apply(extensions: SparkSessionExtensions): Unit = {
 *       extensions.injectResolutionRule { session =>
 *         ...
 *       }
 *       extensions.injectFunction(...)
 *     }
 *   }
 * }}}
 *
 * 请注意，任何注入的构建器都不应假设 [[SparkSession]] 已完全初始化，并且不应触及会话的内部（例如，SessionState）。
 */
@DeveloperApi
@Experimental
@Unstable
class SparkSessionExtensions {
  type RuleBuilder = SparkSession => Rule[LogicalPlan]                    // 逻辑树处理规则
  type CheckRuleBuilder = SparkSession => LogicalPlan => Unit
  type StrategyBuilder = SparkSession => Strategy                         // 将 LocalPlan 变成 SparkPlan
  type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
  type FunctionDescription = (FunctionIdentifier, ExpressionInfo, FunctionBuilder)
  type TableFunctionDescription = (FunctionIdentifier, ExpressionInfo, TableFunctionBuilder)
  type ColumnarRuleBuilder = SparkSession => ColumnarRule
  type QueryStagePrepRuleBuilder = SparkSession => Rule[SparkPlan]    // 物理树处理规则

  private[this] val columnarRuleBuilders = mutable.Buffer.empty[ColumnarRuleBuilder]
  private[this] val queryStagePrepRuleBuilders = mutable.Buffer.empty[QueryStagePrepRuleBuilder]

  /**
   * Build the override rules for columnar execution.
   */
  private[sql] def buildColumnarRules(session: SparkSession): Seq[ColumnarRule] = {
    columnarRuleBuilders.map(_.apply(session)).toSeq
  }

  /**
   * Build the override rules for the query stage preparation phase of adaptive query execution.
   */
  private[sql] def buildQueryStagePrepRules(session: SparkSession): Seq[Rule[SparkPlan]] = {
    queryStagePrepRuleBuilders.map(_.apply(session)).toSeq
  }

  // 注入一个规则，该规则可以覆盖执行器的列式执行。
  def injectColumnar(builder: ColumnarRuleBuilder): Unit = {
    columnarRuleBuilders += builder
  }

  // 规则可以覆盖自适应查询执行的查询阶段准备阶段。
  def injectQueryStagePrepRule(builder: QueryStagePrepRuleBuilder): Unit = {
    queryStagePrepRuleBuilders += builder
  }

  private[this] val resolutionRuleBuilders = mutable.Buffer.empty[RuleBuilder]

  // 使用给定的 [[SparkSession]] 构建分析器解析规则（`Rule`）
  private[sql] def buildResolutionRules(session: SparkSession): Seq[Rule[LogicalPlan]] = {
    resolutionRuleBuilders.map(_.apply(session)).toSeq
  }

  /**
   * 向 [[SparkSession]] 中注入一个分析器解析规则（`Rule`）构建器。
   * 这些分析器规则将在分析的解析阶段执行。
   */
  def injectResolutionRule(builder: RuleBuilder): Unit = {
    resolutionRuleBuilders += builder
  }

  private[this] val postHocResolutionRuleBuilders = mutable.Buffer.empty[RuleBuilder]

  /**
   * Build the analyzer post-hoc resolution `Rule`s using the given [[SparkSession]].
   */
  private[sql] def buildPostHocResolutionRules(session: SparkSession): Seq[Rule[LogicalPlan]] = {
    postHocResolutionRuleBuilders.map(_.apply(session)).toSeq
  }

  /**
   * 向 [[SparkSession]] 中注入一个分析器规则（`Rule`）构建器。
   * 这些分析器规则将在解析之后执行。
   */
  def injectPostHocResolutionRule(builder: RuleBuilder): Unit = {
    postHocResolutionRuleBuilders += builder
  }

  private[this] val checkRuleBuilders = mutable.Buffer.empty[CheckRuleBuilder]

  /**
   * Build the check analysis `Rule`s using the given [[SparkSession]].
   */
  private[sql] def buildCheckRules(session: SparkSession): Seq[LogicalPlan => Unit] = {
    checkRuleBuilders.map(_.apply(session)).toSeq
  }

  /**
   * 向 [[SparkSession]] 中注入一个分析检查规则（`Rule`）构建器。注入的规则将在分析阶段之后执行。
   * 分析检查规则用于检测逻辑计划（LogicalPlan）中的问题，并且在发现问题时应抛出异常。
   */
  def injectCheckRule(builder: CheckRuleBuilder): Unit = {
    checkRuleBuilders += builder
  }

  private[this] val optimizerRules = mutable.Buffer.empty[RuleBuilder]

  private[sql] def buildOptimizerRules(session: SparkSession): Seq[Rule[LogicalPlan]] = {
    optimizerRules.map(_.apply(session)).toSeq
  }

  /**
   * 向 [[SparkSession]] 中注入一个优化器规则（`Rule`）构建器。注入的规则将在操作优化批处理中执行。
   * 优化器规则用于提高已分析逻辑计划的质量；这些规则不应修改逻辑计划的结果。
   */
  def injectOptimizerRule(builder: RuleBuilder): Unit = {
    optimizerRules += builder
  }

  private[this] val preCBORules = mutable.Buffer.empty[RuleBuilder]

  private[sql] def buildPreCBORules(session: SparkSession): Seq[Rule[LogicalPlan]] = {
    preCBORules.map(_.apply(session)).toSeq
  }

  /**
   * 向 [[SparkSession]] 中注入一个重写逻辑计划的优化器规则（`Rule`）构建器。
   * 注入的规则将在操作优化批处理之后且在任何依赖统计数据的基于成本的优化规则之前执行一次。
   */
  def injectPreCBORule(builder: RuleBuilder): Unit = {
    preCBORules += builder
  }

  private[this] val plannerStrategyBuilders = mutable.Buffer.empty[StrategyBuilder]

  private[sql] def buildPlannerStrategies(session: SparkSession): Seq[Strategy] = {
    plannerStrategyBuilders.map(_.apply(session)).toSeq
  }

  /**
   * 向 [[SparkSession]] 中注入一个计划器策略（`Strategy`）构建器。注入的策略将用于将 `LogicalPlan`
   * 转换为可执行的 [[org.apache.spark.sql.execution.SparkPlan]]。
   */
  def injectPlannerStrategy(builder: StrategyBuilder): Unit = {
    plannerStrategyBuilders += builder
  }

  private[this] val parserBuilders = mutable.Buffer.empty[ParserBuilder]

  private[sql] def buildParser(
      session: SparkSession,
      initial: ParserInterface): ParserInterface = {
    parserBuilders.foldLeft(initial) { (parser, builder) =>
      builder(session, parser)
    }
  }

  /**
   * 向 [[SparkSession]] 中注入一个自定义解析器。请注意，构建器会传递一个会话和一个初始解析器。
   * 后者允许用户创建部分解析器并委托给基础解析器以实现完整性。
   * 如果用户注入更多解析器，则这些解析器会堆叠在一起。
   */
  def injectParser(builder: ParserBuilder): Unit = {
    parserBuilders += builder
  }

  private[this] val injectedFunctions = mutable.Buffer.empty[FunctionDescription]

  private[this] val injectedTableFunctions = mutable.Buffer.empty[TableFunctionDescription]

  private[sql] def registerFunctions(functionRegistry: FunctionRegistry) = {
    for ((name, expressionInfo, function) <- injectedFunctions) {
      functionRegistry.registerFunction(name, expressionInfo, function)
    }
    functionRegistry
  }


  private[sql] def registerTableFunctions(tableFunctionRegistry: TableFunctionRegistry) = {
    for ((name, expressionInfo, function) <- injectedTableFunctions) {
      tableFunctionRegistry.registerFunction(name, expressionInfo, function)
    }
    tableFunctionRegistry
  }

  /**
   * 在运行时向 [[org.apache.spark.sql.catalyst.analysis.FunctionRegistry]]
   * 注入一个自定义函数，适用于所有会话。
   */
  def injectFunction(functionDescription: FunctionDescription): Unit = {
    injectedFunctions += functionDescription
  }

  /**
   * 在运行时向 [[org.apache.spark.sql.catalyst.analysis.TableFunctionRegistry]]
   * 注入一个自定义函数，适用于所有会话。
   */
  def injectTableFunction(functionDescription: TableFunctionDescription): Unit = {
    injectedTableFunctions += functionDescription
  }
}
