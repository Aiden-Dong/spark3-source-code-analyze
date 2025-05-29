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
package org.apache.spark.sql.internal

import org.apache.spark.annotation.Unstable
import org.apache.spark.sql.{ExperimentalMethods, SparkSession, UDFRegistration, _}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry, ReplaceCharWithVarchar, ResolveSessionCatalog, TableFunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{FunctionExpressionBuilder, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.{ColumnarRule, CommandExecutionMode, QueryExecution, SparkOptimizer, SparkPlan, SparkPlanner, SparkSqlParser}
import org.apache.spark.sql.execution.aggregate.{ResolveEncodersInScalaAgg, ScalaUDAF}
import org.apache.spark.sql.execution.analysis.DetectAmbiguousSelfJoin
import org.apache.spark.sql.execution.command.CommandCheck
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2.{TableCapabilityCheck, V2SessionCatalog}
import org.apache.spark.sql.execution.streaming.ResolveWriteToStream
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.streaming.StreamingQueryManager
import org.apache.spark.sql.util.ExecutionListenerManager

/**
 * Builder class that coordinates construction of a new [[SessionState]].
 *
 * The builder explicitly defines all components needed by the session state, and creates a session
 * state when `build` is called. Components should only be initialized once. This is not a problem
 * for most components as they are only used in the `build` function. However some components
 * (`conf`, `catalog`, `functionRegistry`, `experimentalMethods` & `sqlParser`) are as dependencies
 * for other components and are shared as a result. These components are defined as lazy vals to
 * make sure the component is created only once.
 *
 * A developer can modify the builder by providing custom versions of components, or by using the
 * hooks provided for the analyzer, optimizer & planner. There are some dependencies between the
 * components (they are documented per dependency), a developer should respect these when making
 * modifications in order to prevent initialization problems.
 *
 * A parent [[SessionState]] can be used to initialize the new [[SessionState]]. The new session
 * state will clone the parent sessions state's `conf`, `functionRegistry`, `experimentalMethods`
 * and `catalog` fields. Note that the state is cloned when `build` is called, and not before.
 */
@Unstable
abstract class BaseSessionStateBuilder(
    val session: SparkSession,
    val parentState: Option[SessionState]) {
  type NewBuilder = (SparkSession, Option[SessionState]) => BaseSessionStateBuilder

  /**
   * Function that produces a new instance of the `BaseSessionStateBuilder`. This is used by the
   * [[SessionState]]'s clone functionality. Make sure to override this when implementing your own
   * [[SessionStateBuilder]].
   */
  protected def newBuilder: NewBuilder

  /**
   * Session extensions defined in the [[SparkSession]].
   */
  protected def extensions: SparkSessionExtensions = session.extensions

  /**
   * SQL-specific key-value configurations.
   *
   * These either get cloned from a pre-existing instance or newly created. The conf is merged
   * with its [[SparkConf]] only when there is no parent session.
   */
  protected lazy val conf: SQLConf = {
    parentState.map { s =>
      val cloned = s.conf.clone()
      if (session.sparkContext.conf.get(StaticSQLConf.SQL_LEGACY_SESSION_INIT_WITH_DEFAULTS)) {
        SQLConf.mergeSparkConf(cloned, session.sparkContext.conf)
      }
      cloned
    }.getOrElse {
      val conf = new SQLConf
      SQLConf.mergeSparkConf(conf, session.sharedState.conf)
      // the later added configs to spark conf shall be respected too
      SQLConf.mergeNonStaticSQLConfigs(conf, session.sparkContext.conf.getAll.toMap)
      SQLConf.mergeNonStaticSQLConfigs(conf, session.initialSessionOptions)
      conf
    }
  }

  /**
   * Internal catalog managing functions registered by the user.
   *
   * This either gets cloned from a pre-existing version or cloned from the built-in registry.
   */
  protected lazy val functionRegistry: FunctionRegistry = {
    parentState.map(_.functionRegistry.clone())
      .getOrElse(extensions.registerFunctions(FunctionRegistry.builtin.clone()))
  }

  /**
   * Internal catalog managing functions registered by the user.
   *
   * This either gets cloned from a pre-existing version or cloned from the built-in registry.
   */
  protected lazy val tableFunctionRegistry: TableFunctionRegistry = {
    parentState.map(_.tableFunctionRegistry.clone())
      .getOrElse(extensions.registerTableFunctions(TableFunctionRegistry.builtin.clone()))
  }

  /**
   * Experimental methods that can be used to define custom optimization rules and custom planning
   * strategies.
   *
   * This either gets cloned from a pre-existing version or newly created.
   */
  protected lazy val experimentalMethods: ExperimentalMethods = {
    parentState.map(_.experimentalMethods.clone()).getOrElse(new ExperimentalMethods)
  }

  /**
   * Parser that extracts expressions, plans, table identifiers etc. from SQL texts.
   *
   * Note: this depends on the `conf` field.
   */
  protected lazy val sqlParser: ParserInterface = {
    extensions.buildParser(session, new SparkSqlParser())
  }

  /**
   * ResourceLoader that is used to load function resources and jars.
   */
  protected lazy val resourceLoader: SessionResourceLoader = new SessionResourceLoader(session)

  /**
   * Catalog for managing table and database states. If there is a pre-existing catalog, the state
   * of that catalog (temp tables & current database) will be copied into the new catalog.
   *
   * Note: this depends on the `conf`, `functionRegistry` and `sqlParser` fields.
   */
  protected lazy val catalog: SessionCatalog = {
    val catalog = new SessionCatalog(
      () => session.sharedState.externalCatalog,
      () => session.sharedState.globalTempViewManager,
      functionRegistry,
      tableFunctionRegistry,
      SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
      sqlParser,
      resourceLoader,
      new SparkUDFExpressionBuilder)
    parentState.foreach(_.catalog.copyStateTo(catalog))
    catalog
  }

  protected lazy val v2SessionCatalog = new V2SessionCatalog(catalog)

  protected lazy val catalogManager = new CatalogManager(v2SessionCatalog, catalog)

  /**
   * Interface exposed to the user for registering user-defined functions.
   *
   * Note 1: The user-defined functions must be deterministic.
   * Note 2: This depends on the `functionRegistry` field.
   */
  protected def udfRegistration: UDFRegistration = new UDFRegistration(functionRegistry)

  /**
   * 用于解析未解析属性和关系的逻辑查询计划分析器。
   * 注意：这依赖于 conf 和 catalog 字段。
   */
  protected def analyzer: Analyzer = new Analyzer(catalogManager) {
    override val extendedResolutionRules: Seq[Rule[LogicalPlan]] = {
        // 识别并绑定数据源表‌，确保查询中引用的表或视图能够正确关联到底层存储的元数据或物理文件。
        // (表路径解析与元数据绑定, 表名作用域管理, 元数据验证与推断)
        new FindDataSourceTable(session) +:
        // ‌文件型数据源上直接执行 SQL 查询‌ 的核心处理模块，其核心目标是通过动态解析文件格式、推断元数据并生成优化的执行计划，
        // 实现无需预导入数据库即可对文件（如 CSV、Parquet、JSON 等）进行 SQL 查询。
        // (文件格式解析与适配, 元数据自动推断, 查询优化与执行, 多文件联合查询,  执行计划生成)
        new ResolveSQLOnFile(session) +:
        // ‌兼容新旧版数据源 API 的容错机制，其核心功能是 ‌在 DataSource V2 接口无法处理特定操作或数据源时，自动回退到 V1 接口
        // (版本兼容性保障, 查询优化与执行流程整合, )
        new FallBackFileSourceV2(session) +:
        // 是用于 Scala 聚合操作中编码器（Encoder）的动态解析与注入‌ 的关键组件，
        // 其核心作用是通过隐式转换机制自动推断并绑定聚合操作所需的数据序列化逻辑，确保分布式计算框架（如 Spark）能够高效处理 Scala 原生数据结构。
        // (编码器自动推断, 聚合逻辑优化, 与分布式框架的集成‌)
        ResolveEncodersInScalaAgg +:
        // 会话级元数据解析与管理‌ 的核心组件，其核心功能是通过会话隔离的元数据存储（SessionCatalog）实现表、视图、函数等对象的名称解析与绑定，
        // 确保 SQL 查询在运行时能够正确访问元数据并保障多会话环境下的隔离性。
        // (元数据解析与绑定, 多级元数据隔离, 元数据注册与更新, 错误处理与兼容性)
        new ResolveSessionCatalog(catalogManager) +:
        ResolveWriteToStream +:
        /**
         *  来自 [[SparkSessionExtensions.injectResolutionRule]] 的自定义规则解析器
         */
        customResolutionRules
    }

    override val postHocResolutionRules: Seq[Rule[LogicalPlan]] = {
        // 用于检测并处理自连接（Self-Join）操作中可能存在的 ‌列引用歧义问题‌，确保查询语义的明确性
        DetectAmbiguousSelfJoin +:
        // 对表创建（CREATE TABLE）操作进行预处理，确保语法、语义及元数据配置的合法性，为后续执行提供标准化的逻辑计划
        // (语法与语义校验, 元数据预处理, 逻辑计划转换, 与 Catalog 的交互)
        PreprocessTableCreation(session) +:
        // 用于预处理 INSERT INTO 操作，确保插入操作的目标表与数据源的兼容性，并优化执行逻辑。
        // (目标表验证与元数据解析, 数据源与目标表字段映射, 执行计划优化, 异常处理与语义修正)
        PreprocessTableInsertion +:
        // 解析和优化数据源（DataSource）相关操作‌ 的组件，主要功能涵盖数据源元数据解析、执行计划适配及性能优化
        // (数据源元数据解析与验证, 执行计划优化, 连接管理与资源优化, 异常处理与语义修正)
        DataSourceAnalysis +:
        // 主要用于 ‌自动将 CHAR 类型替换为 VARCHAR 类型‌，以优化存储效率、提升查询性能并增强数据兼容性。
        // (数据类型优化, 数据兼容性适配, 执行计划优化)
        ReplaceCharWithVarchar +:
        /**
         *  来自 [[SparkSessionExtensions.injectPostHocResolutionRule]] 的自定义规则解析器
         */
        customPostHocResolutionRules
    }

    override val extendedCheckRules: Seq[LogicalPlan => Unit] =
      PreWriteCheck +:
        PreReadCheck +:
        HiveOnlyCheck +:
        TableCapabilityCheck +:
        CommandCheck +:
        customCheckRules
  }

  /**
   * Custom resolution rules to add to the Analyzer. Prefer overriding this instead of creating
   * your own Analyzer.
   *
   * Note that this may NOT depend on the `analyzer` function.
   */
  protected def customResolutionRules: Seq[Rule[LogicalPlan]] = {
    extensions.buildResolutionRules(session)
  }

  /**
   * Custom post resolution rules to add to the Analyzer. Prefer overriding this instead of
   * creating your own Analyzer.
   *
   * Note that this may NOT depend on the `analyzer` function.
   */
  protected def customPostHocResolutionRules: Seq[Rule[LogicalPlan]] = {
    extensions.buildPostHocResolutionRules(session)
  }

  /**
   * Custom check rules to add to the Analyzer. Prefer overriding this instead of creating
   * your own Analyzer.
   *
   * Note that this may NOT depend on the `analyzer` function.
   */
  protected def customCheckRules: Seq[LogicalPlan => Unit] = {
    extensions.buildCheckRules(session)
  }

  /**
   * Logical query plan optimizer.
   *
   * Note: this depends on `catalog` and `experimentalMethods` fields.
   */
  protected def optimizer: Optimizer = {
    new SparkOptimizer(catalogManager, catalog, experimentalMethods) {
      override def earlyScanPushDownRules: Seq[Rule[LogicalPlan]] =
        super.earlyScanPushDownRules ++ customEarlyScanPushDownRules

      override def preCBORules: Seq[Rule[LogicalPlan]] =
        super.preCBORules ++ customPreCBORules

      override def extendedOperatorOptimizationRules: Seq[Rule[LogicalPlan]] =
        super.extendedOperatorOptimizationRules ++ customOperatorOptimizationRules
    }
  }

  /**
   * Custom operator optimization rules to add to the Optimizer. Prefer overriding this instead
   * of creating your own Optimizer.
   *
   * Note that this may NOT depend on the `optimizer` function.
   */
  protected def customOperatorOptimizationRules: Seq[Rule[LogicalPlan]] = {
    extensions.buildOptimizerRules(session)
  }

  /**
   * Custom early scan push down rules to add to the Optimizer. Prefer overriding this instead
   * of creating your own Optimizer.
   *
   * Note that this may NOT depend on the `optimizer` function.
   */
  protected def customEarlyScanPushDownRules: Seq[Rule[LogicalPlan]] = Nil

  /**
   * 用于在算子优化之后、基于成本的优化（CBO）之前重写计划的定制规则。
   * 建议优先重写此方法而非创建自己的优化器。
   * 注意：此规则不得依赖于optimizer函数。
   */
  protected def customPreCBORules: Seq[Rule[LogicalPlan]] = {
    extensions.buildPreCBORules(session)
  }

  /**
   * Planner that converts optimized logical plans to physical plans.
   *
   * Note: this depends on the `conf` and `experimentalMethods` fields.
   */
  protected def planner: SparkPlanner = {
    new SparkPlanner(session, experimentalMethods) {
      override def extraPlanningStrategies: Seq[Strategy] =
        super.extraPlanningStrategies ++ customPlanningStrategies
    }
  }

  /**
   * Custom strategies to add to the planner. Prefer overriding this instead of creating
   * your own Planner.
   *
   * Note that this may NOT depend on the `planner` function.
   */
  protected def customPlanningStrategies: Seq[Strategy] = {
    extensions.buildPlannerStrategies(session)
  }

  protected def columnarRules: Seq[ColumnarRule] = {
    extensions.buildColumnarRules(session)
  }

  protected def queryStagePrepRules: Seq[Rule[SparkPlan]] = {
    extensions.buildQueryStagePrepRules(session)
  }

  /**
   * Create a query execution object.
   */
  protected def createQueryExecution:
    (LogicalPlan, CommandExecutionMode.Value) => QueryExecution =
      (plan, mode) => new QueryExecution(session, plan, mode = mode)

  /**
   * Interface to start and stop streaming queries.
   */
  protected def streamingQueryManager: StreamingQueryManager =
    new StreamingQueryManager(session, conf)

  /**
   * An interface to register custom [[org.apache.spark.sql.util.QueryExecutionListener]]s
   * that listen for execution metrics.
   *
   * This gets cloned from parent if available, otherwise a new instance is created.
   */
  protected def listenerManager: ExecutionListenerManager = {
    parentState.map(_.listenerManager.clone(session, conf)).getOrElse(
      new ExecutionListenerManager(session, conf, loadExtensions = true))
  }

  /**
   * Function used to make clones of the session state.
   */
  protected def createClone: (SparkSession, SessionState) => SessionState = {
    val createBuilder = newBuilder
    (session, state) => createBuilder(session, Option(state)).build()
  }

  /**
   * Build the [[SessionState]].
   */
  def build(): SessionState = {
    new SessionState(
      session.sharedState,
      conf,
      experimentalMethods,
      functionRegistry,
      tableFunctionRegistry,
      udfRegistration,
      () => catalog,
      sqlParser,
      () => analyzer,
      () => optimizer,
      planner,
      () => streamingQueryManager,
      listenerManager,
      () => resourceLoader,
      createQueryExecution,
      createClone,
      columnarRules,
      queryStagePrepRules)
  }
}

/**
 * Helper class for using SessionStateBuilders during tests.
 */
private[sql] trait WithTestConf { self: BaseSessionStateBuilder =>
  def overrideConfs: Map[String, String]

  override protected lazy val conf: SQLConf = {
    val overrideConfigurations = overrideConfs
    parentState.map { s =>
      val cloned = s.conf.clone()
      if (session.sparkContext.conf.get(StaticSQLConf.SQL_LEGACY_SESSION_INIT_WITH_DEFAULTS)) {
        SQLConf.mergeSparkConf(conf, session.sparkContext.conf)
      }
      cloned
    }.getOrElse {
      val conf = new SQLConf {
        clear()
        override def clear(): Unit = {
          super.clear()
          // Make sure we start with the default test configs even after clear
          overrideConfigurations.foreach { case (key, value) => setConfString(key, value) }
        }
      }
      SQLConf.mergeSparkConf(conf, session.sparkContext.conf)
      conf
    }
  }
}

class SparkUDFExpressionBuilder extends FunctionExpressionBuilder {
  override def makeExpression(name: String, clazz: Class[_], input: Seq[Expression]): Expression = {
    if (classOf[UserDefinedAggregateFunction].isAssignableFrom(clazz)) {
      val expr = ScalaUDAF(
        input,
        clazz.getConstructor().newInstance().asInstanceOf[UserDefinedAggregateFunction],
        udafName = Some(name))
      // Check input argument size
      if (expr.inputTypes.size != input.size) {
        throw QueryCompilationErrors.invalidFunctionArgumentsError(
          name, expr.inputTypes.size.toString, input.size)
      }
      expr
    } else {
      throw QueryCompilationErrors.noHandlerForUDAFError(clazz.getCanonicalName)
    }
  }
}
