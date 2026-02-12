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

import java.io.File
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Unstable
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry, TableFunctionRegistry}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.execution._
import org.apache.spark.sql.streaming.StreamingQueryManager
import org.apache.spark.sql.util.ExecutionListenerManager
import org.apache.spark.util.{DependencyUtils, Utils}

/**
 *
 * hive -> [[org.apache.spark.sql.hive.HiveSessionStateBuilder]]
 * in-memory -> [[SessionStateBuilder]]
 *
 * 一个类，用于保存给定 [[SparkSession]] 中的所有会话特定状态。
 *
 * @param sharedState                  跨会话共享的状态，例如全局视图管理器、外部目录。
 * @param conf                         SQLConf 特定的配置。
 * @param experimentalMethods          用于添加自定义规划策略和优化器的接口。
 * @param functionRegistry             用于管理用户注册函数的内部目录。
 * @param udfRegistration              暴露给用户用于注册用户自定义函数的接口。
 * @param catalogBuilder               用于创建管理表和数据库状态的内部目录的函数。
 * @param sqlParser                    从 SQL 文本中提取表达式、计划、表标识符等的解析器。
 * @param analyzerBuilder              用于创建逻辑查询计划分析器以解析未解析的属性和关系的函数。
 * @param optimizerBuilder             用于创建逻辑查询计划优化器的函数。
 * @param planner                      将优化的逻辑计划转换为物理计划的规划器。
 * @param streamingQueryManagerBuilder 用于创建流查询管理器以启动和停止流查询的函数。
 * @param listenerManager              用于注册自定义 [[org.apache.spark.sql.util.QueryExecutionListener]] 的接口。
 * @param resourceLoaderBuilder        用于创建会话共享资源加载器以加载 JAR、文件等的函数。
 * @param createQueryExecution         用于创建 QueryExecution 对象的函数。
 * @param createClone                  用于创建会话状态克隆的函数。
 */
private[sql] class SessionState(
    sharedState: SharedState,
    val conf: SQLConf,
    val experimentalMethods: ExperimentalMethods,          // 实现性的插装
    val functionRegistry: FunctionRegistry,                /** 函数注册   : [[SparkSessionExtensions.registerFunctions(FunctionRegistry.builtin.clone())]] */
    val tableFunctionRegistry: TableFunctionRegistry,      /** 表函数注册， 这些函数返回整个数据集 : SELECT * FROM range(1, 100); [[SparkSessionExtensions.registerTableFunctions(TableFunctionRegistry.builtin.clone())]] */
    val udfRegistration: UDFRegistration,                  /** [[UDFRegistration(functionRegistry)]]  */
    catalogBuilder: () => SessionCatalog,                  /** 对数据库，表，函数， 视图等操作的统一包装入口 : [[HiveSessionCatalog]]  */
    val sqlParser: ParserInterface,                        /** SQL 解析器 : [[SparkSessionExtensions.buildParser(session, new SparkSqlParser())]] */
    analyzerBuilder: () => Analyzer,                       /** 计划树元数据解析绑定 : [[HiveSessionStateBuilder.analyzer]]  */
    optimizerBuilder: () => Optimizer,                     /** 计划树优化器 : [[BaseSessionStateBuilder.optimizer]]  */
    val planner: SparkPlanner,                             /** 物理树适配器 : [[HiveSessionStateBuilder.planner]]  */
    val streamingQueryManagerBuilder: () => StreamingQueryManager,
    val listenerManager: ExecutionListenerManager,                                        // SparkSQL 执行监听器
    resourceLoaderBuilder: () => SessionResourceLoader,
    createQueryExecution: (LogicalPlan, CommandExecutionMode.Value) => QueryExecution,    // 基于LogicalPlan 的计划执行树
    createClone: (SparkSession, SessionState) => SessionState,
    val columnarRules: Seq[ColumnarRule],
    val queryStagePrepRules: Seq[Rule[SparkPlan]]) {

  /** 以下字段是惰性的，以避免在创建 SessionState 时创建 Hive 客户端。 */
  lazy val catalog: SessionCatalog = catalogBuilder()

  /** 解析器 [[BaseSessionStateBuilder.analyzer]] */
  lazy val analyzer: Analyzer = analyzerBuilder()

  /** 优化器 [[BaseSessionStateBuilder.optimizer]] */
  lazy val optimizer: Optimizer = optimizerBuilder()

  // Spark 资源管理
  lazy val resourceLoader: SessionResourceLoader = resourceLoaderBuilder()

  // streamingQueryManager 是惰性的，以避免在连接到 ThriftServer 时为每个会话创建 StreamingQueryManager。
  lazy val streamingQueryManager: StreamingQueryManager = streamingQueryManagerBuilder()

  /** [[CatalogManager(V2SessionCatalog(catalog), catalog)]]  */
  def catalogManager: CatalogManager = analyzer.catalogManager

  // Hadoop Configuration
  def newHadoopConf(): Configuration = SessionState.newHadoopConf(sharedState.sparkContext.hadoopConfiguration, conf)

  def newHadoopConfWithOptions(options: Map[String, String]): Configuration = {
    val hadoopConf = newHadoopConf()
    options.foreach { case (k, v) =>
      if ((v ne null) && k != "path" && k != "paths") {
        hadoopConf.set(k, v)
      }
    }
    hadoopConf
  }

  /** 获取 `SessionState` 的相同副本并将其与给定的 `SparkSession` 关联 */
  def clone(newSparkSession: SparkSession): SessionState = createClone(newSparkSession, this)

  // ------------------------------------------------------
  //  辅助方法，部分是 2.0 之前版本遗留下来的
  // ------------------------------------------------------

  def executePlan(plan: LogicalPlan, mode: CommandExecutionMode.Value = CommandExecutionMode.ALL): QueryExecution =
    createQueryExecution(plan, mode)
}

private[sql] object SessionState {
  def newHadoopConf(hadoopConf: Configuration, sqlConf: SQLConf): Configuration = {
    val newHadoopConf = new Configuration(hadoopConf)
    sqlConf.getAllConfs.foreach { case (k, v) => if (v ne null) newHadoopConf.set(k, v) }
    newHadoopConf
  }
}

/** [[BaseSessionStateBuilder]] 具体实现 */
@Unstable
class SessionStateBuilder(
    session: SparkSession,
    parentState: Option[SessionState])
  extends BaseSessionStateBuilder(session, parentState) {
  override protected def newBuilder: NewBuilder = new SessionStateBuilder(_, _)
}

/** Session 共享 [[FunctionResourceLoader]]. */
@Unstable
class SessionResourceLoader(session: SparkSession) extends FunctionResourceLoader {
  override def loadResource(resource: FunctionResource): Unit = {
    resource.resourceType match {
      case JarResource => addJar(resource.uri)
      case FileResource => session.sparkContext.addFile(resource.uri)
      case ArchiveResource => session.sparkContext.addArchive(resource.uri)
    }
  }

  def resolveJars(path: URI): Seq[String] = {
    path.getScheme match {
      case "ivy" => DependencyUtils.resolveMavenDependencies(path)
      case _ => path.toString :: Nil
    }
  }

  /**
   * 将 jar 路径添加到 [[SparkContext]] 和类加载器。
   *
   * 注意：此方法似乎不访问任何会话状态，但基于 Hive 的 `SessionState` 需要将 jar 添加到其 hive 客户端以用于当前会话。
   * 因此，它仍然需要在[[SessionState]] 中。
   */
  def addJar(path: String): Unit = {
    val uri = Utils.resolveURI(path)
    resolveJars(uri).foreach { p =>
      session.sparkContext.addJar(p)
      val uri = new Path(p).toUri
      val jarURL = if (uri.getScheme == null) {
        // `path` 是没有 URL 方案的本地文件路径
        new File(p).toURI.toURL
      } else {
        // `path` 是带有方案的 URL
        uri.toURL
      }
      session.sharedState.jarClassLoader.addURL(jarURL)
    }
    Thread.currentThread().setContextClassLoader(session.sharedState.jarClassLoader)
  }
}
