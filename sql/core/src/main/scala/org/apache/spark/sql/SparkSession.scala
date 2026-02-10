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

import java.io.Closeable
import java.util.{ServiceLoader, UUID}
import java.util.concurrent.TimeUnit._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag
import scala.util.control.NonFatal

import org.apache.spark.{SPARK_VERSION, SparkConf, SparkContext, TaskContext}
import org.apache.spark.annotation.{DeveloperApi, Experimental, Stable, Unstable}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{ConfigEntry, EXECUTOR_ALLOW_SPARK_CONTEXT}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Range}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.ExternalCommandRunner
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.ExternalCommandExecutor
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.internal._
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.ExecutionListenerManager
import org.apache.spark.util.{CallSite, Utils}

/**
 * 使用 Dataset 和 DataFrame API 编程 Spark 的入口点。
 *
 * 在已经预先创建的环境中（例如 REPL、notebooks），使用 builder 来获取现有会话：
 *
 * {{{
 *   SparkSession.builder().getOrCreate()
 * }}}
 *
 * builder 也可以用于创建新会话：
 *
 * {{{
 *   SparkSession.builder
 *     .master("local")
 *     .appName("Word Count")
 *     .config("spark.some.config.option", "some-value")
 *     .getOrCreate()
 * }}}
 *
 * @param sparkContext 与此 Spark 会话关联的 Spark 上下文。
 * @param existingSharedState 如果提供，使用现有的共享状态而不是创建新的。
 * @param parentSessionState 如果提供，从父会话继承所有会话状态（即临时视图、SQL 配置、UDF 等）。
 */
@Stable
class SparkSession private(
    @transient val sparkContext: SparkContext,
    @transient private val existingSharedState: Option[SharedState],
    @transient private val parentSessionState: Option[SessionState],
    @transient private[sql] val extensions: SparkSessionExtensions,
    @transient private[sql] val initialSessionOptions: Map[String, String])
  extends Serializable with Closeable with Logging { self =>

  // 构造此 SparkSession 的调用位置。
  private val creationSite: CallSite = Utils.getCallSite()

  /**
   * 在 Pyspark 中使用的构造函数。包含 Spark Session Extensions 的显式应用，否则只会在 getOrCreate 期间发生。
   * 我们不能将其添加到默认构造函数中， 因为那会导致每个新会话在当前运行的扩展上重新调用 Spark Session Extensions。
   */
  private[sql] def this(
      sc: SparkContext,
      initialSessionOptions: java.util.HashMap[String, String]) = {
    this(sc, None, None,
      SparkSession.applyExtensions(
        sc.getConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS).getOrElse(Seq.empty),
        new SparkSessionExtensions), initialSessionOptions.asScala.toMap)
  }

  private[sql] def this(sc: SparkContext) = this(sc, new java.util.HashMap[String, String]())

  private[sql] val sessionUUID: String = UUID.randomUUID.toString

  sparkContext.assertNotStopped()

  // 如果没有活动的 SparkSession，使用默认的 SQL 配置。否则，使用会话的配置。
  SQLConf.setSQLConfGetter(() => {
    SparkSession.getActiveSession.filterNot(_.sparkContext.isStopped).map(_.sessionState.conf)
      .getOrElse(SQLConf.getFallbackConf)
  })


  /**
   * 此应用程序运行的 Spark 版本。
   */
  def version: String = SPARK_VERSION

  /* ----------------------- *
   |  会话相关状态             |
   * ----------------------- */

  /**
   * 整个应用共享一个，在会话之间共享的状态，包括 SparkContext、缓存数据、监听器以及与外部系统交互的catalog。
   * 这是 Spark 内部使用的，接口稳定性不受保证。
   */
  @Unstable
  @transient
  lazy val sharedState: SharedState = {
    existingSharedState.getOrElse(new SharedState(sparkContext, initialSessionOptions))
  }
  /**
   * 在会话之间隔离的状态，包括 SQL 配置、临时表、已注册的函数以及
   * 接受 [[org.apache.spark.sql.internal.SQLConf]] 的所有其他内容。
   * 如果 parentSessionState 不为 null，则 SessionState 将是父级的副本。
   * 这是 Spark 内部使用的，接口稳定性不受保证。
   */
  @Unstable
  @transient
  lazy val sessionState: SessionState = {
    parentSessionState
      .map(_.clone(this))
      .getOrElse {
        val state = SparkSession.instantiateSessionState(
          SparkSession.sessionStateClassName(sharedState.conf),
          self)
        state
      }
  }

  /**
   * 此会话的包装版本，以 [[SQLContext]] 的形式，用于向后兼容。
   */
  @transient
  val sqlContext: SQLContext = new SQLContext(this)

  /**
   * Spark 的运行时配置接口。
   *
   * 这是用户可以获取和设置所有与 Spark SQL 相关的 Spark 和 Hadoop
   * 配置的接口。获取配置值时，
   * 如果有的话，默认为底层 `SparkContext` 中设置的值。
   */
  @transient lazy val conf: RuntimeConfig = new RuntimeConfig(sessionState.conf)


  /**
   * 用于注册自定义 [[org.apache.spark.sql.util.QueryExecutionListener]] 以监听执行指标的接口。
   */
  def listenerManager: ExecutionListenerManager = sessionState.listenerManager

  /**
   * :: Experimental ::
   * 一组被认为是实验性的方法，但可用于挂钩到查询规划器以实现高级功能。
   */
  @Experimental
  @Unstable
  def experimental: ExperimentalMethods = sessionState.experimentalMethods

  /**
   * 用于注册用户自定义函数（UDF）的方法集合。
   *
   * 以下示例将 Scala 闭包注册为 UDF：
   * {{{
   *   sparkSession.udf.register("myUDF", (arg1: Int, arg2: String) => arg2 + arg1)
   * }}}
   *
   * 以下示例在 Java 中注册 UDF：
   * {{{
   *   sparkSession.udf().register(
   *       "myUDF",
   *       (Integer arg1, String arg2) -> arg2 + arg1,
   *       DataTypes.StringType
   *    );
   * }}}
   *
   * @note 用户自定义函数必须是确定性的。
   *       由于优化，重复调用可能会被消除，或者函数甚至可能被调用的次数超过它在查询中出现的次数。
   */
  def udf: UDFRegistration = sessionState.udfRegistration

  /**
   * 返回一个 `StreamingQueryManager`，允许管理 `this` 上所有活动的`StreamingQuery`。
   */
  @Unstable
  def streams: StreamingQueryManager = sessionState.streamingQueryManager


  /**
   * 启动一个新会话，具有隔离的 SQL 配置、临时表、已注册的函数，
   * 但共享底层的 `SparkContext` 和缓存数据。
   *
   * @note 除了 `SparkContext` 之外，所有ShareState都是延迟初始化的。
   * 此方法将强制初始化共享状态，以确保父会话和子会话使用相同的共享状态设置。如果底层目录
   * 实现是 Hive，这将初始化 metastore，这可能需要一些时间。
   *
   * @since 2.0.0
   */
  def newSession(): SparkSession = {
    new SparkSession(
      sparkContext,
      Some(sharedState),
      parentSessionState = None,
      extensions,
      initialSessionOptions)
  }

  /**
   * 创建此 `SparkSession` 的相同副本，共享底层的 `SparkContext`和ShareState。
   * 此会话的所有状态（即 SQL 配置、临时表、 已注册的函数）都会被复制，克隆的会话使用与此会话相同的共享状态设置。
   * 克隆的会话独立于此会话，即任一会话中的任何非全局更改都不会反映在另一个会话中。
   *
   * @note 除了 `SparkContext` 之外，所有共享状态都是延迟初始化的。
   * 此方法将强制初始化共享状态，以确保父会话和子会话使用相同的共享状态设置。
   * 如果底层目录实现是 Hive，这将初始化 metastore，这可能需要一些时间。
   */
  private[sql] def cloneSession(): SparkSession = {
    val result = new SparkSession(
      sparkContext,
      Some(sharedState),
      Some(sessionState),
      extensions,
      Map.empty)
    result.sessionState // force copy of SessionState
    result
  }

  /* --------------------------------- *
   |  创建 DataFrame 的方法  |
   * --------------------------------- */

  /**
   * 返回一个没有行或列的 `DataFrame`。
   */
  @transient
  lazy val emptyDataFrame: DataFrame = Dataset.ofRows(self, LocalRelation())

  /**
   * 创建一个包含零个元素的类型为 T 的新 [[Dataset]]。
   */
  def emptyDataset[T: Encoder]: Dataset[T] = {
    val encoder = implicitly[Encoder[T]]
    new Dataset(self, LocalRelation(encoder.schema.toAttributes), encoder)
  }

  // 从 Product 的 RDD（例如 case 类、元组）创建 `DataFrame`。
  def createDataFrame[A <: Product : TypeTag](rdd: RDD[A]): DataFrame = withActive {
    val encoder = Encoders.product[A]
    Dataset.ofRows(self, ExternalRDD(rdd, self)(encoder))
  }

  // 从 Product 的本地 Seq 创建 `DataFrame`。
  def createDataFrame[A <: Product : TypeTag](data: Seq[A]): DataFrame = withActive {
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val attributeSeq = schema.toAttributes
    Dataset.ofRows(self, LocalRelation.fromProduct(attributeSeq, data))
  }

  /**
   * :: DeveloperApi ::
   * 使用给定的 schema 从包含 [[Row]] 的 `RDD` 创建 `DataFrame`。
   * 重要的是要确保提供的 RDD 的每个 [[Row]] 的结构与提供的 schema 匹配。否则，将会出现运行时异常。
   * 示例：
   * {{{
   *  import org.apache.spark.sql._
   *  import org.apache.spark.sql.types._
   *  val sparkSession = new org.apache.spark.sql.SparkSession(sc)
   *
   *  val schema =
   *    StructType(
   *      StructField("name", StringType, false) ::
   *      StructField("age", IntegerType, true) :: Nil)
   *
   *  val people =
   *    sc.textFile("examples/src/main/resources/people.txt").map(
   *      _.split(",")).map(p => Row(p(0), p(1).trim.toInt))
   *  val dataFrame = sparkSession.createDataFrame(people, schema)
   *  dataFrame.printSchema
   *  // root
   *  // |-- name: string (nullable = false)
   *  // |-- age: integer (nullable = true)
   *
   *  dataFrame.createOrReplaceTempView("people")
   *  sparkSession.sql("select name from people").collect.foreach(println)
   * }}}
   */
  @DeveloperApi
  def createDataFrame(rowRDD: RDD[Row], schema: StructType): DataFrame = withActive {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(schema).asInstanceOf[StructType]
    // TODO: use MutableProjection when rowRDD is another DataFrame and the applied
    // schema differs from the existing schema on any field data type.
    val encoder = RowEncoder(replaced)       // 构建 Encoder
    val toRow = encoder.createSerializer()   // 获取序列化工具
    val catalystRows = rowRDD.map(toRow)     // 将 Row 转变成 InternalRow
    internalCreateDataFrame(catalystRows.setName(rowRDD.name), schema)
  }

  /**
   * :: DeveloperApi ::
   * 使用给定的 schema 从包含 [[Row]] 的 `java.util.List` 创建 `DataFrame`。
   * 重要的是要确保提供的 List 的每个 [[Row]] 的结构与提供的 schema 匹配。否则，将会出现运行时异常。
   */
  @DeveloperApi
  def createDataFrame(rowRDD: JavaRDD[Row], schema: StructType): DataFrame = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(schema).asInstanceOf[StructType]
    createDataFrame(rowRDD.rdd, replaced)
  }

  /**
   * :: DeveloperApi ::
   * 使用给定的 schema 从包含 [[Row]] 的 `java.util.List` 创建 `DataFrame`。
   * 重要的是要确保提供的 List 的每个 [[Row]] 的结构与提供的 schema 匹配。否则，将会出现运行时异常。
   */
  @DeveloperApi
  def createDataFrame(rows: java.util.List[Row], schema: StructType): DataFrame = withActive {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(schema).asInstanceOf[StructType]
    Dataset.ofRows(self, LocalRelation.fromExternalRows(replaced.toAttributes, rows.asScala.toSeq))
  }

  /**
   * 将 schema 应用于 Java Bean 的 RDD。
   *
   * 警告：由于 Java Bean 中的字段没有保证的顺序，
   * SELECT * 查询将以未定义的顺序返回列。
   */
  def createDataFrame(rdd: RDD[_], beanClass: Class[_]): DataFrame = withActive {
    val attributeSeq: Seq[AttributeReference] = getSchema(beanClass)
    val className = beanClass.getName
    val rowRdd = rdd.mapPartitions { iter =>
    // BeanInfo is not serializable so we must rediscover it remotely for each partition.
      SQLContext.beansToRows(iter, Utils.classForName(className), attributeSeq)
    }
    Dataset.ofRows(self, LogicalRDD(attributeSeq, rowRdd.setName(rdd.name))(self))
  }

  /**
   * 将 schema 应用于 Java Bean 的 RDD。
   *
   * 警告：由于 Java Bean 中的字段没有保证的顺序， SELECT * 查询将以未定义的顺序返回列。
   */
  def createDataFrame(rdd: JavaRDD[_], beanClass: Class[_]): DataFrame = {
    createDataFrame(rdd.rdd, beanClass)
  }

  /**
   * 将 schema 应用于 Java Bean 的 RDD。
   *
   * 警告：由于 Java Bean 中的字段没有保证的顺序， SELECT * 查询将以未定义的顺序返回列。
   */
  def createDataFrame(data: java.util.List[_], beanClass: Class[_]): DataFrame = withActive {
    val attrSeq = getSchema(beanClass)
    val rows = SQLContext.beansToRows(data.asScala.iterator, beanClass, attrSeq)
    Dataset.ofRows(self, LocalRelation(attrSeq, rows.toSeq))
  }

  /**
   * 将为外部数据源创建的 `BaseRelation` 转换为 `DataFrame`。
   */
  def baseRelationToDataFrame(baseRelation: BaseRelation): DataFrame = {
    Dataset.ofRows(self, LogicalRelation(baseRelation))
  }

  /* ------------------------------- *
   |  创建 DataSet 的方法  |
   * ------------------------------- */

  /**
   * 从给定类型的本地 Seq 数据创建 [[Dataset]]。此方法需要一个编码器（用于将类型 `T` 的 JVM 对象转换为 Spark SQL 内部表示）
   * 通常通过 `SparkSession` 的隐式转换自动创建，或者可以通过调用 [[Encoders]] 上的静态方法显式创建。
   *
   * == 示例 ==
   *
   * {{{
   *
   *   import spark.implicits._
   *   case class Person(name: String, age: Long)
   *   val data = Seq(Person("Michael", 29), Person("Andy", 30), Person("Justin", 19))
   *   val ds = spark.createDataset(data)
   *
   *   ds.show()
   *   // +-------+---+
   *   // |   name|age|
   *   // +-------+---+
   *   // |Michael| 29|
   *   // |   Andy| 30|
   *   // | Justin| 19|
   *   // +-------+---+
   * }}}
   *
   * @since 2.0.0
   */
  def createDataset[T : Encoder](data: Seq[T]): Dataset[T] = {
    val enc = encoderFor[T]
    val toRow = enc.createSerializer()
    val attributes = enc.schema.toAttributes
    val encoded = data.map(d => toRow(d).copy())
    val plan = new LocalRelation(attributes, encoded)
    Dataset[T](self, plan)
  }

  /**
   * 从给定类型的 RDD 创建 [[Dataset]]。此方法需要一个
   * 编码器（用于将类型 `T` 的 JVM 对象转换为 Spark SQL 内部表示）
   * 通常通过 `SparkSession` 的隐式转换自动创建，或者可以
   * 通过调用 [[Encoders]] 上的静态方法显式创建。
   */
  def createDataset[T : Encoder](data: RDD[T]): Dataset[T] = {
    Dataset[T](self, ExternalRDD(data, self))
  }

  /**
   * 从给定类型的 `java.util.List` 创建 [[Dataset]]。此方法需要一个
   * 编码器（用于将类型 `T` 的 JVM 对象转换为 Spark SQL 内部表示）
   * 通常通过 `SparkSession` 的隐式转换自动创建，或者可以
   * 通过调用 [[Encoders]] 上的静态方法显式创建。
   *
   * == Java 示例 ==
   *
   * {{{
   *     List<String> data = Arrays.asList("hello", "world");
   *     Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
   * }}}
   */
  def createDataset[T : Encoder](data: java.util.List[T]): Dataset[T] = {
    createDataset(data.asScala.toSeq)
  }

  /**
   * 创建一个 [[Dataset]]，其中包含一个名为 `id` 的 `LongType` 列，包含
   * 从 0 到 `end`（不包括）的元素，步长值为 1。
   */
  def range(end: Long): Dataset[java.lang.Long] = range(0, end)

  /**
   * 创建一个 [[Dataset]]，其中包含一个名为 `id` 的 `LongType` 列，包含
   * 从 `start` 到 `end`（不包括）的元素，步长值为 1。
   *
   * @since 2.0.0
   */
  def range(start: Long, end: Long): Dataset[java.lang.Long] = {
    range(start, end, step = 1, numPartitions = leafNodeDefaultParallelism)
  }

  /**
   * 创建一个 [[Dataset]]，其中包含一个名为 `id` 的 `LongType` 列，包含
   * 从 `start` 到 `end`（不包括）的元素，具有步长值。
   *
   * @since 2.0.0
   */
  def range(start: Long, end: Long, step: Long): Dataset[java.lang.Long] = {
    range(start, end, step, numPartitions = leafNodeDefaultParallelism)
  }

  /**
   * 创建一个 [[Dataset]]，其中包含一个名为 `id` 的 `LongType` 列，包含
   * 从 `start` 到 `end`（不包括）的元素，具有步长值，并指定
   * 分区数。
   *
   * @since 2.0.0
   */
  def range(start: Long, end: Long, step: Long, numPartitions: Int): Dataset[java.lang.Long] = {
    new Dataset(self, Range(start, end, step, numPartitions), Encoders.LONG)
  }

  /**
   * 从 `RDD[InternalRow]` 创建 `DataFrame`。
   */
  private[sql] def internalCreateDataFrame(
      catalystRows: RDD[InternalRow],
      schema: StructType,
      isStreaming: Boolean = false): DataFrame = {
    // TODO: 当 rowRDD 是另一个 DataFrame 且应用的
    // schema 在任何字段数据类型上与现有 schema 不同时，使用 MutableProjection。
    val logicalPlan = LogicalRDD(
      schema.toAttributes,
      catalystRows,
      isStreaming = isStreaming)(self)
    Dataset.ofRows(self, logicalPlan)
  }

  /* ------------------------- *
   |  Catalog 相关方法          |
   * ------------------------- */

  /**
   * 用户可以通过该接口创建、删除、修改或查询底层的数据库、表、函数等。
   */
  @transient lazy val catalog: Catalog = new CatalogImpl(self)

  /**
   * 将指定的表/视图作为 `DataFrame` 返回。
   * 如果是表，它必须支持批量读取，返回的 DataFrame 是此表的批量扫描查询计划。
   * 如果是视图， 返回的 DataFrame 只是视图的查询计划，可以是批量或流式查询计划。
   *
   * @param tableName 是限定或非限定的名称，用于指定表或视图。
   *                  如果指定了数据库，它从数据库中标识表/视图。
   *                  否则，它首先尝试查找具有给定名称的临时视图
   *                  然后从当前数据库匹配表/视图。
   *                  注意，全局临时视图数据库在这里也是有效的。
   * @since 2.0.0
   */
  def table(tableName: String): DataFrame = {
    read.table(tableName)
  }

  private[sql] def table(tableIdent: TableIdentifier): DataFrame = {
    Dataset.ofRows(self, UnresolvedRelation(tableIdent))
  }

  /* ----------------- *
   |  其他所有内容       |
   * ----------------- */

  /**
   * 使用 Spark 执行 SQL 查询，将结果作为 `DataFrame` 返回。
   * 此 API 会立即运行 DDL/DML 命令，但不会运行 SELECT 查询。
   */
  def sql(sqlText: String): DataFrame = withActive {
    val tracker = new QueryPlanningTracker
    // 构造 spark plan
    val plan = tracker.measurePhase(QueryPlanningTracker.PARSING) {
      sessionState.sqlParser.parsePlan(sqlText)
    }
    Dataset.ofRows(self, plan, tracker)
  }

  /**
   * 在外部执行引擎而不是 Spark 中执行任意字符串命令。
   * 当用户想要在 Spark 之外执行某些命令时，这可能很有用。
   *
   * 例如，
   * 为 JDBC 执行自定义 DDL/DML 命令，为 ElasticSearch 创建索引，为 Solr 创建核心等。
   *
   * 此方法调用后将立即执行命令，返回的
   * DataFrame 将包含命令的输出（如果有）。
   *
   * @param runner 实现 `ExternalCommandRunner` 的运行器的类名。
   * @param command 要执行的目标命令
   * @param options 运行器的选项。
   *
   * @since 3.0.0
   */
  @Unstable
  def executeCommand(runner: String, command: String, options: Map[String, String]): DataFrame = {
    DataSource.lookupDataSource(runner, sessionState.conf) match {
      case source if classOf[ExternalCommandRunner].isAssignableFrom(source) =>
        Dataset.ofRows(self, ExternalCommandExecutor(
          source.newInstance().asInstanceOf[ExternalCommandRunner], command, options))

      case _ =>
        throw QueryCompilationErrors.commandExecutionInRunnerUnsupportedError(runner)
    }
  }

  /**
   * 返回一个 [[DataFrameReader]]，可用于将非流式数据读取为`DataFrame`。
   * {{{
   *   sparkSession.read.parquet("/path/to/file.parquet")
   *   sparkSession.read.schema(schema).json("/path/to/file.json")
   * }}}
   *
   * @since 2.0.0
   */
  def read: DataFrameReader = new DataFrameReader(self)


  /**
   * 返回一个 `DataStreamReader`，可用于将流式数据读取为 `DataFrame`。
   * {{{
   *   sparkSession.readStream.parquet("/path/to/directory/of/parquet/files")
   *   sparkSession.readStream.schema(schema).json("/path/to/directory/of/json/files")
   * }}}
   *
   * @since 2.0.0
   */
  def readStream: DataStreamReader = new DataStreamReader(self)

  /**
   * 执行某些代码块并将执行该块所花费的时间打印到 stdout。这仅在Scala 中可用，主要用于交互式测试和调试。
   */
  def time[T](f: => T): T = {
    val start = System.nanoTime()
    val ret = f
    val end = System.nanoTime()
    // scalastyle:off println
    println(s"Time taken: ${NANOSECONDS.toMillis(end - start)} ms")
    // scalastyle:on println
    ret
  }

  // scalastyle:off
  // 禁用样式检查器，以便 "implicits" 对象可以以小写 i 开头
  /**
   * (Scala 特定) Scala 中可用的隐式方法，用于将
   * 常见的 Scala 对象转换为 `DataFrame`。
   *
   * {{{
   *   val sparkSession = SparkSession.builder.getOrCreate()
   *   import sparkSession.implicits._
   * }}}
   */
  object implicits extends SQLImplicits with Serializable {
    protected override def _sqlContext: SQLContext = SparkSession.this.sqlContext
  }
  // scalastyle:on

  /**
   * 停止底层的 `SparkContext`。
   */
  def stop(): Unit = {
    sparkContext.stop()
  }


  /**
   * `stop()` 的同义词。
   */
  override def close(): Unit = stop()

  /**
   * 解析我们内部字符串表示中的数据类型。数据类型字符串应该具有与 scala 中 `toString` 生成的格式相同的格式。
   * 它仅由 PySpark 使用。
   */
  protected[sql] def parseDataType(dataTypeString: String): DataType = {
    DataType.fromJson(dataTypeString)
  }

  /**
   * 将 schemaString 定义的 schema 应用于 RDD。它仅由 PySpark 使用。
   */
  private[sql] def applySchemaToPythonRDD(
      rdd: RDD[Array[Any]],
      schemaString: String): DataFrame = {
    val schema = DataType.fromJson(schemaString).asInstanceOf[StructType]
    applySchemaToPythonRDD(rdd, schema)
  }

  /**
   * 将 `schema` 应用于 RDD。
   */
  private[sql] def applySchemaToPythonRDD(
      rdd: RDD[Array[Any]],
      schema: StructType): DataFrame = {
    val rowRdd = rdd.mapPartitions { iter =>
      val fromJava = python.EvaluatePython.makeFromJava(schema)
      iter.map(r => fromJava(r).asInstanceOf[InternalRow])
    }
    internalCreateDataFrame(rowRdd, schema)
  }

  /**
   * 返回给定 java bean 类的 Catalyst Schema。
   */
  private def getSchema(beanClass: Class[_]): Seq[AttributeReference] = {
    val (dataType, _) = JavaTypeInference.inferDataType(beanClass) // 将 JavaBean -> StructType
    dataType.asInstanceOf[StructType].fields.map { f =>
      AttributeReference(f.name, f.dataType, f.nullable)()
    }
  }

  /**
   * 执行一个代码块，将此会话设置为活动会话，并在完成时恢复之前的会话。
   */
  private[sql] def withActive[T](block: => T): T = {
    // 直接使用活动会话线程本地变量，以确保我们获得实际
    // 设置的会话，而不是默认会话。这是为了防止我们在完成后将默认会话提升为活动会话。
    val old = SparkSession.activeThreadSession.get()
    SparkSession.setActiveSession(this)
    try block finally {
      SparkSession.setActiveSession(old)
    }
  }

  private[sql] def leafNodeDefaultParallelism: Int = {
    conf.get(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM).getOrElse(sparkContext.defaultParallelism)
  }
}


@Stable
object SparkSession extends Logging {

  /**
   * Builder for [[SparkSession]].
   */
  @Stable
  class Builder extends Logging {

    private[this] val options = new scala.collection.mutable.HashMap[String, String]

    private[this] val extensions = new SparkSessionExtensions

    private[this] var userSuppliedContext: Option[SparkContext] = None

    private[spark] def sparkContext(sparkContext: SparkContext): Builder = synchronized {
      userSuppliedContext = Option(sparkContext)
      this
    }

    /**
     * 为应用程序设置名称，该名称将显示在 Spark Web UI 中。
     * 如果未设置应用程序名称，将使用随机生成的名称。
     */
    def appName(name: String): Builder = config("spark.app.name", name)

    /**
     * 设置配置选项。使用此方法设置的选项会自动传播到`SparkConf` 和 SparkSession 自己的配置。
     */
    def config(key: String, value: String): Builder = synchronized {
      options += key -> value
      this
    }

    /**
     * 设置配置选项。使用此方法设置的选项会自动传播到`SparkConf` 和 SparkSession 自己的配置。
     */
    def config(key: String, value: Long): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * 设置配置选项。使用此方法设置的选项会自动传播到`SparkConf` 和 SparkSession 自己的配置。
     */
    def config(key: String, value: Double): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * 设置配置选项。使用此方法设置的选项会自动传播到`SparkConf` 和 SparkSession 自己的配置。
     */
    def config(key: String, value: Boolean): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * 设置配置选项。使用此方法设置的选项会自动传播到`SparkConf` 和 SparkSession 自己的配置。
     */
    def config(conf: SparkConf): Builder = synchronized {
      conf.getAll.foreach { case (k, v) => options += k -> v }
      this
    }

    /**
     * 设置要连接的 Spark master URL，例如 "local" 表示本地运行，"local[4]" 表示
     * 使用 4 个核心本地运行，或 "spark://master:7077" 表示在 Spark 独立集群上运行。
     *
     * @since 2.0.0
     */
    def master(master: String): Builder = config("spark.master", master)


    /**
     * 启用 Hive 支持，包括连接到持久化的 Hive metastore、支持Hive serdes 以及 Hive 用户自定义函数。
     */
    def enableHiveSupport(): Builder = synchronized {
      if (hiveClassesArePresent) {
        config(CATALOG_IMPLEMENTATION.key, "hive")
      } else {
        throw new IllegalArgumentException(
          "Unable to instantiate SparkSession with Hive support because " +
            "Hive classes are not found.")
      }
    }

    /**
     * 将扩展注入到 [[SparkSession]] 中。这允许用户添加 Analyzer 规则、
     * Optimizer 规则、Planning Strategies 或自定义解析器。
     */
    def withExtensions(f: SparkSessionExtensions => Unit): Builder = synchronized {
      f(extensions)
      this
    }

    /**
     * 获取现有的 [[SparkSession]]，如果没有现有的，则根据此构建器中设置的选项创建一个新的。
     * 此方法首先检查是否有有效的线程本地 SparkSession， 如果有，返回该 SparkSession。然后检查是否有有效的全局
     * 默认 SparkSession，如果有，返回该 SparkSession。如果没有有效的全局默认
     * SparkSession，该方法创建一个新的 SparkSession 并将新创建的 SparkSession 分配为全局默认 SparkSession。
     *
     * 如果返回现有的 SparkSession，则此构建器中指定的非静态配置选项将应用于现有的 SparkSession。
     *
     * @since 2.0.0
     */
    def getOrCreate(): SparkSession = synchronized {
      val sparkConf = new SparkConf()
      options.foreach { case (k, v) => sparkConf.set(k, v) }

      if (!sparkConf.get(EXECUTOR_ALLOW_SPARK_CONTEXT)) {
        assertOnDriver()
      }

      // 从当前线程的活动会话获取会话。
      var session = activeThreadSession.get()
      if ((session ne null) && !session.sparkContext.isStopped) {
        applyModifiableSettings(session, new java.util.HashMap[String, String](options.asJava))
        return session
      }

      // 全局同步，因此我们只会设置一次默认会话。
      SparkSession.synchronized {
        // 如果当前线程没有活动会话，从全局会话获取。
        session = defaultSession.get()
        if ((session ne null) && !session.sparkContext.isStopped) {
          applyModifiableSettings(session, new java.util.HashMap[String, String](options.asJava))
          return session
        }

        // 没有活动或全局默认会话。创建一个新的。
        val sparkContext = userSuppliedContext.getOrElse {
          // set a random app name if not given.
          if (!sparkConf.contains("spark.app.name")) {
            sparkConf.setAppName(java.util.UUID.randomUUID().toString)
          }

          SparkContext.getOrCreate(sparkConf)
          // Do not update `SparkConf` for existing `SparkContext`, as it's shared by all sessions.
        }

        loadExtensions(extensions)
        applyExtensions(
          sparkContext.getConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS).getOrElse(Seq.empty),
          extensions)

        session = new SparkSession(sparkContext, None, None, extensions, options.toMap)
        setDefaultSession(session)
        setActiveSession(session)
        registerContextListener(sparkContext)
      }

      return session
    }
  }

  /**
   * 创建用于构造 [[SparkSession]] 的 [[SparkSession.Builder]]。
   */
  def builder(): Builder = new Builder

  /**
   * 更改在此线程及其子线程中调用 SparkSession.getOrCreate() 时将返回的 SparkSession。
   * 这可用于确保给定线程接收具有隔离会话的 SparkSession，
   * 而不是全局（首次创建的）上下文。
   */
  def setActiveSession(session: SparkSession): Unit = {
    activeThreadSession.set(session)
  }

  /**
   * 清除当前线程的活动 SparkSession。后续对 getOrCreate 的调用将
   * 返回首次创建的上下文，而不是线程本地覆盖。
   *
   * @since 2.0.0
   */
  def clearActiveSession(): Unit = {
    activeThreadSession.remove()
  }

  /**
   * 设置构建器返回的默认 SparkSession。
   *
   * @since 2.0.0
   */
  def setDefaultSession(session: SparkSession): Unit = {
    defaultSession.set(session)
  }

  /**
   * 清除构建器返回的默认 SparkSession
   */
  def clearDefaultSession(): Unit = {
    defaultSession.set(null)
  }


  /**
   * 返回当前线程的活动 SparkSession，由构建器返回。
   */
  def getActiveSession: Option[SparkSession] = {
    if (Utils.isInRunningSparkTask) {
      // Return None when running on executors.
      None
    } else {
      Option(activeThreadSession.get)
    }
  }

  /**
   * 返回构建器返回的默认 SparkSession。
   */
  def getDefaultSession: Option[SparkSession] = {
    if (Utils.isInRunningSparkTask) {
      // Return None when running on executors.
      None
    } else {
      Option(defaultSession.get)
    }
  }

  /**
   * 返回当前活动的 SparkSession，否则返回默认的。如果没有默认的 SparkSession，则抛出异常。
   */
  def active: SparkSession = {
    getActiveSession.getOrElse(getDefaultSession.getOrElse(
      throw new IllegalStateException("No active or default Spark session found")))
  }

  /**
   * 将可修改的设置应用于现有的 [[SparkSession]]。
   * 此方法在 Scala 和 Python 中都使用，因此将其放在 [[SparkSession]] 对象下。
   */
  private[sql] def applyModifiableSettings(
      session: SparkSession,
      options: java.util.HashMap[String, String]): Unit = {
    // 延迟 val 以避免不必要的会话状态初始化
    lazy val conf = session.sessionState.conf

    val dedupOptions = if (options.isEmpty) Map.empty[String, String] else (
      options.asScala.toSet -- conf.getAllConfs.toSet).toMap
    val (staticConfs, otherConfs) =
      dedupOptions.partition(kv => SQLConf.isStaticConfigKey(kv._1))

    otherConfs.foreach { case (k, v) => conf.setConfString(k, v) }

    // 注意，其他运行时 SQL 选项，例如，对于其他第三方数据源
    // 可以在这里标记为被忽略的配置。
    val maybeIgnoredConfs = otherConfs.filterNot { case (k, _) => conf.isModifiable(k) }

    if (staticConfs.nonEmpty || maybeIgnoredConfs.nonEmpty) {
      logWarning(
        "Using an existing Spark session; only runtime SQL configurations will take effect.")
    }
    if (staticConfs.nonEmpty) {
      logDebug("Ignored static SQL configurations:\n  " +
        conf.redactOptions(staticConfs).toSeq.map { case (k, v) => s"$k=$v" }.mkString("\n  "))
    }
    if (maybeIgnoredConfs.nonEmpty) {
      // 仅打印出非静态和非运行时 SQL 配置。
      // 注意，这可能会显示核心配置或在第三方数据源中定义的源特定选项。
      logDebug("Configurations that might not take effect:\n  " +
        conf.redactOptions(
          maybeIgnoredConfs).toSeq.map { case (k, v) => s"$k=$v" }.mkString("\n  "))
    }
  }

  /**
   * 返回一个克隆的 SparkSession，其中所有指定的配置都被禁用，或者
   * 如果所有配置都已被禁用，则返回原始 SparkSession。
   */
  private[sql] def getOrCloneSessionWithConfigsOff(
      session: SparkSession,
      configurations: Seq[ConfigEntry[Boolean]]): SparkSession = {
    val configsEnabled = configurations.filter(session.sessionState.conf.getConf(_))
    if (configsEnabled.isEmpty) {
      session
    } else {
      val newSession = session.cloneSession()
      configsEnabled.foreach(conf => {
        newSession.sessionState.conf.setConf(conf, false)
      })
      newSession
    }
  }

  ////////////////////////////////////////////////////////////////////////////////////////
  // 从现在开始的私有方法
  ////////////////////////////////////////////////////////////////////////////////////////
  private val listenerRegistered: AtomicBoolean = new AtomicBoolean(false)

  /** 将 AppEnd 监听器注册到 Context  */
  private def registerContextListener(sparkContext: SparkContext): Unit = {
    if (!listenerRegistered.get()) {
      sparkContext.addSparkListener(new SparkListener {
        override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
          defaultSession.set(null)
          listenerRegistered.set(false)
        }
      })
      listenerRegistered.set(true)
    }
  }

  /** 当前线程的活动 SparkSession。 */
  private val activeThreadSession = new InheritableThreadLocal[SparkSession]

  /** Reference to the root SparkSession. */
  private val defaultSession = new AtomicReference[SparkSession]

  private val HIVE_SESSION_STATE_BUILDER_CLASS_NAME =
    "org.apache.spark.sql.hive.HiveSessionStateBuilder"

  private def sessionStateClassName(conf: SparkConf): String = {
    conf.get(CATALOG_IMPLEMENTATION) match {
      case "hive" => HIVE_SESSION_STATE_BUILDER_CLASS_NAME
      case "in-memory" => classOf[SessionStateBuilder].getCanonicalName
    }
  }

  private def assertOnDriver(): Unit = {
    if (TaskContext.get != null) {
      // we're accessing it during task execution, fail.
      throw new IllegalStateException(
        "SparkSession should only be created and accessed on the driver.")
    }
  }

  /**
   * 基于 `className` 从 conf 创建 `SessionState` 实例的辅助方法。
   * 结果是 `SessionState` 或基于 Hive 的 `SessionState`。
   */
  private def instantiateSessionState(
      className: String,
      sparkSession: SparkSession): SessionState = {
    try {
      // 调用 new [Hive]SessionStateBuilder(
      //   SparkSession,
      //   Option[SessionState])
      val clazz = Utils.classForName(className)
      val ctor = clazz.getConstructors.head
      ctor.newInstance(sparkSession, None).asInstanceOf[BaseSessionStateBuilder].build()
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }
  }

  /**
   * @return 如果可以加载 Hive 类，则返回 true，否则返回 false。
   */
  private[spark] def hiveClassesArePresent: Boolean = {
    try {
      Utils.classForName(HIVE_SESSION_STATE_BUILDER_CLASS_NAME)
      Utils.classForName("org.apache.hadoop.hive.conf.HiveConf")
      true
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => false
    }
  }

  private[spark] def cleanupAnyExistingSession(): Unit = {
    val session = getActiveSession.orElse(getDefaultSession)
    if (session.isDefined) {
      logWarning(
        s"""An existing Spark session exists as the active or default session.
           |This probably means another suite leaked it. Attempting to stop it before continuing.
           |This existing Spark session was created at:
           |
           |${session.get.creationSite.longForm}
           |
         """.stripMargin)
      session.get.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  /**
   * 为给定的扩展类名初始化扩展。这些类将应用于
   * 传递到此函数的扩展。
   */
  private def applyExtensions(
      extensionConfClassNames: Seq[String],
      extensions: SparkSessionExtensions): SparkSessionExtensions = {
    extensionConfClassNames.foreach { extensionConfClassName =>
      try {
        val extensionConfClass = Utils.classForName(extensionConfClassName)
        val extensionConf = extensionConfClass.getConstructor().newInstance()
          .asInstanceOf[SparkSessionExtensions => Unit]
        extensionConf(extensions)
      } catch {
        // 如果找不到类或类的类型错误，则忽略错误。
        case e@(_: ClassCastException |
                _: ClassNotFoundException |
                _: NoClassDefFoundError) =>
          logWarning(s"Cannot use $extensionConfClassName to configure session extensions.", e)
      }
    }
    extensions
  }

  /**
   * 从 [[ServiceLoader]] 加载扩展并使用它们
   */
  private def loadExtensions(extensions: SparkSessionExtensions): Unit = {
    val loader = ServiceLoader.load(classOf[SparkSessionExtensionsProvider],
      Utils.getContextOrSparkClassLoader)
    val loadedExts = loader.iterator()

    while (loadedExts.hasNext) {
      try {
        val ext = loadedExts.next()
        ext(extensions)
      } catch {
        case e: Throwable => logWarning("Failed to load session extension", e)
      }
    }
  }
}
