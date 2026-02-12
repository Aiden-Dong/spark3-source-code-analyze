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

import java.net.URL
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import javax.annotation.concurrent.GuardedBy

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FsUrlStreamHandlerFactory, Path}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.CacheManager
import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.execution.ui.{SQLAppStatusListener, SQLAppStatusStore, SQLTab, StreamingQueryStatusStore}
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.sql.streaming.ui.{StreamingQueryStatusListener, StreamingQueryTab}
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.Utils

/**
 * 一个类，用于保存给定 [[SQLContext]] 中跨会话共享的所有状态。
 *
 *   关键 :
 *      GlobalTempViewManager
 *      CacheManager
 *
 * @param sparkContext 与此 SharedState 关联的 Spark 上下文
 * @param initialConfigs 来自第一个创建的 SparkSession 的配置
 */
private[sql] class SharedState(
    val sparkContext: SparkContext,                         // Spark 操作上下文
    initialConfigs: scala.collection.Map[String, String])
  extends Logging {

  SharedState.setFsUrlStreamHandlerFactory(sparkContext.conf, sparkContext.hadoopConfiguration)


  private[sql] val (conf, hadoopConf) = {
    // spark.sql.warehouse.dir
    val warehousePath = SharedState.resolveWarehousePath(
      sparkContext.conf, sparkContext.hadoopConfiguration, initialConfigs)

    val confClone = sparkContext.conf.clone()
    val hadoopConfClone = new Configuration(sparkContext.hadoopConfiguration)
    // 从 `SparkConf` 中提取条目并将它们放入 Hadoop 配置中。
    confClone.getAll.foreach { case (k, v) =>
      if (v ne null) hadoopConfClone.set(k, v)
    }
    // 如果使用现有的 `SparkContext` 实例实例化 `SparkSession` 且没有现有的
    // `SharedState`，则所有 `SparkSession` 级别的配置具有更高的优先级来生成
    // `SharedState` 实例。这只会执行一次，然后在 `SparkSession` 之间共享
    initialConfigs.foreach {
      // 我们已经解析了仓库路径，不应该在这里设置仓库配置。
      case (k, _) if k == WAREHOUSE_PATH.key || k == SharedState.HIVE_WAREHOUSE_CONF_NAME =>
      case (k, v) if SQLConf.isStaticConfigKey(k) =>
        logDebug(s"Applying static initial session options to SparkConf: $k -> $v")
        confClone.set(k, v)
      case (k, v) =>
        logDebug(s"Applying other initial session options to HadoopConf: $k -> $v")
        hadoopConfClone.set(k, v)
    }
    val qualified = try {
      SharedState.qualifyWarehousePath(hadoopConfClone, warehousePath)
    } catch {
      case NonFatal(e) =>
        logWarning("Cannot qualify the warehouse path, leaving it unqualified.", e)
        warehousePath
    }
    // 在 SparkConf 和 Hadoop 配置中设置仓库路径，以便从 `SparkContext` 在应用程序范围内可访问。
    SharedState.setWarehousePathConf(sparkContext.conf, sparkContext.hadoopConfiguration, qualified)
    SharedState.setWarehousePathConf(confClone, hadoopConfClone, qualified)
    (confClone, hadoopConfClone)
  }

  /**
   * 用于缓存在未来执行中重用的查询结果的类。
   */
  val cacheManager: CacheManager = new CacheManager

  /** 用于所有流查询生命周期跟踪和管理的全局锁。 */
  private[sql] val activeQueriesLock = new Object

  /**
   * 用于查询此 Spark 应用程序的 SQL 状态/指标的状态存储，基于 SQL 特定的 [[org.apache.spark.scheduler.SparkListenerEvent]]。
   */
  @GuardedBy("activeQueriesLock")
  private[sql] val activeStreamingQueries = new ConcurrentHashMap[UUID, StreamExecution]()

  /**
   * 用于查询此 Spark 应用程序的 SQL 状态/指标的状态存储，基于 SQL 特定的 [[org.apache.spark.scheduler.SparkListenerEvent]]。
   */
  val statusStore: SQLAppStatusStore = {
    val kvStore = sparkContext.statusStore.store.asInstanceOf[ElementTrackingStore]
    val listener = new SQLAppStatusListener(conf, kvStore, live = true)
    sparkContext.listenerBus.addToStatusQueue(listener)
    val statusStore = new SQLAppStatusStore(kvStore, Some(listener))
    sparkContext.ui.foreach(new SQLTab(statusStore, _))
    statusStore
  }

  /**
   * 用于结构化流 UI 的 [[StreamingQueryListener]]，它包含要显示的所有流查询 UI 数据。
   */
  lazy val streamingQueryStatusListener: Option[StreamingQueryStatusListener] = {
    sparkContext.ui.flatMap { ui =>
      if (conf.get(STREAMING_UI_ENABLED)) {
        val kvStore = sparkContext.statusStore.store.asInstanceOf[ElementTrackingStore]
        new StreamingQueryTab(new StreamingQueryStatusStore(kvStore), ui)
        Some(new StreamingQueryStatusListener(conf, kvStore))
      } else {
        None
      }
    }
  }

  /**
   * 获取  spark.sql.catalogImplementation ：  [hive, in-memory]
   */
  lazy val externalCatalog: ExternalCatalogWithListener = {
    // spark.sql.catalogImplementation : [hive, in-memory]
    val externalCatalog = SharedState.reflect[ExternalCatalog, SparkConf, Configuration](
      SharedState.externalCatalogClassName(conf), conf, hadoopConf)

    val defaultDbDefinition = CatalogDatabase(
      SessionCatalog.DEFAULT_DATABASE,
      "default database",
      CatalogUtils.stringToURI(conf.get(WAREHOUSE_PATH)),   // spark.sql.warehouse.dir
      Map())

    // 判断 default database 是否存在
    if (!externalCatalog.databaseExists(SessionCatalog.DEFAULT_DATABASE)) {
      // 可能有另一个 Spark 应用程序同时创建默认数据库，这里我们设置 `ignoreIfExists = true` 以避免 `DatabaseAlreadyExists` 异常。
      externalCatalog.createDatabase(defaultDbDefinition, ignoreIfExists = true)
    }
    // 包装以提供目录事件
    val wrapped = new ExternalCatalogWithListener(externalCatalog)

    // 利用 ExternalCatalog 分装器，将
    wrapped.addListener((event: ExternalCatalogEvent) => sparkContext.listenerBus.post(event))

    wrapped
  }

  /**
   * 全局临时视图的管理器。
   */
  lazy val globalTempViewManager: GlobalTempViewManager = {
    val globalTempDB = conf.get(GLOBAL_TEMP_DATABASE)
    if (externalCatalog.databaseExists(globalTempDB)) {
      throw QueryExecutionErrors.databaseNameConflictWithSystemPreservedDatabaseError(globalTempDB)
    }
    new GlobalTempViewManager(globalTempDB)
  }

  /**
   * 用于加载所有用户添加的 jar 的类加载器。
   */
  val jarClassLoader = new NonClosableMutableURLClassLoader(
    org.apache.spark.util.Utils.getContextOrSparkClassLoader)

}

object SharedState extends Logging {
  @volatile private var fsUrlStreamHandlerFactoryInitialized = false

  private def setFsUrlStreamHandlerFactory(conf: SparkConf, hadoopConf: Configuration): Unit = {
    if (!fsUrlStreamHandlerFactoryInitialized &&
        conf.get(DEFAULT_URL_STREAM_HANDLER_FACTORY_ENABLED)) {
      synchronized {
        if (!fsUrlStreamHandlerFactoryInitialized) {
          try {
            URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory(hadoopConf))
            fsUrlStreamHandlerFactoryInitialized = true
          } catch {
            case NonFatal(_) =>
              logWarning("URL.setURLStreamHandlerFactory failed to set FsUrlStreamHandlerFactory")
          }
        }
      }
    }
  }

  private val HIVE_EXTERNAL_CATALOG_CLASS_NAME = "org.apache.spark.sql.hive.HiveExternalCatalog"

  private def externalCatalogClassName(conf: SparkConf): String = {
    conf.get(CATALOG_IMPLEMENTATION) match {
      case "hive" => HIVE_EXTERNAL_CATALOG_CLASS_NAME
      case "in-memory" => classOf[InMemoryCatalog].getCanonicalName
    }
  }

  /**
   * 辅助方法，用于使用接受 [[Arg1]] 和 [[Arg2]] 的单参数构造函数创建 [[T]] 的实例。
   */
  private def reflect[T, Arg1 <: AnyRef, Arg2 <: AnyRef](
      className: String,
      ctorArg1: Arg1,
      ctorArg2: Arg2)(
      implicit ctorArgTag1: ClassTag[Arg1],
      ctorArgTag2: ClassTag[Arg2]): T = {
    try {
      val clazz = Utils.classForName(className)
      val ctor = clazz.getDeclaredConstructor(ctorArgTag1.runtimeClass, ctorArgTag2.runtimeClass)
      val args = Array[AnyRef](ctorArg1, ctorArg2)
      ctor.newInstance(args: _*).asInstanceOf[T]
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }
  }

  private val HIVE_WAREHOUSE_CONF_NAME = "hive.metastore.warehouse.dir"

  /**
   * 使用 [[SparkConf]] 中的键 `spark.sql.warehouse.dir` 或来自第一个创建的 SparkSession 实例的初始选项，
   * 以及 hadoop [[Configuration]] 中的 `hive.metastore.warehouse.dir` 来确定仓库路径。
   * 优先级顺序为：
   * initialConfigs 中的 s.s.w.d
   *   > spark conf 中的 s.s.w.d（用户指定）
   *   > hadoop conf 中的 h.m.w.d（用户指定）
   *   > spark conf 中的 s.s.w.d（默认值）
   *
   * @return 解析后的仓库路径。
   */
  def resolveWarehousePath(
      sparkConf: SparkConf,
      hadoopConf: Configuration,
      initialConfigs: scala.collection.Map[String, String] = Map.empty): String = {
    val sparkWarehouseOption =
      initialConfigs.get(WAREHOUSE_PATH.key).orElse(sparkConf.getOption(WAREHOUSE_PATH.key))
    if (initialConfigs.contains(HIVE_WAREHOUSE_CONF_NAME)) {
      logWarning(s"Not allowing to set $HIVE_WAREHOUSE_CONF_NAME in SparkSession's " +
        s"options, please use ${WAREHOUSE_PATH.key} to set statically for cross-session usages")
    }
    // hive.metastore.warehouse.dir 只保留在 hadoopConf 中
    sparkConf.remove(HIVE_WAREHOUSE_CONF_NAME)
    // 将 Hive 元数据仓库路径设置为我们使用的路径
    val hiveWarehouseDir = hadoopConf.get(HIVE_WAREHOUSE_CONF_NAME)
    if (hiveWarehouseDir != null && sparkWarehouseOption.isEmpty) {
      // 如果设置了 hive.metastore.warehouse.dir 但未设置 spark.sql.warehouse.dir，
      // 我们将遵循 hive.metastore.warehouse.dir 的值。
      logInfo(s"${WAREHOUSE_PATH.key} is not set, but $HIVE_WAREHOUSE_CONF_NAME is set. " +
        s"Setting ${WAREHOUSE_PATH.key} to the value of $HIVE_WAREHOUSE_CONF_NAME.")
      hiveWarehouseDir
    } else {
      // 如果设置了 spark.sql.warehouse.dir，我们将使用 spark.sql.warehouse.dir 的值覆盖 hive.metastore.warehouse.dir。
      // 当 spark.sql.warehouse.dir 和 hive.metastore.warehouse.dir 都未设置时，
      // 我们将把 hive.metastore.warehouse.dir 设置为 spark.sql.warehouse.dir 的默认值。
      val sparkWarehouseDir = sparkWarehouseOption.getOrElse(WAREHOUSE_PATH.defaultValueString)
      logInfo(s"Setting $HIVE_WAREHOUSE_CONF_NAME ('$hiveWarehouseDir') to the value of " +
        s"${WAREHOUSE_PATH.key}.")
      sparkWarehouseDir
    }
  }

  def qualifyWarehousePath(hadoopConf: Configuration, warehousePath: String): String = {
    val tempPath = new Path(warehousePath)
    val qualified = tempPath.getFileSystem(hadoopConf).makeQualified(tempPath).toString
    logInfo(s"Warehouse path is '$qualified'.")
    qualified
  }

  def setWarehousePathConf(
      sparkConf: SparkConf,
      hadoopConf: Configuration,
      warehousePath: String): Unit = {
    sparkConf.set(WAREHOUSE_PATH.key, warehousePath)
    hadoopConf.set(HIVE_WAREHOUSE_CONF_NAME, warehousePath)
  }
}
