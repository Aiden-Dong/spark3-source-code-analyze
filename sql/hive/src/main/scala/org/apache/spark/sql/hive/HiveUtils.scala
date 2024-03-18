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

package org.apache.spark.sql.hive

import java.io.File
import java.net.{URL, URLClassLoader}
import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.util.Try

import org.apache.commons.lang3.{JavaVersion, SystemUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.util.VersionInfo
import org.apache.hive.common.util.HiveVersionInfo

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.hive.client._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH
import org.apache.spark.sql.types._
import org.apache.spark.util.{ChildFirstURLClassLoader, Utils}


private[spark] object HiveUtils extends Logging {

  // Spark SQL 内部使用的 Hive 版本
  val builtinHiveVersion: String = HiveVersionInfo.getVersion

  val BUILTIN_HIVE_VERSION = buildStaticConf("spark.sql.hive.version")
    .doc("Spark 分发的内置 Hive 版本的编译版本。" +
      "请注意，这是一个只读配置，仅用于报告内置的 Hive 版本。" +
      "如果您想要 Spark 调用不同的元数据存储客户端，请参考 spark.sql.hive.metastore.version。")
    .version("1.1.1")
    .stringConf
    .checkValue(_ == builtinHiveVersion,
      "The builtin Hive version is read-only, please use spark.sql.hive.metastore.version")
    .createWithDefault(builtinHiveVersion)

  private def isCompatibleHiveVersion(hiveVersionStr: String): Boolean = {
    Try { IsolatedClientLoader.hiveVersion(hiveVersionStr) }.isSuccess
  }

  val HIVE_METASTORE_VERSION = buildStaticConf("spark.sql.hive.metastore.version")
    .doc("Hive 元数据存储的版本。可用选项包括 0.12.0 到 2.3.9, 以及 3.0.0 到 3.1.2")
    .version("1.4.0")
    .stringConf
    .checkValue(isCompatibleHiveVersion, "Unsupported Hive Metastore version")
    .createWithDefault(builtinHiveVersion)

  val HIVE_METASTORE_JARS = buildStaticConf("spark.sql.hive.metastore.jars")
    .doc(s"""
      |用于实例化 HiveMetastoreClient 的 JAR 包的位置。此属性可以是四个选项之一：
      | 1. "builtin"
      |  使用内置的 Hive ${builtinHiveVersion}，它在启用 <code>-Phive</code> 时与 Spark 汇编一起打包。
      |  选择此选项时，<code>spark.sql.hive.metastore.version</code> 必须是 <code>${builtinHiveVersion}</code> 或未定义。
      | 2. "maven"
      |   使用从 Maven 仓库下载的指定版本的 Hive JAR 包。
      | 3. "path"
      |  使用由 spark.sql.hive.metastore.jars.path 配置的 Hive JAR 包，以逗号分隔的格式。支持本地或远程路径。
      |  提供的 JAR 包应与 ${HIVE_METASTORE_VERSION.key} 的版本相同。
      | 4. 标准格式的 Hive 和 Hadoop 类路径。提供的 JAR 包应与 ${HIVE_METASTORE_VERSION.key} 的版本相同。
      """.stripMargin)
    .version("1.4.0")
    .stringConf
    .createWithDefault("builtin")

  val HIVE_METASTORE_JARS_PATH = buildStaticConf("spark.sql.hive.metastore.jars.path")
    .doc(s"""
      |
      |逗号分隔的 JAR 包路径，用于实例化 HiveMetastoreClient。此配置仅在 ${HIVE_METASTORE_JARS.key} 设置为 path 时有用。路径可以是以下任何格式之一：
      | file://path/to/jar/foo.jar
      | hdfs://nameservice/path/to/jar/foo.jar
      |/path/to/jar/（路径不包含 URI 方案，遵循 conf fs.defaultFS 的 URI 方案）
      | 请注意，1、2 和 3 支持通配符。例如：
      |
      |file://path/to/jar/,file://path2/to/jar//*.jar
      |hdfs://nameservice/path/to/jar/,hdfs://nameservice2/path/to/jar//*.jar
      """.stripMargin)
    .version("3.1.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val CONVERT_METASTORE_PARQUET = buildConf("spark.sql.hive.convertMetastoreParquet")
    .doc("当设置为 true 时，将使用内置的 Parquet 读取器和写入器来处理通过使用 HiveQL 语法创建的 Parquet 表，" +
      "而不是使用 Hive serde。")
    .version("1.1.1")
    .booleanConf
    .createWithDefault(true)

  val CONVERT_METASTORE_PARQUET_WITH_SCHEMA_MERGING =
    buildConf("spark.sql.hive.convertMetastoreParquet.mergeSchema")
      .doc("当设置为 true 时，还会尝试合并可能不同但兼容的 Parquet 数据文件中的不同 Parquet 模式。" +
        "此配置仅在 \"spark.sql.hive.convertMetastoreParquet\" 为 true 时生效。")
      .version("1.3.1")
      .booleanConf
      .createWithDefault(false)

  val CONVERT_METASTORE_ORC = buildConf("spark.sql.hive.convertMetastoreOrc")
    .doc("当设置为 true 时，将使用内置的 ORC 读取器和写入器来处理通过使用 HiveQL 语法创建的 ORC 表，而不是使用 Hive serde。")
    .version("2.0.0")
    .booleanConf
    .createWithDefault(true)

  val CONVERT_INSERTING_PARTITIONED_TABLE =
    buildConf("spark.sql.hive.convertInsertingPartitionedTable")
      .doc("当设置为 true，并且 spark.sql.hive.convertMetastoreParquet 或 spark.sql.hive.convertMetastoreOrc 为 true 时，" +
        "将使用内置的 ORC/Parquet 写入器来处理使用 HiveSQL 语法创建的分区 ORC/Parquet 表的插入操作。")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  val CONVERT_METASTORE_CTAS = buildConf("spark.sql.hive.convertMetastoreCtas")
    .doc("当设置为 true 时，Spark 将尝试在 CTAS（Create Table As Select）中使用内置的数据源写入器，而不是使用 Hive serde。" +
      "此标志仅在分别启用 Parquet 和 ORC 格式的 spark.sql.hive.convertMetastoreParquet 或 spark.sql.hive.convertMetastoreOrc 时有效。")
    .version("3.0.0")
    .booleanConf
    .createWithDefault(true)

  val CONVERT_METASTORE_INSERT_DIR = buildConf("spark.sql.hive.convertMetastoreInsertDir")
    .doc("当设置为 true 时，Spark 将尝试在 INSERT OVERWRITE DIRECTORY 中使用内置的数据源写入器，而不是使用 Hive serde。" +
      "此标志仅在分别启用 Parquet 和 ORC 格式的 spark.sql.hive.convertMetastoreParquet 或 spark.sql.hive.convertMetastoreOrc 时有效。")
    .version("3.3.0")
    .booleanConf
    .createWithDefault(true)

  val HIVE_METASTORE_SHARED_PREFIXES = buildStaticConf("spark.sql.hive.metastore.sharedPrefixes")
    .doc("一个逗号分隔的类前缀列表，应使用在 Spark SQL 和特定版本的 Hive 之间共享的类加载器加载。" +
      "一个需要共享的类的示例是与访问元数据存储的 JDBC 驱动程序。" +
      "其他需要共享的类是与已经共享的类进行交互的类。例如，被 log4j 使用的自定义追加器。")
    .version("1.4.0")
    .stringConf
    .toSequence
    .createWithDefault(jdbcPrefixes)

  private def jdbcPrefixes = Seq(
    "com.mysql.jdbc", "org.postgresql", "com.microsoft.sqlserver", "oracle.jdbc")

  val HIVE_METASTORE_BARRIER_PREFIXES = buildStaticConf("spark.sql.hive.metastore.barrierPrefixes")
    .doc("一个逗号分隔的类前缀列表，应该针对 Spark SQL 与每个版本的 Hive 通信时显式重新加载。" +
      "例如，在通常应该共享的前缀中声明的 Hive UDF（即 <code>org.apache.spark.*</code>）。")
    .version("1.4.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val HIVE_THRIFT_SERVER_ASYNC = buildConf("spark.sql.hive.thriftServer.async")
    .doc("当设置为 true 时，Hive Thrift 服务器以异步方式执行 SQL 查询。.")
    .version("1.5.0")
    .booleanConf
    .createWithDefault(true)

  /**
   * 将用于与元数据存储通信的 Hive 客户端版本。
   * 请注意，这不一定需要与 Spark SQL 内部用于执行的 Hive 版本相同。
   */
  private def hiveMetastoreVersion(conf: SQLConf): String = {
    conf.getConf(HIVE_METASTORE_VERSION)
  }

  /**
   * The location of the jars that should be used to instantiate the HiveMetastoreClient.  This
   * property can be one of three options:
   *  - a classpath in the standard format for both hive and hadoop.
   *  - path - attempt to discover the jars with paths configured by `HIVE_METASTORE_JARS_PATH`.
   *  - builtin - attempt to discover the jars that were used to load Spark SQL and use those. This
   *              option is only valid when using the execution version of Hive.
   *  - maven - download the correct version of hive on demand from maven.
   */
  private def hiveMetastoreJars(conf: SQLConf): String = {
    conf.getConf(HIVE_METASTORE_JARS)
  }

  /**
   * Hive jars paths, only work when `HIVE_METASTORE_JARS` is `path`.
   */
  private def hiveMetastoreJarsPath(conf: SQLConf): Seq[String] = {
    conf.getConf(HIVE_METASTORE_JARS_PATH)
  }

  /**
   * A comma separated list of class prefixes that should be loaded using the classloader that
   * is shared between Spark SQL and a specific version of Hive. An example of classes that should
   * be shared is JDBC drivers that are needed to talk to the metastore. Other classes that need
   * to be shared are those that interact with classes that are already shared.  For example,
   * custom appenders that are used by log4j.
   */
  private def hiveMetastoreSharedPrefixes(conf: SQLConf): Seq[String] = {
    conf.getConf(HIVE_METASTORE_SHARED_PREFIXES).filterNot(_ == "")
  }

  /**
   * A comma separated list of class prefixes that should explicitly be reloaded for each version
   * of Hive that Spark SQL is communicating with.  For example, Hive UDFs that are declared in a
   * prefix that typically would be shared (i.e. org.apache.spark.*)
   */
  private def hiveMetastoreBarrierPrefixes(conf: SQLConf): Seq[String] = {
    conf.getConf(HIVE_METASTORE_BARRIER_PREFIXES).filterNot(_ == "")
  }

  /**
   * Change time configurations needed to create a [[HiveClient]] into unified [[Long]] format.
   */
  private[hive] def formatTimeVarsForHiveClient(hadoopConf: Configuration): Map[String, String] = {
    // Hive 0.14.0 引入了在 HiveConf 中的超时操作，并通过添加时间后缀（s、ms 和 d 等）来更改了一堆时间 ConfVar 的默认值。
    // 当用户试图连接到较低版本的 Hive metastore 时，这会破坏向后兼容性，因为在较低版本的 Hive 中，这些选项预期是整数值。
    // 在这里，我们列举了所有的时间 ConfVar 并根据它们的输出时间单位将它们的值转换为数字字符串。

    val commonTimeVars = Seq(
      ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY -> TimeUnit.SECONDS,
      ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.METASTORE_CLIENT_SOCKET_LIFETIME -> TimeUnit.SECONDS,
      ConfVars.HMSHANDLERINTERVAL -> TimeUnit.MILLISECONDS,
      ConfVars.METASTORE_EVENT_DB_LISTENER_TTL -> TimeUnit.SECONDS,
      ConfVars.METASTORE_EVENT_CLEAN_FREQ -> TimeUnit.SECONDS,
      ConfVars.METASTORE_EVENT_EXPIRY_DURATION -> TimeUnit.SECONDS,
      ConfVars.METASTORE_AGGREGATE_STATS_CACHE_TTL -> TimeUnit.SECONDS,
      ConfVars.METASTORE_AGGREGATE_STATS_CACHE_MAX_WRITER_WAIT -> TimeUnit.MILLISECONDS,
      ConfVars.METASTORE_AGGREGATE_STATS_CACHE_MAX_READER_WAIT -> TimeUnit.MILLISECONDS,
      ConfVars.HIVES_AUTO_PROGRESS_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_LOG_INCREMENTAL_PLAN_PROGRESS_INTERVAL -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_LOCK_SLEEP_BETWEEN_RETRIES -> TimeUnit.SECONDS,
      ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_TXN_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_COMPACTOR_WORKER_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_COMPACTOR_CHECK_INTERVAL -> TimeUnit.SECONDS,
      ConfVars.HIVE_COMPACTOR_CLEANER_RUN_INTERVAL -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_HTTP_MAX_IDLE_TIME -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_HTTP_WORKER_KEEPALIVE_TIME -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_MAX_AGE -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_LOGIN_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_WORKER_KEEPALIVE_TIME -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_ASYNC_EXEC_KEEPALIVE_TIME -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_LONG_POLLING_TIMEOUT -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_SESSION_CHECK_INTERVAL -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_IDLE_SESSION_TIMEOUT -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_IDLE_OPERATION_TIMEOUT -> TimeUnit.MILLISECONDS,
      ConfVars.SERVER_READ_SOCKET_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_LOCALIZE_RESOURCE_WAIT_INTERVAL -> TimeUnit.MILLISECONDS,
      ConfVars.SPARK_CLIENT_FUTURE_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.SPARK_JOB_MONITOR_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.SPARK_RPC_CLIENT_CONNECT_TIMEOUT -> TimeUnit.MILLISECONDS,
      ConfVars.SPARK_RPC_CLIENT_HANDSHAKE_TIMEOUT -> TimeUnit.MILLISECONDS
    ).map { case (confVar, unit) =>
      confVar.varname -> HiveConf.getTimeVar(hadoopConf, confVar, unit).toString
    }

    // The following configurations were removed by HIVE-12164(Hive 2.0)
    val hardcodingTimeVars = Seq(
      ("hive.stats.jdbc.timeout", "30s") -> TimeUnit.SECONDS,
      ("hive.stats.retries.wait", "3000ms") -> TimeUnit.MILLISECONDS
    ).map { case ((key, defaultValue), unit) =>
      val value = hadoopConf.get(key, defaultValue)
      key -> HiveConf.toTime(value, unit, unit).toString
    }

    (commonTimeVars ++ hardcodingTimeVars).toMap
  }

  /**
   * Check current Thread's SessionState type
   * @return true when SessionState.get returns an instance of CliSessionState,
   *         false when it gets non-CliSessionState instance or null
   */
  def isCliSessionState(): Boolean = {
    val state = SessionState.get
    var temp: Class[_] = if (state != null) state.getClass else null
    var found = false
    while (temp != null && !found) {
      found = temp.getName == "org.apache.hadoop.hive.cli.CliSessionState"
      temp = temp.getSuperclass
    }
    found
  }

  /**
   * Create a [[HiveClient]] used for execution.
   *
   * Currently this must always be the Hive built-in version that packaged
   * with Spark SQL. This copy of the client is used for execution related tasks like
   * registering temporary functions or ensuring that the ThreadLocal SessionState is
   * correctly populated.  This copy of Hive is *not* used for storing persistent metadata,
   * and only point to a dummy metastore in a temporary directory.
   */
  protected[hive] def newClientForExecution(
      conf: SparkConf,
      hadoopConf: Configuration): HiveClientImpl = {
    logInfo(s"Initializing execution hive, version $builtinHiveVersion")
    val loader = new IsolatedClientLoader(
      version = IsolatedClientLoader.hiveVersion(builtinHiveVersion),
      sparkConf = conf,
      execJars = Seq.empty,
      hadoopConf = hadoopConf,
      config = newTemporaryConfiguration(useInMemoryDerby = true),
      isolationOn = false,
      baseClassLoader = Utils.getContextOrSparkClassLoader)
    loader.createClient().asInstanceOf[HiveClientImpl]
  }

  /**
   * 创建一个用于从 Hive MetaStore 检索元数据的 [[HiveClient]]。
   * 这里使用的 Hive 客户端版本必须与在 hive-site.xml 文件中配置的元数据存储相匹配。
   */
  protected[hive] def newClientForMetadata(
      conf: SparkConf,
      hadoopConf: Configuration): HiveClient = {
    val configurations = formatTimeVarsForHiveClient(hadoopConf)
    newClientForMetadata(conf, hadoopConf, configurations)
  }

  protected[hive] def newClientForMetadata(
      conf: SparkConf,
      hadoopConf: Configuration,
      configurations: Map[String, String]): HiveClient = {
    val sqlConf = new SQLConf
    sqlConf.setConf(SQLContext.getSQLProperties(conf))
    val hiveMetastoreVersion = HiveUtils.hiveMetastoreVersion(sqlConf)
    val hiveMetastoreJars = HiveUtils.hiveMetastoreJars(sqlConf)
    val hiveMetastoreSharedPrefixes = HiveUtils.hiveMetastoreSharedPrefixes(sqlConf)
    val hiveMetastoreBarrierPrefixes = HiveUtils.hiveMetastoreBarrierPrefixes(sqlConf)
    val metaVersion = IsolatedClientLoader.hiveVersion(hiveMetastoreVersion)

    def addLocalHiveJars(file: File): Seq[URL] = {
      if (file.getName == "*") {
        val files = file.getParentFile.listFiles()
        if (files == null) {
          logWarning(s"Hive jar path '${file.getPath}' does not exist.")
          Nil
        } else {
          files.filter(_.getName.toLowerCase(Locale.ROOT).endsWith(".jar")).map(_.toURI.toURL).toSeq
        }
      } else {
        file.toURI.toURL :: Nil
      }
    }

    val isolatedLoader = if (hiveMetastoreJars == "builtin") {
      if (builtinHiveVersion != hiveMetastoreVersion) {
        throw new IllegalArgumentException(
          "Builtin jars can only be used when hive execution version == hive metastore version. " +
            s"Execution: $builtinHiveVersion != Metastore: $hiveMetastoreVersion. " +
            s"Specify a valid path to the correct hive jars using ${HIVE_METASTORE_JARS.key} " +
            s"or change ${HIVE_METASTORE_VERSION.key} to $builtinHiveVersion.")
      }

      // We recursively find all jars in the class loader chain,
      // starting from the given classLoader.
      def allJars(classLoader: ClassLoader): Array[URL] = classLoader match {
        case null => Array.empty[URL]
        case childFirst: ChildFirstURLClassLoader =>
          childFirst.getURLs() ++ allJars(Utils.getSparkClassLoader)
        case urlClassLoader: URLClassLoader =>
          urlClassLoader.getURLs ++ allJars(urlClassLoader.getParent)
        case other => allJars(other.getParent)
      }

      val classLoader = Utils.getContextOrSparkClassLoader
      val jars: Array[URL] = if (SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_9)) {
         // 不执行任何操作。在 Java 9 中，系统类加载器不再是 URLClassLoader，
         // 因此它不会与 allJars 中的情况匹配。它也不再公开系统类路径的 URL。
        Array.empty[URL]
      } else {
        val loadedJars = allJars(classLoader)
        // Verify at least one jar was found
        if (loadedJars.length == 0) {
          throw new IllegalArgumentException(
            "Unable to locate hive jars to connect to metastore. " +
              s"Please set ${HIVE_METASTORE_JARS.key}.")
        }
        loadedJars
      }

      logInfo(
        s"Initializing HiveMetastoreConnection version $hiveMetastoreVersion using Spark classes.")
      new IsolatedClientLoader(
        version = metaVersion,
        sparkConf = conf,
        hadoopConf = hadoopConf,
        execJars = jars.toSeq,
        config = configurations,
        isolationOn = !isCliSessionState(),
        barrierPrefixes = hiveMetastoreBarrierPrefixes,
        sharedPrefixes = hiveMetastoreSharedPrefixes)
    } else if (hiveMetastoreJars == "maven") {
      // TODO: Support for loading the jars from an already downloaded location.
      logInfo(
        s"Initializing HiveMetastoreConnection version $hiveMetastoreVersion using maven.")
      IsolatedClientLoader.forVersion(
        hiveMetastoreVersion = hiveMetastoreVersion,
        hadoopVersion = VersionInfo.getVersion,
        sparkConf = conf,
        hadoopConf = hadoopConf,
        config = configurations,
        barrierPrefixes = hiveMetastoreBarrierPrefixes,
        sharedPrefixes = hiveMetastoreSharedPrefixes)
    } else if (hiveMetastoreJars == "path") {
      // Convert to files and expand any directories.
      val jars =
        HiveUtils.hiveMetastoreJarsPath(sqlConf)
          .flatMap {
            case path if path.contains("\\") && Utils.isWindows =>
              addLocalHiveJars(new File(path))
            case path =>
              DataSource.checkAndGlobPathIfNecessary(
                pathStrings = Seq(path),
                hadoopConf = hadoopConf,
                checkEmptyGlobPath = true,
                checkFilesExist = false,
                enableGlobbing = true
              ).map(_.toUri.toURL)
          }

      logInfo(
        s"Initializing HiveMetastoreConnection version $hiveMetastoreVersion " +
          s"using path: ${jars.mkString(";")}")
      new IsolatedClientLoader(
        version = metaVersion,
        sparkConf = conf,
        hadoopConf = hadoopConf,
        execJars = jars,
        config = configurations,
        isolationOn = true,
        barrierPrefixes = hiveMetastoreBarrierPrefixes,
        sharedPrefixes = hiveMetastoreSharedPrefixes)
    } else {
      // Convert to files and expand any directories.
      val jars =
        hiveMetastoreJars
          .split(File.pathSeparator)
          .flatMap { path =>
            addLocalHiveJars(new File(path))
          }

      logInfo(
        s"Initializing HiveMetastoreConnection version $hiveMetastoreVersion " +
          s"using ${jars.mkString(":")}")
      new IsolatedClientLoader(
        version = metaVersion,
        sparkConf = conf,
        hadoopConf = hadoopConf,
        execJars = jars.toSeq,
        config = configurations,
        isolationOn = true,
        barrierPrefixes = hiveMetastoreBarrierPrefixes,
        sharedPrefixes = hiveMetastoreSharedPrefixes)
    }
    isolatedLoader.createClient()
  }

  /** Constructs a configuration for hive, where the metastore is located in a temp directory. */
  def newTemporaryConfiguration(useInMemoryDerby: Boolean): Map[String, String] = {
    val withInMemoryMode = if (useInMemoryDerby) "memory:" else ""

    val tempDir = Utils.createTempDir()
    val localMetastore = new File(tempDir, "metastore")
    val propMap: HashMap[String, String] = HashMap()
    // We have to mask all properties in hive-site.xml that relates to metastore data source
    // as we used a local metastore here.
    HiveConf.ConfVars.values().foreach { confvar =>
      if (confvar.varname.contains("datanucleus") || confvar.varname.contains("jdo")
        || confvar.varname.contains("hive.metastore.rawstore.impl")) {
        propMap.put(confvar.varname, confvar.getDefaultExpr())
      }
    }
    propMap.put(WAREHOUSE_PATH.key, localMetastore.toURI.toString)
    propMap.put(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname,
      s"jdbc:derby:${withInMemoryMode};databaseName=${localMetastore.getAbsolutePath};create=true")
    propMap.put("datanucleus.rdbms.datastoreAdapterClassName",
      "org.datanucleus.store.rdbms.adapter.DerbyAdapter")

    // Disable schema verification and allow schema auto-creation in the
    // Derby database, in case the config for the metastore is set otherwise.
    // Without these settings, starting the client fails with
    // MetaException(message:Version information not found in metastore.)
    propMap.put("hive.metastore.schema.verification", "false")
    propMap.put("datanucleus.schema.autoCreateAll", "true")

    // SPARK-11783: When "hive.metastore.uris" is set, the metastore connection mode will be
    // remote (https://cwiki.apache.org/confluence/display/Hive/AdminManual+MetastoreAdmin
    // mentions that "If hive.metastore.uris is empty local mode is assumed, remote otherwise").
    // Remote means that the metastore server is running in its own process.
    // When the mode is remote, configurations like "javax.jdo.option.ConnectionURL" will not be
    // used (because they are used by remote metastore server that talks to the database).
    // Because execution Hive should always connects to an embedded derby metastore.
    // We have to remove the value of hive.metastore.uris. So, the execution Hive client connects
    // to the actual embedded derby metastore instead of the remote metastore.
    // You can search HiveConf.ConfVars.METASTOREURIS in the code of HiveConf (in Hive's repo).
    // Then, you will find that the local metastore mode is only set to true when
    // hive.metastore.uris is not set.
    propMap.put(ConfVars.METASTOREURIS.varname, "")

    // The execution client will generate garbage events, therefore the listeners that are generated
    // for the execution clients are useless. In order to not output garbage, we don't generate
    // these listeners.
    propMap.put(ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname, "")
    propMap.put(ConfVars.METASTORE_EVENT_LISTENERS.varname, "")
    propMap.put(ConfVars.METASTORE_END_FUNCTION_LISTENERS.varname, "")

    // SPARK-21451: Spark will gather all `spark.hadoop.*` properties from a `SparkConf` to a
    // Hadoop Configuration internally, as long as it happens after SparkContext initialized.
    // Some instances such as `CliSessionState` used in `SparkSQLCliDriver` may also rely on these
    // Configuration. But it happens before SparkContext initialized, we need to take them from
    // system properties in the form of regular hadoop configurations.
    SparkHadoopUtil.get.appendSparkHadoopConfigs(sys.props.toMap, propMap)
    SparkHadoopUtil.get.appendSparkHiveConfigs(sys.props.toMap, propMap)

    propMap.toMap
  }

  /**
   * Infers the schema for Hive serde tables and returns the CatalogTable with the inferred schema.
   * When the tables are data source tables or the schema already exists, returns the original
   * CatalogTable.
   */
  def inferSchema(table: CatalogTable): CatalogTable = {
    if (DDLUtils.isDatasourceTable(table) || table.dataSchema.nonEmpty) {
      table
    } else {
      val hiveTable = HiveClientImpl.toHiveTable(table)
      // Note: Hive separates partition columns and the schema, but for us the
      // partition columns are part of the schema
      val partCols = hiveTable.getPartCols.asScala.map(HiveClientImpl.fromHiveColumn)
      val dataCols = hiveTable.getCols.asScala.map(HiveClientImpl.fromHiveColumn)
      table.copy(schema = StructType((dataCols ++ partCols).toSeq))
    }
  }
}
