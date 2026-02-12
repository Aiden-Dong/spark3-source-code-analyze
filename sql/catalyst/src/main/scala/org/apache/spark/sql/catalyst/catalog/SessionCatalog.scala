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

package org.apache.spark.sql.catalyst.catalog

import java.net.URI
import java.util.Locale
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import javax.annotation.concurrent.GuardedBy
import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, ExpressionInfo, UpCast}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias, View}
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, StringUtils}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.GLOBAL_TEMP_DATABASE
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, PartitioningUtils}
import org.apache.spark.util.Utils

object SessionCatalog {
  val DEFAULT_DATABASE = "default"
}

/**
 * 一个由 Spark 会话使用的内部目录。这个内部目录充当底层元数据存储（例如 Hive 元数据存储）的代理，它还管理所属 Spark 会话的临时视图和函数。
 *
 * Spark SQL 中的 Catalog 体系实现以 SessionCatalog 为主体，通过 SparkSession (Spark 程序入口）提供给外部调用。
 * 一般一个 SparkSession 对应一个 SessionCatalog。
 *
 * 本质上，Session Catalog 起到了一个代理的作用，对底层的元数据信息、临时表信息、视图信息和函数信息进行了封装。
 *
 * 这个类必须是线程安全的。
 */
class SessionCatalog(   /** => [[HiveSessionCatalog]] */
    externalCatalogBuilder: () => ExternalCatalog,              // SparkSession.sharedState.externalCatalog [spark.sql.catalogImplementation ：  [hive, in-memory]]
    globalTempViewManagerBuilder: () => GlobalTempViewManager,  // SparkSession.sharedState.globalTempViewManager
    functionRegistry: FunctionRegistry,                         //** 函数注册   : [[SparkSessionExtensions.registerFunctions(FunctionRegistry.builtin.clone())]] */
    tableFunctionRegistry: TableFunctionRegistry,               /** 表函数注册， 这些函数返回整个数据集 : SELECT * FROM range(1, 100); [[SparkSessionExtensions.registerTableFunctions(TableFunctionRegistry.builtin.clone())]] */
    hadoopConf: Configuration,                                  // Hadoop 配置管理
    parser: ParserInterface,                                    // SparkSession.buildParser(session, new SparkSqlParser())
    functionResourceLoader: FunctionResourceLoader,             /** [[SessionResourceLoader]] */
    functionExpressionBuilder: FunctionExpressionBuilder,       /** [[HiveUDFExpressionBuilder]] */
    cacheSize: Int = SQLConf.get.tableRelationCacheSize,        // spark.sql.filesourceTableRelationCacheSize : 1000
    cacheTTL: Long = SQLConf.get.metadataCacheTTL               // spark.sql.metadataCacheTTLSeconds : -1
) extends SQLConfHelper with Logging {
  import SessionCatalog._
  import CatalogTypes.TablePartitionSpec

  // For testing only.
  def this(
      externalCatalog: ExternalCatalog,
      functionRegistry: FunctionRegistry,
      tableFunctionRegistry: TableFunctionRegistry,
      conf: SQLConf) = {
    this(
      () => externalCatalog,
      () => new GlobalTempViewManager(conf.getConf(GLOBAL_TEMP_DATABASE)),
      functionRegistry,
      tableFunctionRegistry,
      new Configuration(),
      new CatalystSqlParser(),
      DummyFunctionResourceLoader,
      DummyFunctionExpressionBuilder,
      conf.tableRelationCacheSize,
      conf.metadataCacheTTL)
  }

  def this(
      externalCatalog: ExternalCatalog,
      functionRegistry: FunctionRegistry,
      conf: SQLConf) = {
    this(externalCatalog, functionRegistry, new SimpleTableFunctionRegistry, conf)
  }

  def this(
      externalCatalog: ExternalCatalog,
      functionRegistry: FunctionRegistry,
      tableFunctionRegistry: TableFunctionRegistry) = {
    this(externalCatalog, functionRegistry, tableFunctionRegistry, SQLConf.get)
  }

  def this(externalCatalog: ExternalCatalog, functionRegistry: FunctionRegistry) = {
    this(externalCatalog, functionRegistry, SQLConf.get)
  }

  def this(externalCatalog: ExternalCatalog) = {
    this(externalCatalog, new SimpleFunctionRegistry)
  }

  lazy val externalCatalog = externalCatalogBuilder()                                                // SparkSession.sharedState.externalCatalog [spark.sql.catalogImplementation ：  [hive, in-memory]]
  lazy val globalTempViewManager = globalTempViewManagerBuilder()                                    // 全局视图 : SparkSession.sharedState.globalTempViewManager
  @GuardedBy("this") protected val tempViews = new mutable.HashMap[String, TemporaryViewRelation]    // 当前 session级别的视图
  @GuardedBy("this") protected var currentDb: String = formatDatabaseName(DEFAULT_DATABASE)          // 当前数据库名字

  private val validNameFormat = "([\\w_]+)".r

  /**
   * 检查给定名称是否符合 Hive 标准（"[a-zA-Z_0-9]+"），
   * 即此名称是否仅包含字符、数字和下划线。
   *
   * 此方法旨在与 org.apache.hadoop.hive.metastore.MetaStoreUtils.validateName 具有相同的行为。
   */
  private def validateName(name: String): Unit = {
    if (!validNameFormat.pattern.matcher(name).matches()) {
      throw QueryCompilationErrors.invalidNameForTableOrDatabaseError(name)
    }
  }

  /** 格式化表名，考虑大小写敏感性。 */
  protected[this] def formatTableName(name: String): String = {
    if (conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
  }

  /** 格式化表名，考虑大小写敏感性。 */
  protected[this] def formatDatabaseName(name: String): String = {
    if (conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
  }

  private val tableRelationCache: Cache[QualifiedTableName, LogicalPlan] = {
    var builder = CacheBuilder.newBuilder()
      .maximumSize(cacheSize)

    if (cacheTTL > 0) {
      builder = builder.expireAfterWrite(cacheTTL, TimeUnit.SECONDS)
    }

    builder.build[QualifiedTableName, LogicalPlan]()
  }

  /** 此方法提供了一种获取缓存计划的方式 */
  def getCachedPlan(t: QualifiedTableName, c: Callable[LogicalPlan]): LogicalPlan = {
    tableRelationCache.get(t, c)
  }

  /** 此方法提供了一种在键存在时获取缓存计划的方式 */
  def getCachedTable(key: QualifiedTableName): LogicalPlan = {
    tableRelationCache.getIfPresent(key)
  }

  /** 此方法提供了一种缓存计划的方式 */
  def cacheTable(t: QualifiedTableName, l: LogicalPlan): Unit = {
    tableRelationCache.put(t, l)
  }

  /** 此方法提供了一种使缓存计划失效的方式 */
  def invalidateCachedTable(key: QualifiedTableName): Unit = {
    tableRelationCache.invalidate(key)
  }

  /** 此方法丢弃给定表标识符的任何缓存表关系计划 */
  def invalidateCachedTable(name: TableIdentifier): Unit = {
    val dbName = formatDatabaseName(name.database.getOrElse(currentDb))
    val tableName = formatTableName(name.table)
    invalidateCachedTable(QualifiedTableName(dbName, tableName))
  }

/** 此方法提供了一种使所有缓存计划失效的方式 */
  def invalidateAllCachedTables(): Unit = {
    tableRelationCache.invalidateAll()
  }

  /**
   * 此方法用于在将路径存储到底层外部目录之前使给定路径成为限定路径。
   * 因此，当路径不包含方案时，在更改默认文件系统后，此路径不会更改。
   */
  private def makeQualifiedPath(path: URI): URI = {
    CatalogUtils.makeQualifiedPath(path, hadoopConf)
  }

  private def requireDbExists(db: String): Unit = {
    if (!databaseExists(db)) {
      throw new NoSuchDatabaseException(db)
    }
  }

  private def requireTableExists(name: TableIdentifier): Unit = {
    if (!tableExists(name)) {
      val db = name.database.getOrElse(currentDb)
      throw new NoSuchTableException(db = db, table = name.table)
    }
  }

  private def requireTableNotExists(name: TableIdentifier): Unit = {
    if (tableExists(name)) {
      val db = name.database.getOrElse(currentDb)
      throw new TableAlreadyExistsException(db = db, table = name.table)
    }
  }


  // ----------------------------------------------------------------------------
  // 数据库
  // ----------------------------------------------------------------------------
  // 此类别中的所有方法都直接与底层目录交互 : 调用 ExternalCatalog 操作库
  // ----------------------------------------------------------------------------

  def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    val dbName = formatDatabaseName(dbDefinition.name)
    if (dbName == globalTempViewManager.database) {
      throw QueryCompilationErrors.cannotCreateDatabaseWithSameNameAsPreservedDatabaseError(
        globalTempViewManager.database)
    }
    validateName(dbName)
    externalCatalog.createDatabase(
      dbDefinition.copy(name = dbName, locationUri = makeQualifiedDBPath(dbDefinition.locationUri)),
      ignoreIfExists)
  }

  private def makeQualifiedDBPath(locationUri: URI): URI = {
    CatalogUtils.makeQualifiedDBObjectPath(locationUri, conf.warehousePath, hadoopConf)
  }

  def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    val dbName = formatDatabaseName(db)
    if (dbName == DEFAULT_DATABASE) {
      throw QueryCompilationErrors.cannotDropDefaultDatabaseError
    }
    if (!ignoreIfNotExists) {
      requireDbExists(dbName)
    }
    if (cascade && databaseExists(dbName)) {
      listTables(dbName).foreach { t =>
        invalidateCachedTable(QualifiedTableName(dbName, t.table))
      }
    }
    externalCatalog.dropDatabase(dbName, ignoreIfNotExists, cascade)
  }

  def alterDatabase(dbDefinition: CatalogDatabase): Unit = {
    val dbName = formatDatabaseName(dbDefinition.name)
    requireDbExists(dbName)
    externalCatalog.alterDatabase(dbDefinition.copy(
      name = dbName, locationUri = makeQualifiedDBPath(dbDefinition.locationUri)))
  }

  def getDatabaseMetadata(db: String): CatalogDatabase = {
    val dbName = formatDatabaseName(db)
    requireDbExists(dbName)
    externalCatalog.getDatabase(dbName)
  }

  def databaseExists(db: String): Boolean = {
    val dbName = formatDatabaseName(db)
    externalCatalog.databaseExists(dbName)
  }

  def listDatabases(): Seq[String] = {
    externalCatalog.listDatabases()
  }

  def listDatabases(pattern: String): Seq[String] = {
    externalCatalog.listDatabases(pattern)
  }

  def getCurrentDatabase: String = synchronized { currentDb }

  def setCurrentDatabase(db: String): Unit = {
    val dbName = formatDatabaseName(db)
    if (dbName == globalTempViewManager.database) {
      throw QueryCompilationErrors.cannotUsePreservedDatabaseAsCurrentDatabaseError(
        globalTempViewManager.database)
    }
    requireDbExists(dbName)
    synchronized { currentDb = dbName }
  }

  /** 当用户未提供数据库位置时，获取创建非默认数据库的路径 */
  def getDefaultDBPath(db: String): URI = {
    CatalogUtils.stringToURI(formatDatabaseName(db) + ".db")
  }

  // ----------------------------------------------------------------------------
  // 表
  // ----------------------------------------------------------------------------
  // 有两种表：临时视图和元数据存储表。
  // 临时视图在会话之间是隔离的，不属于任何特定数据库。
  // 元数据存储表可以跨多个会话使用，因为它们的元数据持久化在底层目录中 : 调用 ExternalCatalog 操作
  // ----------------------------------------------------------------------------

  // ----------------------------------------------------
  // |         仅与元数据存储表交互的方法                   |
  // ----------------------------------------------------


  /** 在 `tableDefinition` 中指定的数据库中创建元数据存储表。  如果未指定此类数据库，则在当前数据库中创建它。 */
  def createTable(
      tableDefinition: CatalogTable,
      ignoreIfExists: Boolean,
      validateLocation: Boolean = true): Unit = {
    val isExternal = tableDefinition.tableType == CatalogTableType.EXTERNAL
    if (isExternal && tableDefinition.storage.locationUri.isEmpty) {
      throw QueryCompilationErrors.createExternalTableWithoutLocationError
    }

    val db = formatDatabaseName(tableDefinition.identifier.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableDefinition.identifier.table)
    val tableIdentifier = TableIdentifier(table, Some(db))
    validateName(table)

    val newTableDefinition = if (tableDefinition.storage.locationUri.isDefined
      && !tableDefinition.storage.locationUri.get.isAbsolute) {
      // make the location of the table qualified.
      val qualifiedTableLocation =
        makeQualifiedTablePath(tableDefinition.storage.locationUri.get, db)
      tableDefinition.copy(
        storage = tableDefinition.storage.copy(locationUri = Some(qualifiedTableLocation)),
        identifier = tableIdentifier)
    } else {
      tableDefinition.copy(identifier = tableIdentifier)
    }

    requireDbExists(db)
    if (tableExists(newTableDefinition.identifier)) {
      if (!ignoreIfExists) {
        throw new TableAlreadyExistsException(db = db, table = table)
      }
    } else {
      if (validateLocation) {
        validateTableLocation(newTableDefinition)
      }
      externalCatalog.createTable(newTableDefinition, ignoreIfExists)
    }
  }

  def validateTableLocation(table: CatalogTable): Unit = {
    // SPARK-19724: the default location of a managed table should be non-existent or empty.
    if (table.tableType == CatalogTableType.MANAGED) {
      val tableLocation =
        new Path(table.storage.locationUri.getOrElse(defaultTablePath(table.identifier)))
      val fs = tableLocation.getFileSystem(hadoopConf)

      if (fs.exists(tableLocation) && fs.listStatus(tableLocation).nonEmpty) {
        throw QueryCompilationErrors.cannotOperateManagedTableWithExistingLocationError(
          "create", table.identifier, tableLocation)
      }
    }
  }

  private def makeQualifiedTablePath(locationUri: URI, database: String): URI = {
    if (locationUri.isAbsolute) {
      locationUri
    } else if (new Path(locationUri).isAbsolute) {
      makeQualifiedPath(locationUri)
    } else {
      val dbName = formatDatabaseName(database)
      val dbLocation = makeQualifiedDBPath(getDatabaseMetadata(dbName).locationUri)
      new Path(new Path(dbLocation), CatalogUtils.URIToString(locationUri)).toUri
    }
  }

  /**
   * 更改由 `tableDefinition` 标识的现有元数据存储表的元数据。
   * 如果 `tableDefinition` 中未指定数据库，则假定该表在当前数据库中。
   *
   * 注意：如果底层实现不支持更改某个字段，这将变成无操作。
   */
  def alterTable(tableDefinition: CatalogTable): Unit = {
    val db = formatDatabaseName(tableDefinition.identifier.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableDefinition.identifier.table)
    val tableIdentifier = TableIdentifier(table, Some(db))
    requireDbExists(db)
    requireTableExists(tableIdentifier)
    val newTableDefinition = if (tableDefinition.storage.locationUri.isDefined
      && !tableDefinition.storage.locationUri.get.isAbsolute) {
      // make the location of the table qualified.
      val qualifiedTableLocation =
        makeQualifiedTablePath(tableDefinition.storage.locationUri.get, db)
      tableDefinition.copy(
        storage = tableDefinition.storage.copy(locationUri = Some(qualifiedTableLocation)),
        identifier = tableIdentifier)
    } else {
      tableDefinition.copy(identifier = tableIdentifier)
    }

    externalCatalog.alterTable(newTableDefinition)
  }

  /**
   * 更改由提供的表标识符标识的表的数据模式。新数据模式不应与现有分区列有冲突的列名， 并且仍应包含所有现有数据列。
   *
   * @param identifier 表标识符
   * @param newDataSchema 用于表的更新数据模式
   */
  def alterTableDataSchema(
      identifier: TableIdentifier,
      newDataSchema: StructType): Unit = {
    val db = formatDatabaseName(identifier.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(identifier.table)
    val tableIdentifier = TableIdentifier(table, Some(db))
    requireDbExists(db)
    requireTableExists(tableIdentifier)

    val catalogTable = externalCatalog.getTable(db, table)
    val oldDataSchema = catalogTable.dataSchema
    // not supporting dropping columns yet
    val nonExistentColumnNames =
      oldDataSchema.map(_.name).filterNot(columnNameResolved(newDataSchema, _))
    if (nonExistentColumnNames.nonEmpty) {
      throw QueryCompilationErrors.dropNonExistentColumnsNotSupportedError(nonExistentColumnNames)
    }

    externalCatalog.alterTableDataSchema(db, table, newDataSchema)
  }

  private def columnNameResolved(schema: StructType, colName: String): Boolean = {
    schema.fields.map(_.name).exists(conf.resolver(_, colName))
  }

  /** 更改由提供的表标识符标识的现有元数据存储表的 Spark 统计信息。 */
  def alterTableStats(identifier: TableIdentifier, newStats: Option[CatalogStatistics]): Unit = {
    val db = formatDatabaseName(identifier.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(identifier.table)
    val tableIdentifier = TableIdentifier(table, Some(db))
    requireDbExists(db)
    requireTableExists(tableIdentifier)
    externalCatalog.alterTableStats(db, table, newStats)
    // Invalidate the table relation cache
    refreshTable(identifier)
  }

  /** 返回具有指定名称的表/视图是否存在。如果未指定数据库，则检查当前数据库。 */
  def tableExists(name: TableIdentifier): Boolean = {
    val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(name.table)
    externalCatalog.tableExists(db, table)
  }

  /**
   * 检索现有永久表/视图的所有元数据。如果未指定数据库，则假定表/视图在当前数据库中。
   * 只返回可以检索的属于同一数据库的表/视图。
   * 例如，如果无法检索任何请求的表，则返回空列表。 不保证返回表的顺序。
   */
  @throws[NoSuchDatabaseException]
  @throws[NoSuchTableException]
  def getTableMetadata(name: TableIdentifier): CatalogTable = {
    val t = getTableRawMetadata(name)
    t.copy(schema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(t.schema))
  }

  /**
   * 检索现有永久表/视图的元数据。如果未指定数据库，则假定表/视图在当前数据库中。
   */
  @throws[NoSuchDatabaseException]
  @throws[NoSuchTableException]
  def getTableRawMetadata(name: TableIdentifier): CatalogTable = {
    val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(name.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Some(db)))
    externalCatalog.getTable(db, table)
  }

  /**
   * 检索现有永久表/视图的所有元数据。如果未指定数据库，则假定表/视图在当前数据库中。
   * 只返回可以检索的属于同一数据库的表/视图。
   * 例如，如果无法检索任何请求的表，则返回空列表。  不保证返回表的顺序。
   */
  @throws[NoSuchDatabaseException]
  def getTablesByName(names: Seq[TableIdentifier]): Seq[CatalogTable] = {
    if (names.nonEmpty) {
      val dbs = names.map(_.database.getOrElse(getCurrentDatabase))
      if (dbs.distinct.size != 1) {
        val tables = names.map(name => formatTableName(name.table))
        val qualifiedTableNames = dbs.zip(tables).map { case (d, t) => QualifiedTableName(d, t)}
        throw QueryCompilationErrors.cannotRetrieveTableOrViewNotInSameDatabaseError(
          qualifiedTableNames)
      }
      val db = formatDatabaseName(dbs.head)
      requireDbExists(db)
      val tables = names.map(name => formatTableName(name.table))
      externalCatalog.getTablesByName(db, tables)
    } else {
      Seq.empty
    }
  }

  /**
   * 将存储在给定路径中的文件加载到现有元数据存储表中。
   * 如果未指定数据库，则假定该表在当前数据库中。
   * 如果在数据库中找不到指定的表，则抛出 [[NoSuchTableException]]。
   */
  def loadTable(
      name: TableIdentifier,
      loadPath: String,
      isOverwrite: Boolean,
      isSrcLocal: Boolean): Unit = {
    val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(name.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Some(db)))
    externalCatalog.loadTable(db, table, loadPath, isOverwrite, isSrcLocal)
  }

  /**
   * 将存储在给定路径中的文件加载到现有元数据存储表的分区中。
   * 如果未指定数据库，则假定该表在当前数据库中。
   * 如果在数据库中找不到指定的表，则抛出 [[NoSuchTableException]]。
   */
  def loadPartition(
      name: TableIdentifier,
      loadPath: String,
      spec: TablePartitionSpec,
      isOverwrite: Boolean,
      inheritTableSpecs: Boolean,
      isSrcLocal: Boolean): Unit = {
    val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(name.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Some(db)))
    requireNonEmptyValueInPartitionSpec(Seq(spec))
    externalCatalog.loadPartition(
      db, table, loadPath, spec, isOverwrite, inheritTableSpecs, isSrcLocal)
  }

  def defaultTablePath(tableIdent: TableIdentifier): URI = {
    val dbName = formatDatabaseName(tableIdent.database.getOrElse(getCurrentDatabase))
    val dbLocation = getDatabaseMetadata(dbName).locationUri

    new Path(new Path(dbLocation), formatTableName(tableIdent.table)).toUri
  }
  // ---------------------------------------------------------------------------
  // | 仅与临时视图交互的方法  : （tempViews，globalTempViewManager ）              |
  // ---------------------------------------------------------------------------

  /** 创建本地临时视图 */
  def createTempView(
      name: String,
      viewDefinition: TemporaryViewRelation,
      overrideIfExists: Boolean): Unit = synchronized {
    val table = formatTableName(name)
    if (tempViews.contains(table) && !overrideIfExists) {
      throw new TempTableAlreadyExistsException(name)
    }
    tempViews.put(table, viewDefinition)
  }

  /** 创建全局临时视图 */
  def createGlobalTempView(
      name: String,
      viewDefinition: TemporaryViewRelation,
      overrideIfExists: Boolean): Unit = {
    globalTempViewManager.create(formatTableName(name), viewDefinition, overrideIfExists)
  }

  /** 更改与给定名称匹配的本地/全局临时视图的定义，如果匹配并更改了临时视图则返回 true，否则返回 false。 */
  def alterTempViewDefinition(
      name: TableIdentifier,
      viewDefinition: TemporaryViewRelation): Boolean = synchronized {
    val viewName = formatTableName(name.table)
    if (name.database.isEmpty) {
      if (tempViews.contains(viewName)) {
        createTempView(viewName, viewDefinition, overrideIfExists = true)
        true
      } else {
        false
      }
    } else if (formatDatabaseName(name.database.get) == globalTempViewManager.database) {
      globalTempViewManager.update(viewName, viewDefinition)
    } else {
      false
    }
  }

  /** 完全按照存储的方式返回本地临时视图 */
  def getRawTempView(name: String): Option[TemporaryViewRelation] = synchronized {
    tempViews.get(formatTableName(name))
  }

  /** 从存储的临时视图生成 [[View]] 操作符   */
  def getTempView(name: String): Option[View] = synchronized {
    getRawTempView(name).map(getTempViewPlan)
  }

  def getTempViewNames(): Seq[String] = synchronized {
    tempViews.keySet.toSeq
  }

  /** 完全按照存储的方式返回全局临时视图 */
  def getRawGlobalTempView(name: String): Option[TemporaryViewRelation] = {
    globalTempViewManager.get(formatTableName(name))
  }

  /** 从存储的全局临时视图生成 [[View]] 操作符 */
  def getGlobalTempView(name: String): Option[View] = {
    getRawGlobalTempView(name).map(getTempViewPlan)
  }

  /**
   * 删除本地临时视图
   * 如果成功删除此视图，则返回 true，否则返回 false。
   */
  def dropTempView(name: String): Boolean = synchronized {
    tempViews.remove(formatTableName(name)).isDefined
  }


  /**
   * 删除全局临时视图
   * 如果成功删除此视图，则返回 true，否则返回 false。
   */
  def dropGlobalTempView(name: String): Boolean = {
    globalTempViewManager.remove(formatTableName(name))
  }

  // -------------------------------------------------------------
  // |           与临时表和元数据存储表交互的方法                      |
  // -------------------------------------------------------------

  /**
   * 检索现有临时视图或永久表/视图的元数据。
   *
   * 如果在 `name` 中指定了数据库，这将返回该数据库中表/视图的元数据。
   * 如果未指定数据库，这将首先尝试获取具有相同名称的临时视图的元数据， 然后，如果不存在，则返回当前数据库中表/视图的元数据。
   */
  def getTempViewOrPermanentTableMetadata(name: TableIdentifier): CatalogTable = synchronized {
    val table = formatTableName(name.table)
    if (name.database.isEmpty) {
      tempViews.get(table).map(_.tableMeta).getOrElse(getTableMetadata(name))
    } else if (formatDatabaseName(name.database.get) == globalTempViewManager.database) {
      globalTempViewManager.get(table).map(_.tableMeta)
        .getOrElse(throw new NoSuchTableException(globalTempViewManager.database, table))
    } else {
      getTableMetadata(name)
    }
  }

  /**
   * 重命名表。
   *
   * 如果在 `oldName` 中指定了数据库，这将重命名该数据库中的表。
   * 如果未指定数据库，这将首先尝试重命名具有相同名称的临时视图， 然后，如果不存在，则重命名当前数据库中的表。
   *
   * 这假定 `newName` 中指定的数据库与 `oldName` 中的数据库匹配。
   */
  def renameTable(oldName: TableIdentifier, newName: TableIdentifier): Unit = synchronized {
    val db = formatDatabaseName(oldName.database.getOrElse(currentDb))
    newName.database.map(formatDatabaseName).foreach { newDb =>
      if (db != newDb) {
        throw QueryCompilationErrors.renameTableSourceAndDestinationMismatchError(db, newDb)
      }
    }

    val oldTableName = formatTableName(oldName.table)
    val newTableName = formatTableName(newName.table)
    if (db == globalTempViewManager.database) {
      globalTempViewManager.rename(oldTableName, newTableName)
    } else {
      requireDbExists(db)
      if (oldName.database.isDefined || !tempViews.contains(oldTableName)) {
        validateName(newTableName)
        validateNewLocationOfRename(
          TableIdentifier(oldTableName, Some(db)), TableIdentifier(newTableName, Some(db)))
        externalCatalog.renameTable(db, oldTableName, newTableName)
      } else {
        if (newName.database.isDefined) {
          throw QueryCompilationErrors.cannotRenameTempViewWithDatabaseSpecifiedError(
            oldName, newName)
        }
        if (tempViews.contains(newTableName)) {
          throw QueryCompilationErrors.cannotRenameTempViewToExistingTableError(
            oldName, newName)
        }
        val table = tempViews(oldTableName)
        tempViews.remove(oldTableName)
        tempViews.put(newTableName, table)
      }
    }
  }

  /**
   * 删除表。
   *
   * 如果在 `name` 中指定了数据库，这将从该数据库中删除表。
   * 如果未指定数据库，这将首先尝试删除具有相同名称的临时视图，然后，如果不存在，则从当前数据库中删除表。
   */
  def dropTable(
      name: TableIdentifier,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = synchronized {
    val db = formatDatabaseName(name.database.getOrElse(currentDb))
    val table = formatTableName(name.table)
    if (db == globalTempViewManager.database) {
      val viewExists = globalTempViewManager.remove(table)
      if (!viewExists && !ignoreIfNotExists) {
        throw new NoSuchTableException(globalTempViewManager.database, table)
      }
    } else {
      if (name.database.isDefined || !tempViews.contains(table)) {
        requireDbExists(db)
        // When ignoreIfNotExists is false, no exception is issued when the table does not exist.
        // Instead, log it as an error message.
        if (tableExists(TableIdentifier(table, Option(db)))) {
          externalCatalog.dropTable(db, table, ignoreIfNotExists = true, purge = purge)
        } else if (!ignoreIfNotExists) {
          throw new NoSuchTableException(db = db, table = table)
        }
      } else {
        tempViews.remove(table)
      }
    }
  }

  /**
   * 返回表示给定表或视图的 [[LogicalPlan]]。
   *
   * 如果在 `name` 中指定了数据库，这将返回该数据库中的表/视图。
   * 如果未指定数据库，这将首先尝试返回具有相同名称的临时视图， 然后，如果不存在，则返回当前数据库中的表/视图。
   *
   * 注意，全局临时视图数据库在这里也是有效的，这将返回与给定名称匹配的全局临时视图。
   *
   * 如果关系是视图，我们从视图描述生成 [[View]] 操作符，
   * 并将逻辑计划包装在 [[SubqueryAlias]] 中，它将跟踪视图的名称。
   * [[SubqueryAlias]] 还将跟踪表/视图的名称和数据库（可选）
   *
   * @param name 我们查找的表/视图的名称
   */
  def lookupRelation(name: TableIdentifier): LogicalPlan = {
    synchronized {
      val db = formatDatabaseName(name.database.getOrElse(currentDb))
      val table = formatTableName(name.table)
      if (db == globalTempViewManager.database) {
        globalTempViewManager.get(table).map { viewDef =>
          SubqueryAlias(table, db, getTempViewPlan(viewDef))
        }.getOrElse(throw new NoSuchTableException(db, table))
      } else if (name.database.isDefined || !tempViews.contains(table)) {
        val metadata = externalCatalog.getTable(db, table)
        getRelation(metadata)
      } else {
        SubqueryAlias(table, getTempViewPlan(tempViews(table)))
      }
    }
  }

  def getRelation(
      metadata: CatalogTable,
      options: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty()): LogicalPlan = {
    val name = metadata.identifier
    val db = formatDatabaseName(name.database.getOrElse(currentDb))
    val table = formatTableName(name.table)
    val multiParts = Seq(CatalogManager.SESSION_CATALOG_NAME, db, table)

    if (metadata.tableType == CatalogTableType.VIEW) {
      // The relation is a view, so we wrap the relation by:
      // 1. Add a [[View]] operator over the relation to keep track of the view desc;
      // 2. Wrap the logical plan in a [[SubqueryAlias]] which tracks the name of the view.
      SubqueryAlias(multiParts, fromCatalogTable(metadata, isTempView = false))
    } else {
      SubqueryAlias(multiParts, UnresolvedCatalogRelation(metadata, options))
    }
  }

  private def getTempViewPlan(viewInfo: TemporaryViewRelation): View = viewInfo.plan match {
    case Some(p) => View(desc = viewInfo.tableMeta, isTempView = true, child = p)
    case None => fromCatalogTable(viewInfo.tableMeta, isTempView = true)
  }

  private def buildViewDDL(metadata: CatalogTable, isTempView: Boolean): Option[String] = {
    if (isTempView) {
      None
    } else {
      val viewName = metadata.identifier.unquotedString
      val viewText = metadata.viewText.get
      val userSpecifiedColumns =
        if (metadata.schema.fieldNames.toSeq == metadata.viewQueryColumnNames) {
          ""
        } else {
          s"(${metadata.schema.fieldNames.mkString(", ")})"
        }
      Some(s"CREATE OR REPLACE VIEW $viewName $userSpecifiedColumns AS $viewText")
    }
  }

  private def isHiveCreatedView(metadata: CatalogTable): Boolean = {
    // For views created by hive without explicit column names, there will be auto-generated
    // column names like "_c0", "_c1", "_c2"...
    metadata.viewQueryColumnNames.isEmpty &&
      metadata.schema.fieldNames.exists(_.matches("_c[0-9]+"))
  }

  private def fromCatalogTable(metadata: CatalogTable, isTempView: Boolean): View = {
    val viewText = metadata.viewText.getOrElse {
      throw new IllegalStateException("Invalid view without text.")
    }
    val viewConfigs = metadata.viewSQLConfigs
    val origin = Origin(
      objectType = Some("VIEW"),
      objectName = Some(metadata.qualifiedName)
    )
    val parsedPlan = SQLConf.withExistingConf(View.effectiveSQLConf(viewConfigs, isTempView)) {
      try {
        CurrentOrigin.withOrigin(origin) {
          parser.parseQuery(viewText)
        }
      } catch {
        case _: ParseException =>
          throw QueryCompilationErrors.invalidViewText(viewText, metadata.qualifiedName)
      }
    }
    val projectList = if (!isHiveCreatedView(metadata)) {
      val viewColumnNames = if (metadata.viewQueryColumnNames.isEmpty) {
        // For view created before Spark 2.2.0, the view text is already fully qualified, the plan
        // output is the same with the view output.
        metadata.schema.fieldNames.toSeq
      } else {
        assert(metadata.viewQueryColumnNames.length == metadata.schema.length)
        metadata.viewQueryColumnNames
      }

      // For view queries like `SELECT * FROM t`, the schema of the referenced table/view may
      // change after the view has been created. We need to add an extra SELECT to pick the columns
      // according to the recorded column names (to get the correct view column ordering and omit
      // the extra columns that we don't require), with UpCast (to make sure the type change is
      // safe) and Alias (to respect user-specified view column names) according to the view schema
      // in the catalog.
      // Note that, the column names may have duplication, e.g. `CREATE VIEW v(x, y) AS
      // SELECT 1 col, 2 col`. We need to make sure that the matching attributes have the same
      // number of duplications, and pick the corresponding attribute by ordinal.
      val viewConf = View.effectiveSQLConf(metadata.viewSQLConfigs, isTempView)
      val normalizeColName: String => String = if (viewConf.caseSensitiveAnalysis) {
        identity
      } else {
        _.toLowerCase(Locale.ROOT)
      }
      val nameToCounts = viewColumnNames.groupBy(normalizeColName).mapValues(_.length)
      val nameToCurrentOrdinal = scala.collection.mutable.HashMap.empty[String, Int]
      val viewDDL = buildViewDDL(metadata, isTempView)

      viewColumnNames.zip(metadata.schema).map { case (name, field) =>
        val normalizedName = normalizeColName(name)
        val count = nameToCounts(normalizedName)
        val ordinal = nameToCurrentOrdinal.getOrElse(normalizedName, 0)
        nameToCurrentOrdinal(normalizedName) = ordinal + 1
        val col = GetViewColumnByNameAndOrdinal(
          metadata.identifier.toString, name, ordinal, count, viewDDL)
        Alias(UpCast(col, field.dataType), field.name)(explicitMetadata = Some(field.metadata))
      }
    } else {
      // For view created by hive, the parsed view plan may have different output columns with
      // the schema stored in metadata. For example: `CREATE VIEW v AS SELECT 1 FROM t`
      // the schema in metadata will be `_c0` while the parsed view plan has column named `1`
      metadata.schema.zipWithIndex.map { case (field, index) =>
        val col = GetColumnByOrdinal(index, field.dataType)
        Alias(UpCast(col, field.dataType), field.name)(explicitMetadata = Some(field.metadata))
      }
    }
    View(desc = metadata, isTempView = isTempView, child = Project(projectList, parsedPlan))
  }

  def lookupTempView(table: String): Option[SubqueryAlias] = {
    val formattedTable = formatTableName(table)
    getTempView(formattedTable).map { view =>
      SubqueryAlias(formattedTable, view)
    }
  }

  def lookupGlobalTempView(db: String, table: String): Option[SubqueryAlias] = {
    val formattedDB = formatDatabaseName(db)
    if (formattedDB == globalTempViewManager.database) {
      val formattedTable = formatTableName(table)
      getGlobalTempView(formattedTable).map { view =>
        SubqueryAlias(formattedTable, formattedDB, view)
      }
    } else {
      None
    }
  }

  /**  返回给定名称部分是否属于临时或全局临时视图 */
  def isTempView(nameParts: Seq[String]): Boolean = {
    if (nameParts.length > 2) return false
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    isTempView(nameParts.asTableIdentifier)
  }

  def lookupTempView(name: TableIdentifier): Option[View] = {
    val tableName = formatTableName(name.table)
    if (name.database.isEmpty) {
      tempViews.get(tableName).map(getTempViewPlan)
    } else if (formatDatabaseName(name.database.get) == globalTempViewManager.database) {
      globalTempViewManager.get(tableName).map(getTempViewPlan)
    } else {
      None
    }
  }


  /**
   * 返回具有指定名称的表是否为临时视图。
   * 注意：仅当未显式指定数据库时才检查临时视图缓存。
   */
  def isTempView(name: TableIdentifier): Boolean = synchronized {
    lookupTempView(name).isDefined
  }

  def isView(nameParts: Seq[String]): Boolean = {
    nameParts.length <= 2 && {
      import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
      val ident = nameParts.asTableIdentifier
      try {
        getTempViewOrPermanentTableMetadata(ident).tableType == CatalogTableType.VIEW
      } catch {
        case _: NoSuchTableException => false
        case _: NoSuchDatabaseException => false
        case _: NoSuchNamespaceException => false
      }
    }
  }

  /**
   * 列出指定数据库中的所有表，包括本地临时视图。
   * 注意，如果指定的数据库是全局临时视图数据库，我们将列出全局临时视图。
   */
  def listTables(db: String): Seq[TableIdentifier] = listTables(db, "*")

  /**
   * 列出指定数据库中所有匹配的表，包括本地临时视图。
   * 注意，如果指定的数据库是全局临时视图数据库，我们将列出全局临时视图。
   */
  def listTables(db: String, pattern: String): Seq[TableIdentifier] = listTables(db, pattern, true)

  /**
   * 列出指定数据库中所有匹配的表，如果启用 includeLocalTempViews，则包括本地临时视图。
   *
   * 注意，如果指定的数据库是全局临时视图数据库，我们将列出全局临时视图。
   */
  def listTables(
      db: String,
      pattern: String,
      includeLocalTempViews: Boolean): Seq[TableIdentifier] = {
    val dbName = formatDatabaseName(db)
    val dbTables = if (dbName == globalTempViewManager.database) {
      globalTempViewManager.listViewNames(pattern).map { name =>
        TableIdentifier(name, Some(globalTempViewManager.database))
      }
    } else {
      requireDbExists(dbName)
      externalCatalog.listTables(dbName, pattern).map { name =>
        TableIdentifier(name, Some(dbName))
      }
    }

    if (includeLocalTempViews) {
      dbTables ++ listLocalTempViews(pattern)
    } else {
      dbTables
    }
  }

  /** 列出指定数据库中所有匹配的视图，包括本地临时视图。 */
  def listViews(db: String, pattern: String): Seq[TableIdentifier] = {
    val dbName = formatDatabaseName(db)
    val dbViews = if (dbName == globalTempViewManager.database) {
      globalTempViewManager.listViewNames(pattern).map { name =>
        TableIdentifier(name, Some(globalTempViewManager.database))
      }
    } else {
      requireDbExists(dbName)
      externalCatalog.listViews(dbName, pattern).map { name =>
        TableIdentifier(name, Some(dbName))
      }
    }

    dbViews ++ listLocalTempViews(pattern)
  }

  /** 列出所有匹配的本地临时视图。 */
  def listLocalTempViews(pattern: String): Seq[TableIdentifier] = {
    synchronized {
      StringUtils.filterPattern(tempViews.keys.toSeq, pattern).map { name =>
        TableIdentifier(name)
      }
    }
  }

  /**
   * 刷新会话目录维护的结构中的表条目，例如：
   *   - 临时或全局临时视图名称到其逻辑计划的映射
   *   - 将表标识符映射到其逻辑计划的关系缓存
   *
   * 对于临时视图，它刷新其逻辑计划，因此可以刷新视图中使用的基础关系的文件索引（例如 `HadoopFsRelation`）。该方法仍将视图保留在会话目录的内部列表中。
   *
   * 对于表/视图，它从关系缓存中删除其条目。
   *
   * 该方法应在以下情况下使用：
   *   1. 表/视图的逻辑计划已更改，并且显式清除了缓存的表/视图数据。
   *      例如，像 `AlterTableRenameCommand` 中那样重新缓存表本身。
   *      否则，如果需要刷新缓存数据，请考虑使用 `CatalogImpl.refreshTable()`。
   *   2. 表/视图不存在，只需要删除其在关系缓存中的条目，因为缓存数据像在 `DropTableCommand`
   *      中那样显式失效，它会取消缓存表/视图数据本身。
   *   3. 临时视图中使用的任何关系的元数据（例如文件索引）应该更新。
   */
  def refreshTable(name: TableIdentifier): Unit = synchronized {
    lookupTempView(name).map(_.refresh).getOrElse {
      val dbName = formatDatabaseName(name.database.getOrElse(currentDb))
      val tableName = formatTableName(name.table)
      val qualifiedTableName = QualifiedTableName(dbName, tableName)
      tableRelationCache.invalidate(qualifiedTableName)
    }
  }

  /**
   * 删除所有现有的临时视图。
   * 仅用于测试。
   */
  def clearTempTables(): Unit = synchronized {
    tempViews.clear()
  }

  // ----------------------------------------------------------------------------
  // 分区
  // ----------------------------------------------------------------------------
  // 此类别中的所有方法都直接与底层目录交互。
  // 这些方法仅涉及元数据存储表。
  // ----------------------------------------------------------------------------

  // TODO: 我们需要弄清楚这些方法如何与我们的数据源表交互。
  // 对于此类表，我们不在元数据存储中存储分区列的值。
  // 目前，当我们加载表时，将自动发现数据源表的分区值。

  /**
   * 在现有表中创建分区，假设它存在。
   * 如果未指定数据库，则假定该表在当前数据库中。
   */
  def createPartitions(
      tableName: TableIdentifier,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    requireExactMatchedPartitionSpec(parts.map(_.spec), getTableMetadata(tableName))
    requireNonEmptyValueInPartitionSpec(parts.map(_.spec))
    externalCatalog.createPartitions(
      db, table, partitionWithQualifiedPath(tableName, parts), ignoreIfExists)
  }


  /**
   * 从表中删除分区，假设它们存在。
   * 如果未指定数据库，则假定该表在当前数据库中。
   */
  def dropPartitions(
      tableName: TableIdentifier,
      specs: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean,
      purge: Boolean,
      retainData: Boolean): Unit = {
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    requirePartialMatchedPartitionSpec(specs, getTableMetadata(tableName))
    requireNonEmptyValueInPartitionSpec(specs)
    externalCatalog.dropPartitions(db, table, specs, ignoreIfNotExists, purge, retainData)
  }

  /**
   * 覆盖一个或多个现有表分区的规范，假设它们存在。
   *
   * 这假定 `specs` 的索引 i 对应于 `newSpecs` 的索引 i。
   * 如果未指定数据库，则假定该表在当前数据库中。
   */
  def renamePartitions(
      tableName: TableIdentifier,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = {
    val tableMetadata = getTableMetadata(tableName)
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    requireExactMatchedPartitionSpec(specs, tableMetadata)
    requireExactMatchedPartitionSpec(newSpecs, tableMetadata)
    requireNonEmptyValueInPartitionSpec(specs)
    requireNonEmptyValueInPartitionSpec(newSpecs)
    externalCatalog.renamePartitions(db, table, specs, newSpecs)
  }

  /**
   * 更改一个或多个表分区，其规范与 `parts` 中指定的规范匹配，假设分区存在。
   *
   * 如果未指定数据库，则假定该表在当前数据库中。
   *
   * 注意：如果底层实现不支持更改某个字段，这将变成无操作。
   */
  def alterPartitions(tableName: TableIdentifier, parts: Seq[CatalogTablePartition]): Unit = {
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    requireExactMatchedPartitionSpec(parts.map(_.spec), getTableMetadata(tableName))
    requireNonEmptyValueInPartitionSpec(parts.map(_.spec))
    externalCatalog.alterPartitions(db, table, partitionWithQualifiedPath(tableName, parts))
  }

  /**
   * 检索表分区的元数据，假设它存在。
   * 如果未指定数据库，则假定该表在当前数据库中。
   */
  def getPartition(tableName: TableIdentifier, spec: TablePartitionSpec): CatalogTablePartition = {
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    requireExactMatchedPartitionSpec(Seq(spec), getTableMetadata(tableName))
    requireNonEmptyValueInPartitionSpec(Seq(spec))
    externalCatalog.getPartition(db, table, spec)
  }

  /**
   * 列出属于指定表的所有分区的名称，假设该表存在。
   *
   * 可以选择性地提供部分分区规范以过滤返回的分区。
   * 例如，如果存在分区 (a='1', b='2')、(a='1', b='3') 和 (a='2', b='4')，
   * 那么部分规范 (a='1') 将仅返回前两个。
   */
  def listPartitionNames(
      tableName: TableIdentifier,
      partialSpec: Option[TablePartitionSpec] = None): Seq[String] = {
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    partialSpec.foreach { spec =>
      requirePartialMatchedPartitionSpec(Seq(spec), getTableMetadata(tableName))
      requireNonEmptyValueInPartitionSpec(Seq(spec))
    }
    externalCatalog.listPartitionNames(db, table, partialSpec)
  }

  /**
   * 列出属于指定表的所有分区的元数据，假设该表存在。
   * 可以选择性地提供部分分区规范以过滤返回的分区。
   * 例如，如果存在分区 (a='1', b='2')、(a='1', b='3') 和 (a='2', b='4')，
   * 那么部分规范 (a='1') 将仅返回前两个。
   */
  def listPartitions(
      tableName: TableIdentifier,
      partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = {
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    partialSpec.foreach { spec =>
      requirePartialMatchedPartitionSpec(Seq(spec), getTableMetadata(tableName))
      requireNonEmptyValueInPartitionSpec(Seq(spec))
    }
    externalCatalog.listPartitions(db, table, partialSpec)
  }

  /** 列出满足给定分区修剪谓词表达式的属于指定表的分区的元数据，假设该表存在。  */
  def listPartitionsByFilter(
      tableName: TableIdentifier,
      predicates: Seq[Expression]): Seq[CatalogTablePartition] = {
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    externalCatalog.listPartitionsByFilter(db, table, predicates, conf.sessionLocalTimeZone)
  }

  /** 验证输入分区规范是否有任何空值。 */
  private def requireNonEmptyValueInPartitionSpec(specs: Seq[TablePartitionSpec]): Unit = {
    specs.foreach { s =>
      if (s.values.exists(v => v != null && v.isEmpty)) {
        val spec = s.map(p => p._1 + "=" + p._2).mkString("[", ", ", "]")
        throw QueryCompilationErrors.invalidPartitionSpecError(
          s"The spec ($spec) contains an empty partition column value")
      }
    }
  }

  /** 验证输入分区规范是否与现有定义的分区规范完全匹配列必须相同，但顺序可以不同。
   */
  private def requireExactMatchedPartitionSpec(
      specs: Seq[TablePartitionSpec],
      table: CatalogTable): Unit = {
    specs.foreach { spec =>
      PartitioningUtils.requireExactMatchedPartitionSpec(
        table.identifier.toString,
        spec,
        table.partitionColumnNames)
    }
  }

  /** 验证输入分区规范是否部分匹配现有定义的分区规范 也就是说，分区规范的列应该是定义的分区规范的一部分。 */
  private def requirePartialMatchedPartitionSpec(
      specs: Seq[TablePartitionSpec],
      table: CatalogTable): Unit = {
    val defined = table.partitionColumnNames
    specs.foreach { s =>
      if (!s.keys.forall(defined.contains)) {
        throw QueryCompilationErrors.invalidPartitionSpecError(
          s"The spec (${s.keys.mkString(", ")}) must be contained " +
          s"within the partition spec (${table.partitionColumnNames.mkString(", ")}) defined " +
          s"in table '${table.identifier}'")
      }
    }
  }


  /**
   * 使分区路径成为限定路径。
   * 如果分区路径是相对的，例如 'paris'，它将使用表位置的父路径进行限定， 例如 'file:/warehouse/table/paris'
   */
  private def partitionWithQualifiedPath(
      tableIdentifier: TableIdentifier,
      parts: Seq[CatalogTablePartition]): Seq[CatalogTablePartition] = {
    lazy val tbl = getTableMetadata(tableIdentifier)
    parts.map { part =>
      if (part.storage.locationUri.isDefined && !part.storage.locationUri.get.isAbsolute) {
        val partPath = new Path(new Path(tbl.location), new Path(part.storage.locationUri.get))
        val qualifiedPartPath = makeQualifiedPath(CatalogUtils.stringToURI(partPath.toString))
        part.copy(storage = part.storage.copy(locationUri = Some(qualifiedPartPath)))
      } else part
    }
  }

  // ----------------------------------------------------------------------------
  // 函数
  // ----------------------------------------------------------------------------
  // 有两种函数：临时函数和元数据存储函数（永久 UDF）。
  // 临时函数在会话之间是隔离的。
  // 元数据存储函数可以跨多个会话使用，因为它们的元数据持久化在底层目录中。
  // ----------------------------------------------------------------------------

  // -------------------------------------------------------
  // |              仅与元数据存储函数交互的方法                |
  // -------------------------------------------------------

  /**
   * 在 `funcDefinition` 中指定的数据库中创建函数。
   * 如果未指定此类数据库，则在当前数据库中创建它。
   *
   * @param ignoreIfExists: 为 true 时，如果指定数据库中存在具有指定名称的函数，则忽略
   */
  def createFunction(funcDefinition: CatalogFunction, ignoreIfExists: Boolean): Unit = {
    val db = formatDatabaseName(funcDefinition.identifier.database.getOrElse(getCurrentDatabase))
    requireDbExists(db)
    val identifier = FunctionIdentifier(funcDefinition.identifier.funcName, Some(db))
    val newFuncDefinition = funcDefinition.copy(identifier = identifier)
    if (!functionExists(identifier)) {
      externalCatalog.createFunction(db, newFuncDefinition)
    } else if (!ignoreIfExists) {
      throw new FunctionAlreadyExistsException(db = db, func = identifier.toString)
    }
  }

  /**
   * 删除元数据存储函数。
   * 如果未指定数据库，则假定该函数在当前数据库中。
   */
  def dropFunction(name: FunctionIdentifier, ignoreIfNotExists: Boolean): Unit = {
    val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    requireDbExists(db)
    val identifier = name.copy(database = Some(db))
    if (functionExists(identifier)) {
      if (functionRegistry.functionExists(identifier)) {
        // 如果我们已将此函数加载到 FunctionRegistry 中， 也从那里删除它。
        // 对于永久函数，因为我们在首次使用时将其加载到 FunctionRegistry 中， 我们还需要从 FunctionRegistry 中删除它。
        functionRegistry.dropFunction(identifier)
      }
      externalCatalog.dropFunction(db, name.funcName)
    } else if (!ignoreIfNotExists) {
      throw new NoSuchPermanentFunctionException(db = db, func = identifier.toString)
    }
  }

  /**
   * 覆盖 `funcDefinition` 中指定的数据库中的元数据存储函数。
   * 如果未指定数据库，则假定该函数在当前数据库中。
   */
  def alterFunction(funcDefinition: CatalogFunction): Unit = {
    val db = formatDatabaseName(funcDefinition.identifier.database.getOrElse(getCurrentDatabase))
    requireDbExists(db)
    val identifier = FunctionIdentifier(funcDefinition.identifier.funcName, Some(db))
    val newFuncDefinition = funcDefinition.copy(identifier = identifier)
    if (functionExists(identifier)) {
      if (functionRegistry.functionExists(identifier)) {
        // 如果我们已将此函数加载到 FunctionRegistry 中， 也从那里删除它。
        // 对于永久函数，因为我们在首次使用时将其加载到 FunctionRegistry 中， 我们还需要从 FunctionRegistry 中删除它。
        functionRegistry.dropFunction(identifier)
      }
      externalCatalog.alterFunction(db, newFuncDefinition)
    } else {
      throw new NoSuchPermanentFunctionException(db = db, func = identifier.toString)
    }
  }


  /**
   * 检索元数据存储函数的元数据。
   * 如果在 `name` 中指定了数据库，这将返回该数据库中的函数。 如果未指定数据库，这将返回当前数据库中的函数。
   */
  def getFunctionMetadata(name: FunctionIdentifier): CatalogFunction = {
    val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    requireDbExists(db)
    externalCatalog.getFunction(db, name.funcName)
  }

  /** 检查具有指定名称的函数是否存在 */
  def functionExists(name: FunctionIdentifier): Boolean = {
    functionRegistry.functionExists(name) || tableFunctionRegistry.functionExists(name) || {
      val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
      requireDbExists(db)
      externalCatalog.functionExists(db, name.funcName)
    }
  }

  // ----------------------------------------------------------------
  // |         与临时函数和元数据存储函数交互的方法                       |
  // ----------------------------------------------------------------

  /**  根据提供的函数元数据构造一个 [[FunctionBuilder]]。 */
  private def makeFunctionBuilder(func: CatalogFunction): FunctionBuilder = {
    val className = func.className
    if (!Utils.classIsLoadable(className)) {
      throw QueryCompilationErrors.cannotLoadClassWhenRegisteringFunctionError(className, func.identifier)
    }
    val clazz = Utils.classForName(className)     // 加载对应的类
    val name = func.identifier.unquotedString
    (input: Seq[Expression]) => functionExpressionBuilder.makeExpression(name, clazz, input)
  }

  /** 为函数加载资源，例如 JAR 和文件。每个资源由一个元组（资源类型，资源 uri）表示。 */
  def loadFunctionResources(resources: Seq[FunctionResource]): Unit = {
    resources.foreach(functionResourceLoader.loadResource)
  }

  /** 将临时或永久标量函数注册到特定会话的 [[FunctionRegistry]] 中。 */
  def registerFunction(
      funcDefinition: CatalogFunction,
      overrideIfExists: Boolean,
      functionBuilder: Option[FunctionBuilder] = None): Unit = {
    val builder = functionBuilder.getOrElse(makeFunctionBuilder(funcDefinition))
    registerFunction(funcDefinition, overrideIfExists, functionRegistry, builder)
  }

  private def registerFunction[T](
      funcDefinition: CatalogFunction,
      overrideIfExists: Boolean,
      registry: FunctionRegistryBase[T],
      functionBuilder: FunctionRegistryBase[T]#FunctionBuilder): Unit = {
    val func = funcDefinition.identifier
    if (registry.functionExists(func) && !overrideIfExists) {
      throw QueryCompilationErrors.functionAlreadyExistsError(func)
    }
    val info = makeExprInfoForHiveFunction(funcDefinition)
    registry.registerFunction(func, info, functionBuilder)
  }

  private def makeExprInfoForHiveFunction(func: CatalogFunction): ExpressionInfo = {
    new ExpressionInfo(
      func.className,
      func.identifier.database.orNull,
      func.identifier.funcName,
      null,
      "",
      "",
      "",
      "",
      "",
      "",
      "hive")
  }

  /**
   * 从特定会话的 [[FunctionRegistry]] 中注销临时或永久函数
   * 如果函数存在则返回 true。
   */
  def unregisterFunction(name: FunctionIdentifier): Boolean = {
    functionRegistry.dropFunction(name)
  }

  /**
   * 删除临时函数。
   */
  def dropTempFunction(name: String, ignoreIfNotExists: Boolean): Unit = {
    if (!functionRegistry.dropFunction(FunctionIdentifier(name)) &&
        !tableFunctionRegistry.dropFunction(FunctionIdentifier(name)) &&
        !ignoreIfNotExists) {
      throw new NoSuchTempFunctionException(name)
    }
  }

  /**
   * 返回它是否为临时函数。如果不存在，则返回 false。
   */
  def isTemporaryFunction(name: FunctionIdentifier): Boolean = {
    // A temporary function is a function that has been registered in functionRegistry
    // without a database name, and is neither a built-in function nor a Hive function
    name.database.isEmpty && isRegisteredFunction(name) && !isBuiltinFunction(name)
  }


  /**
   * 返回此函数是否已在当前会话的函数注册表中注册。如果不存在，则返回 false。
   */
  def isRegisteredFunction(name: FunctionIdentifier): Boolean = {
    functionRegistry.functionExists(name) || tableFunctionRegistry.functionExists(name)
  }

  /**
   * 返回它是否为持久函数。如果不存在，则返回 false。
   */
  def isPersistentFunction(name: FunctionIdentifier): Boolean = {
    val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    databaseExists(db) && externalCatalog.functionExists(db, name.funcName)
  }

  def isBuiltinFunction(name: FunctionIdentifier): Boolean = {
    FunctionRegistry.builtin.functionExists(name) ||
      TableFunctionRegistry.builtin.functionExists(name)
  }

  /**
   * 返回它是否为内置函数。
   */
  protected[sql] def failFunctionLookup(
      name: FunctionIdentifier, cause: Option[Throwable] = None): Nothing = {
    throw new NoSuchFunctionException(
      db = name.database.getOrElse(getCurrentDatabase), func = name.funcName, cause)
  }

  /**
   * 如果是内置或临时函数，则按名称查找给定函数的 `ExpressionInfo`。
   * 这仅支持标量函数。
   */
  def lookupBuiltinOrTempFunction(name: String): Option[ExpressionInfo] = {
    FunctionRegistry.builtinOperators.get(name.toLowerCase(Locale.ROOT)).orElse {
      synchronized(lookupTempFuncWithViewContext(
        name, FunctionRegistry.builtin.functionExists, functionRegistry.lookupFunction))
    }
  }

  /**
   * 如果是内置或临时表函数，则按名称查找给定函数的 `ExpressionInfo`。
   */
  def lookupBuiltinOrTempTableFunction(name: String): Option[ExpressionInfo] = synchronized {
    lookupTempFuncWithViewContext(
      name, TableFunctionRegistry.builtin.functionExists, tableFunctionRegistry.lookupFunction)
  }


  /**
   * 按名称查找内置或临时标量函数，如果存在此类函数，则将其解析为 Expression。
   */
  def resolveBuiltinOrTempFunction(name: String, arguments: Seq[Expression]): Option[Expression] = {
    resolveBuiltinOrTempFunctionInternal(
      name, arguments, FunctionRegistry.builtin.functionExists, functionRegistry)
  }

  /**
   * 按名称查找内置或临时表函数，如果存在此类函数，则将其解析为 LogicalPlan。
   */
  def resolveBuiltinOrTempTableFunction(
      name: String, arguments: Seq[Expression]): Option[LogicalPlan] = {
    resolveBuiltinOrTempFunctionInternal(
      name, arguments, TableFunctionRegistry.builtin.functionExists, tableFunctionRegistry)
  }

  private def resolveBuiltinOrTempFunctionInternal[T](
      name: String,
      arguments: Seq[Expression],
      isBuiltin: FunctionIdentifier => Boolean,
      registry: FunctionRegistryBase[T]): Option[T] = synchronized {
    val funcIdent = FunctionIdentifier(name)
    if (!registry.functionExists(funcIdent)) {
      None
    } else {
      lookupTempFuncWithViewContext(
        name, isBuiltin, ident => Option(registry.lookupFunction(ident, arguments)))
    }
  }

  private def lookupTempFuncWithViewContext[T](
      name: String,
      isBuiltin: FunctionIdentifier => Boolean,
      lookupFunc: FunctionIdentifier => Option[T]): Option[T] = {
    val funcIdent = FunctionIdentifier(name)
    if (isBuiltin(funcIdent)) {
      lookupFunc(funcIdent)
    } else {
      val isResolvingView = AnalysisContext.get.catalogAndNamespace.nonEmpty
      val referredTempFunctionNames = AnalysisContext.get.referredTempFunctionNames
      if (isResolvingView) {
        // When resolving a view, only return a temp function if it's referred by this view.
        if (referredTempFunctionNames.contains(name)) {
          lookupFunc(funcIdent)
        } else {
          None
        }
      } else {
        val result = lookupFunc(funcIdent)
        if (result.isDefined) {
          // We are not resolving a view and the function is a temp one, add it to
          // `AnalysisContext`, so during the view creation, we can save all referred temp
          // functions to view metadata.
          AnalysisContext.get.referredTempFunctionNames.add(name)
        }
        result
      }
    }
  }

  /**
   * 如果是持久函数，则按名称查找给定函数的 `ExpressionInfo`。 这支持标量函数和表函数。
   */
  def lookupPersistentFunction(name: FunctionIdentifier): ExpressionInfo = {
    val database = name.database.orElse(Some(currentDb)).map(formatDatabaseName)
    val qualifiedName = name.copy(database = database)
    functionRegistry.lookupFunction(qualifiedName)
      .orElse(tableFunctionRegistry.lookupFunction(qualifiedName))
      .getOrElse {
        val db = qualifiedName.database.get
        requireDbExists(db)
        if (externalCatalog.functionExists(db, name.funcName)) {
          val metadata = externalCatalog.getFunction(db, name.funcName)
          makeExprInfoForHiveFunction(metadata.copy(identifier = qualifiedName))
        } else {
          failFunctionLookup(name)
        }
      }
  }

  /**
   * 按名称查找持久标量函数并将其解析为 Expression。
   */
  def resolvePersistentFunction(
      name: FunctionIdentifier, arguments: Seq[Expression]): Expression = {
    resolvePersistentFunctionInternal(name, arguments, functionRegistry, makeFunctionBuilder)
  }

  /**
   * 按名称查找持久表函数并将其解析为 LogicalPlan。
   */
  def resolvePersistentTableFunction(
      name: FunctionIdentifier,
      arguments: Seq[Expression]): LogicalPlan = {
    // We don't support persistent table functions yet.
    val builder = (func: CatalogFunction) => failFunctionLookup(name)
    resolvePersistentFunctionInternal(name, arguments, tableFunctionRegistry, builder)
  }

  private def resolvePersistentFunctionInternal[T](
      name: FunctionIdentifier,
      arguments: Seq[Expression],
      registry: FunctionRegistryBase[T],
      createFunctionBuilder: CatalogFunction => FunctionRegistryBase[T]#FunctionBuilder): T = {
    val database = formatDatabaseName(name.database.getOrElse(currentDb))
    val qualifiedName = name.copy(database = Some(database))
    if (registry.functionExists(qualifiedName)) {
      // This function has been already loaded into the function registry.
      registry.lookupFunction(qualifiedName, arguments)
    } else {
      // The function has not been loaded to the function registry, which means
      // that the function is a persistent function (if it actually has been registered
      // in the metastore). We need to first put the function in the function registry.
      val catalogFunction = try {
        externalCatalog.getFunction(database, qualifiedName.funcName)
      } catch {
        case _: AnalysisException => failFunctionLookup(qualifiedName)
      }
      loadFunctionResources(catalogFunction.resources)
      // Please note that qualifiedName is provided by the user. However,
      // catalogFunction.identifier.unquotedString is returned by the underlying
      // catalog. So, it is possible that qualifiedName is not exactly the same as
      // catalogFunction.identifier.unquotedString (difference is on case-sensitivity).
      // At here, we preserve the input from the user.
      val funcMetadata = catalogFunction.copy(identifier = qualifiedName)
      registerFunction(
        funcMetadata,
        overrideIfExists = false,
        registry = registry,
        functionBuilder = createFunctionBuilder(funcMetadata))
      // Now, we need to create the Expression.
      registry.lookupFunction(qualifiedName, arguments)
    }
  }

  /**
   * 查找与指定函数关联的 [[ExpressionInfo]]，假设它存在。
   */
  def lookupFunctionInfo(name: FunctionIdentifier): ExpressionInfo = synchronized {
    if (name.database.isEmpty) {
      lookupBuiltinOrTempFunction(name.funcName)
        .orElse(lookupBuiltinOrTempTableFunction(name.funcName))
        .getOrElse(lookupPersistentFunction(name))
    } else {
      lookupPersistentFunction(name)
    }
  }
  // 实际的函数查找逻辑首先查找临时/内置函数，然后从 v1 或 v2 目录查找持久函数。
  // 此方法仅查找 v1 目录。
  def lookupFunction(name: FunctionIdentifier, children: Seq[Expression]): Expression = {
    if (name.database.isEmpty) {
      resolveBuiltinOrTempFunction(name.funcName, children)
        .getOrElse(resolvePersistentFunction(name, children))
    } else {
      resolvePersistentFunction(name, children)
    }
  }

  def lookupTableFunction(name: FunctionIdentifier, children: Seq[Expression]): LogicalPlan = {
    if (name.database.isEmpty) {
      resolveBuiltinOrTempTableFunction(name.funcName, children)
        .getOrElse(resolvePersistentTableFunction(name, children))
    } else {
      resolvePersistentTableFunction(name, children)
    }
  }

  /**
   * 列出数据库中具有给定模式的所有已注册函数。
   */
  private def listRegisteredFunctions(db: String, pattern: String): Seq[FunctionIdentifier] = {
    val functions = (functionRegistry.listFunction() ++ tableFunctionRegistry.listFunction())
      .filter(_.database.forall(_ == db))
    StringUtils.filterPattern(functions.map(_.unquotedString), pattern).map { f =>
      // In functionRegistry, function names are stored as an unquoted format.
      Try(parser.parseFunctionIdentifier(f)) match {
        case Success(e) => e
        case Failure(_) =>
          // The names of some built-in functions are not parsable by our parser, e.g., %
          FunctionIdentifier(f)
      }
    }
  }

  /**
   * 列出指定数据库中的所有函数，包括临时函数。
   * 这返回函数标识符及其定义的范围（系统或用户定义）。
   */
  def listFunctions(db: String): Seq[(FunctionIdentifier, String)] = listFunctions(db, "*")

  /**
   * 列出指定数据库中所有匹配的函数，包括临时函数。
   * 这返回函数标识符及其定义的范围（系统或用户定义）。
   */
  def listFunctions(db: String, pattern: String): Seq[(FunctionIdentifier, String)] = {
    val dbName = formatDatabaseName(db)
    requireDbExists(dbName)
    val dbFunctions = externalCatalog.listFunctions(dbName, pattern).map { f =>
      FunctionIdentifier(f, Some(dbName)) }
    val loadedFunctions = listRegisteredFunctions(db, pattern)
    val functions = dbFunctions ++ loadedFunctions
    // The session catalog caches some persistent functions in the FunctionRegistry
    // so there can be duplicates.
    functions.map {
      case f if FunctionRegistry.functionSet.contains(f) => (f, "SYSTEM")
      case f if TableFunctionRegistry.functionSet.contains(f) => (f, "SYSTEM")
      case f => (f, "USER")
    }.distinct
  }


  // -----------------
  // |   其他方法      |
  // -----------------

  /**
   * 删除所有现有数据库（"default" 除外）、表、分区和函数， 并将当前数据库设置为 "default"。
   *
   * 这主要用于测试。
   */
  def reset(): Unit = synchronized {
    setCurrentDatabase(DEFAULT_DATABASE)
    externalCatalog.setCurrentDatabase(DEFAULT_DATABASE)
    listDatabases().filter(_ != DEFAULT_DATABASE).foreach { db =>
      dropDatabase(db, ignoreIfNotExists = false, cascade = true)
    }
    listTables(DEFAULT_DATABASE).foreach { table =>
      dropTable(table, ignoreIfNotExists = false, purge = false)
    }
    listFunctions(DEFAULT_DATABASE).map(_._1).foreach { func =>
      if (func.database.isDefined) {
        dropFunction(func, ignoreIfNotExists = false)
      } else {
        dropTempFunction(func.funcName, ignoreIfNotExists = false)
      }
    }
    clearTempTables()
    globalTempViewManager.clear()
    functionRegistry.clear()
    tableFunctionRegistry.clear()
    tableRelationCache.invalidateAll()
    // restore built-in functions
    FunctionRegistry.builtin.listFunction().foreach { f =>
      val expressionInfo = FunctionRegistry.builtin.lookupFunction(f)
      val functionBuilder = FunctionRegistry.builtin.lookupFunctionBuilder(f)
      require(expressionInfo.isDefined, s"built-in function '$f' is missing expression info")
      require(functionBuilder.isDefined, s"built-in function '$f' is missing function builder")
      functionRegistry.registerFunction(f, expressionInfo.get, functionBuilder.get)
    }
    // restore built-in table functions
    TableFunctionRegistry.builtin.listFunction().foreach { f =>
      val expressionInfo = TableFunctionRegistry.builtin.lookupFunction(f)
      val functionBuilder = TableFunctionRegistry.builtin.lookupFunctionBuilder(f)
      require(expressionInfo.isDefined, s"built-in function '$f' is missing expression info")
      require(functionBuilder.isDefined, s"built-in function '$f' is missing function builder")
      tableFunctionRegistry.registerFunction(f, expressionInfo.get, functionBuilder.get)
    }
  }


  /**
   * 将目录的当前状态复制到另一个目录。
   *
   * 此函数在此 [[SessionCatalog]]（源）上同步，以确保复制的状态是一致的。
   * 目标 [[SessionCatalog]] 不同步，也不应该同步，因为此时不应发布目标 [[SessionCatalog]]。
   * 如果此假设不成立，调用者必须在目标上同步。
   */
  private[sql] def copyStateTo(target: SessionCatalog): Unit = synchronized {
    target.currentDb = currentDb
    // copy over temporary views
    tempViews.foreach(kv => target.tempViews.put(kv._1, kv._2))
  }

  /**
   * 在重命名托管表之前验证新位置，该位置应该不存在。
   */
  private def validateNewLocationOfRename(
      oldName: TableIdentifier,
      newName: TableIdentifier): Unit = {
    requireTableExists(oldName)
    requireTableNotExists(newName)
    val oldTable = getTableMetadata(oldName)
    if (oldTable.tableType == CatalogTableType.MANAGED) {
      assert(oldName.database.nonEmpty)
      val databaseLocation =
        externalCatalog.getDatabase(oldName.database.get).locationUri
      val newTableLocation = new Path(new Path(databaseLocation), formatTableName(newName.table))
      val fs = newTableLocation.getFileSystem(hadoopConf)
      if (fs.exists(newTableLocation)) {
        throw QueryCompilationErrors.cannotOperateManagedTableWithExistingLocationError(
          "rename", oldName, newTableLocation)
      }
    }
  }
}
