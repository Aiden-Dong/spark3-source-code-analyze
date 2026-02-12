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

package org.apache.spark.sql.connector.catalog

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * 一个线程安全的 [[CatalogPlugin]] 管理器。它跟踪所有已注册的catalog，并允许调用方按名称查找catalog。
 * 目前仍有许多命令（例如 ANALYZE TABLE）不支持 v2 Catalog API。这些命令会忽略当前 [[Catalog]]， 直接访问 v1 的 [[SessionCatalog]]。
 * 为了避免同时在 [[SessionCatalog]] 和 [[CatalogManager]] 中 跟踪当前命名空间，我们让 [[CatalogManager]] 在当前目录是会话目录时，负责设置/获取
 * SessionCatalog 的当前数据库。
 */
// TODO: all commands should look up table from the current catalog. The `SessionCatalog` doesn't
//       need to track current database at all.
private[sql]
class CatalogManager(defaultSessionCatalog: CatalogPlugin,
           val v1SessionCatalog: SessionCatalog
   ) extends SQLConfHelper with Logging {

  import CatalogManager.SESSION_CATALOG_NAME      // spark_catalog
  import CatalogV2Util._

  private val catalogs = mutable.HashMap.empty[String, CatalogPlugin]

  def catalog(name: String): CatalogPlugin = synchronized {
    if (name.equalsIgnoreCase(SESSION_CATALOG_NAME)) {
      v2SessionCatalog
    } else {
      catalogs.getOrElseUpdate(name, Catalogs.load(name, conf))
    }
  }

  def isCatalogRegistered(name: String): Boolean = {
    try {
      catalog(name)
      true
    } catch {
      case _: CatalogNotFoundException => false
    }
  }

  private def loadV2SessionCatalog(): CatalogPlugin = {
    // 缺人用户有没有自定义 spark_catalog : spark.sql.catalog.spark_catalog
    Catalogs.load(SESSION_CATALOG_NAME, conf) match {
      case extension: CatalogExtension =>
        extension.setDelegateCatalog(defaultSessionCatalog)
        extension
      case other => other
    }
  }

  /**
   * 如果指定了 V2_SESSION_CATALOG 配置，我们会尝试实例化用户指定的 V2 Session Catalog, 否则返回默认 Session Catalog。
   * 该catalog是一个委托给 v1 session catalog 的 v2 catalog。
   *
   * 当session catalog 负责某个identifier，但数据源要求使用 v2 catalog API 时就会使用它。
   * 这种情况发生在数据源实现扩展了 v2 TableProvider API 且未被列入回退配置（spark.sql.sources.useV1SourceList）时。
   */
  private[sql] def v2SessionCatalog: CatalogPlugin = {
    conf.getConf(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION).map { _ =>
      catalogs.getOrElseUpdate(SESSION_CATALOG_NAME, loadV2SessionCatalog())
    }.getOrElse(defaultSessionCatalog)
  }

  private var _currentNamespace: Option[Array[String]] = None

  def currentNamespace: Array[String] = {
    val defaultNamespace = if (currentCatalog.name() == SESSION_CATALOG_NAME) {
      Array(v1SessionCatalog.getCurrentDatabase)
    } else {
      currentCatalog.defaultNamespace()
    }

    this.synchronized {
      _currentNamespace.getOrElse {
        defaultNamespace
      }
    }
  }

  def setCurrentNamespace(namespace: Array[String]): Unit = synchronized {
    currentCatalog match {
      case _ if isSessionCatalog(currentCatalog) && namespace.length == 1 => v1SessionCatalog.setCurrentDatabase(namespace.head)
      case _ if isSessionCatalog(currentCatalog) =>
        throw QueryCompilationErrors.noSuchNamespaceError(namespace)
      case catalog: SupportsNamespaces if !catalog.namespaceExists(namespace) =>
        throw QueryCompilationErrors.noSuchNamespaceError(namespace)
      case _ =>
        _currentNamespace = Some(namespace)
    }
  }

  private var _currentCatalogName: Option[String] = None

  def currentCatalog: CatalogPlugin = synchronized {
    catalog(_currentCatalogName.getOrElse(conf.getConf(SQLConf.DEFAULT_CATALOG)))
  }

  def setCurrentCatalog(catalogName: String): Unit = synchronized {
    // `setCurrentCatalog` is noop if it doesn't switch to a different catalog.
    if (currentCatalog.name() != catalogName) {
      catalog(catalogName)
      _currentCatalogName = Some(catalogName)
      _currentNamespace = None
      // Reset the current database of v1 `SessionCatalog` when switching current catalog, so that
      // when we switch back to session catalog, the current namespace definitely is ["default"].
      v1SessionCatalog.setCurrentDatabase(SessionCatalog.DEFAULT_DATABASE)
    }
  }

  def listCatalogs(pattern: Option[String]): Seq[String] = {
    val allCatalogs = synchronized(catalogs.keys.toSeq).sorted
    pattern.map(StringUtils.filterPattern(allCatalogs, _)).getOrElse(allCatalogs)
  }

  // Clear all the registered catalogs. Only used in tests.
  private[sql] def reset(): Unit = synchronized {
    catalogs.clear()
    _currentNamespace = None
    _currentCatalogName = None
    v1SessionCatalog.setCurrentDatabase(SessionCatalog.DEFAULT_DATABASE)
  }
}

private[sql] object CatalogManager {
  val SESSION_CATALOG_NAME: String = "spark_catalog"
}
