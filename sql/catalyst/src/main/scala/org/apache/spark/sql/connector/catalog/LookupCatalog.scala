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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

/**
 * A trait to encapsulate catalog lookup function and helpful extractors.
 */
private[sql] trait LookupCatalog extends Logging {

  protected val catalogManager: CatalogManager

  /**
   * Returns the current catalog set.
   */
  def currentCatalog: CatalogPlugin = catalogManager.currentCatalog

  /**
   * Extract catalog plugin and remaining identifier names.
   *
   * This does not substitute the default catalog if no catalog is set in the identifier.
   */
  private object CatalogAndMultipartIdentifier {
    def unapply(parts: Seq[String]): Some[(Option[CatalogPlugin], Seq[String])] = parts match {
      case Seq(_) =>
        Some((None, parts))
      case Seq(catalogName, tail @ _*) =>
        try {
          Some((Some(catalogManager.catalog(catalogName)), tail))
        } catch {
          case _: CatalogNotFoundException =>
            Some((None, parts))
        }
    }
  }

  /**
   * Extract session catalog and identifier from a multi-part identifier.
   */
  object SessionCatalogAndIdentifier {

    def unapply(parts: Seq[String]): Option[(CatalogPlugin, Identifier)] = parts match {
      case CatalogAndIdentifier(catalog, ident) if CatalogV2Util.isSessionCatalog(catalog) =>
        Some(catalog, ident)
      case _ => None
    }
  }

  /**
   * Extract non-session catalog and identifier from a multi-part identifier.
   */
  object NonSessionCatalogAndIdentifier {
    def unapply(parts: Seq[String]): Option[(CatalogPlugin, Identifier)] = parts match {
      case CatalogAndIdentifier(catalog, ident) if !CatalogV2Util.isSessionCatalog(catalog) =>
        Some(catalog, ident)
      case _ => None
    }
  }

  /**
   * Extract catalog and namespace from a multi-part name with the current catalog if needed.
   * Catalog name takes precedence over namespaces.
   */
  object CatalogAndNamespace {
    def unapply(nameParts: Seq[String]): Some[(CatalogPlugin, Seq[String])] = {
      assert(nameParts.nonEmpty)
      try {
        Some((catalogManager.catalog(nameParts.head), nameParts.tail))
      } catch {
        case _: CatalogNotFoundException =>
          Some((currentCatalog, nameParts))
      }
    }
  }

  /**
   * 从多部分名称中提取catalog和标识符（必要时使用当前catalog). catalog名称优先于identifier，但对于单部分名称，标识符优先于catalog名称。
   * 注意：此模式用于查找永久catalog 对象（如table、view、function等）。
   * 如果需要查找临时对象（如临时视图），请在调用此模式之前单独处理，因为临时对象不属于任何catalog。
   */
  object CatalogAndIdentifier {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper

    private val globalTempDB = SQLConf.get.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE)

    def unapply(nameParts: Seq[String]): Option[(CatalogPlugin, Identifier)] = {
      assert(nameParts.nonEmpty)
      if (nameParts.length == 1) {
        Some((currentCatalog, Identifier.of(catalogManager.currentNamespace, nameParts.head)))
      } else if (nameParts.head.equalsIgnoreCase(globalTempDB)) {

        // 从概念上讲，全局临时视图属于一个特殊保留catalog。但由于v2 catalog API暂不支持 view，我们仍需使用v1命令处理全局临时视图。
        // 为简化实现，现将全局临时视图置于会话catalog的特殊namespace中。该特殊namespace在名称解析时具有更高优先级。
        // 例如：若自定义catalog名称与`GLOBAL_TEMP_DATABASE`相同，则无法访问该自定义catalog。
        Some((catalogManager.v2SessionCatalog, nameParts.asIdentifier))
      } else {
        try {
          // 基于 CatalogManager 获取catalog 实体
          // spark_catalog -> catalogManager.v2SessionCatalog
          Some((catalogManager.catalog(nameParts.head), nameParts.tail.asIdentifier))
        } catch {
          case _: CatalogNotFoundException =>
            Some((currentCatalog, nameParts.asIdentifier))
        }
      }
    }
  }

  /**
   * Extract legacy table identifier from a multi-part identifier.
   *
   * For legacy support only. Please use [[CatalogAndIdentifier]] instead on DSv2 code paths.
   */
  object AsTableIdentifier {
    def unapply(parts: Seq[String]): Option[TableIdentifier] = {
      def namesToTableIdentifier(names: Seq[String]): Option[TableIdentifier] = names match {
        case Seq(name) => Some(TableIdentifier(name))
        case Seq(database, name) => Some(TableIdentifier(name, Some(database)))
        case _ => None
      }
      parts match {
        case CatalogAndMultipartIdentifier(None, names)
          if CatalogV2Util.isSessionCatalog(currentCatalog) =>
          namesToTableIdentifier(names)
        case CatalogAndMultipartIdentifier(Some(catalog), names)
          if CatalogV2Util.isSessionCatalog(catalog) &&
             CatalogV2Util.isSessionCatalog(currentCatalog) =>
          namesToTableIdentifier(names)
        case _ => None
      }
    }
  }
}
