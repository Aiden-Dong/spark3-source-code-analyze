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

import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable

import org.apache.spark.sql.catalyst.analysis.TempTableAlreadyExistsException
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.errors.QueryCompilationErrors



/**
 * 一个线程安全的全局临时视图管理器，提供原子操作来管理它们， 例如创建、更新、删除等。
 * 注意，视图名称在这里始终是大小写敏感的，调用者需要根据大小写敏感配置来格式化视图名称。
 *
 * @param database 系统保留的虚拟数据库，用于保存所有全局临时视图。 spark.sql.globalTempDatabase : global_temp
 */
class GlobalTempViewManager(val database: String) {

  /** 视图定义列表，从视图名称映射到逻辑计划。 */
  @GuardedBy("this")
  private val viewDefinitions = new mutable.HashMap[String, TemporaryViewRelation]

  /**
   * 返回与给定名称匹配的全局视图定义，如果未找到则返回 None。
   */
  def get(name: String): Option[TemporaryViewRelation] = synchronized {
    viewDefinitions.get(name)
  }

  /**
   * 创建一个全局临时视图，如果视图已存在且 `overrideIfExists` 为 false， 则抛出异常。
   */
  def create(
      name: String,
      viewDefinition: TemporaryViewRelation,
      overrideIfExists: Boolean): Unit = synchronized {
    if (!overrideIfExists && viewDefinitions.contains(name)) {
      throw new TempTableAlreadyExistsException(name)
    }
    viewDefinitions.put(name, viewDefinition)
  }

  /**
   * 如果全局临时视图存在则更新它，更新成功返回 true，否则返回 false。
   */
  def update(
      name: String,
      viewDefinition: TemporaryViewRelation): Boolean = synchronized {
    if (viewDefinitions.contains(name)) {
      viewDefinitions.put(name, viewDefinition)
      true
    } else {
      false
    }
  }

  /**
   * 如果全局临时视图存在则删除它，删除成功返回 true，否则返回 false。
   */
  def remove(name: String): Boolean = synchronized {
    viewDefinitions.remove(name).isDefined
  }

  /**
   * 如果源视图存在且目标视图不存在，则重命名全局临时视图；
   * 如果源视图存在但目标视图已存在，则抛出异常。
   * 重命名成功返回 true，否则返回 false。
   */
  def rename(oldName: String, newName: String): Boolean = synchronized {
    if (viewDefinitions.contains(oldName)) {
      if (viewDefinitions.contains(newName)) {
        throw QueryCompilationErrors.renameTempViewToExistingViewError(oldName, newName)
      }

      val viewDefinition = viewDefinitions(oldName)
      viewDefinitions.remove(oldName)
      viewDefinitions.put(newName, viewDefinition)
      true
    } else {
      false
    }
  }

  /**
   * 列出所有全局临时视图的名称。
   */
  def listViewNames(pattern: String): Seq[String] = synchronized {
    StringUtils.filterPattern(viewDefinitions.keys.toSeq, pattern)
  }

  /**
   * 清除所有全局临时视图。
   */
  def clear(): Unit = synchronized {
    viewDefinitions.clear()
  }
}
