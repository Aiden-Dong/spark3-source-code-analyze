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

package org.apache.spark.sql.execution

import scala.collection.immutable.IndexedSeq

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, SubqueryExpression}
import org.apache.spark.sql.catalyst.optimizer.EliminateResolvedHint
import org.apache.spark.sql.catalyst.plans.logical.{IgnoreCachedData, LogicalPlan, ResolvedHint}
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_EXPRESSION
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.command.CommandUtils
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, FileTable}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

/** 保存缓存的逻辑计划及其数据 */
case class CachedData(plan: LogicalPlan, cachedRepresentation: InMemoryRelation)

/**
 * 在 SQLContext 中提供缓存查询结果的支持，并在执行后续查询时自动使用这些缓存结果。
 * 数据使用存储在 InMemoryRelation 中的字节缓冲区进行缓存。 此关系会自动替换返回与原始缓存查询 `sameResult` 的查询计划。
 *
 * Spark SQL 内部使用。
 */
class CacheManager extends Logging with AdaptiveSparkPlanHelper {

  /**
   * 将缓存计划列表维护为不可变序列。
   * 对列表的任何更新 都应在 "this.synchronized" 块中受到保护，该块包括读取
   * 现有值和更新 cachedData 变量。
   */
  @transient @volatile
  private var cachedData = IndexedSeq[CachedData]()

  /**
   * 需要关闭的配置，以避免缓存查询的回归，
   * 以便稍后可以利用 底层缓存查询计划的 outputPartitioning。
   * 配置包括：
   * 1. AQE
   * 2. 自动分桶表扫描
   */
  private val forceDisableConfigs: Seq[ConfigEntry[Boolean]] = Seq(
    SQLConf.ADAPTIVE_EXECUTION_ENABLED,
    SQLConf.AUTO_BUCKETED_SCAN_ENABLED)

  /** 清除所有缓存的表。 */
  def clearCache(): Unit = this.synchronized {
    cachedData.foreach(_.cachedRepresentation.cacheBuilder.clearCache())
    cachedData = IndexedSeq[CachedData]()
  }

  /** 检查缓存是否为空。 */
  def isEmpty: Boolean = {
    cachedData.isEmpty
  }

  /**
   * 缓存给定 [[Dataset]] 的逻辑表示产生的数据。
   * 与 `RDD.cache()` 不同，默认存储级别设置为 `MEMORY_AND_DISK`，因为重新计算底层表的内存列式表示的成本很高。
   */
  def cacheQuery(
      query: Dataset[_],
      tableName: Option[String] = None,
      storageLevel: StorageLevel = MEMORY_AND_DISK): Unit = {
    cacheQuery(query.sparkSession, query.logicalPlan, tableName, storageLevel)
  }

  /**
   * 缓存给定 [[LogicalPlan]] 产生的数据。
   * 与 `RDD.cache()` 不同，默认存储级别设置为 `MEMORY_AND_DISK`，因为
   * 重新计算底层表的内存列式表示的成本很高。
   */
  def cacheQuery(
      spark: SparkSession,
      planToCache: LogicalPlan,
      tableName: Option[String]): Unit = {
    cacheQuery(spark, planToCache, tableName, MEMORY_AND_DISK)
  }

  /**
   * 缓存给定 [[LogicalPlan]] 产生的数据。
   */
  def cacheQuery(
      spark: SparkSession,              // Spark 会话空间
      planToCache: LogicalPlan,         // 要缓存的 LogicalPlan
      tableName: Option[String],        // 当前缓存数据命名
      storageLevel: StorageLevel): Unit = {  // 要缓存的级别
    if (lookupCachedData(planToCache).nonEmpty) {
      logWarning("Asked to cache already cached data.")
    } else {
      // 设置缓存LogicalPlan 的指定 SparkSession(spark.clone(conf))
      val sessionWithConfigsOff = getOrCloneSessionWithConfigsOff(spark)

      // 创建 InmemoryRelation
      val inMemoryRelation = sessionWithConfigsOff.withActive {
        val qe = sessionWithConfigsOff.sessionState.executePlan(planToCache)
        InMemoryRelation(
          storageLevel,
          qe,
          tableName)
      }

      this.synchronized {
        if (lookupCachedData(planToCache).nonEmpty) {
          logWarning("Data has already been cached.")
        } else {
          // 将当前的InmemoryRelation 添加到缓存管理
          cachedData = CachedData(planToCache, inMemoryRelation) +: cachedData
        }
      }
    }
  }

  /**
   * 取消缓存给定计划或引用给定计划的所有缓存条目。
   * @param query     要取消缓存的 [[Dataset]]。
   * @param cascade   如果为 true，取消缓存引用给定 [[Dataset]] 的所有缓存条目；否则仅取消缓存给定的 [[Dataset]]。
   */
  def uncacheQuery(
      query: Dataset[_],
      cascade: Boolean): Unit = {
    uncacheQuery(query.sparkSession, query.logicalPlan, cascade)
  }

  /**
   * 取消缓存给定计划或引用给定计划的所有缓存条目。
   * @param spark     Spark 会话。
   * @param plan      要取消缓存的计划。
   * @param cascade   如果为 true，取消缓存引用给定计划的所有缓存条目；否则仅取消缓存给定的计划。
   * @param blocking  是否阻塞直到所有块被删除。
   */
  def uncacheQuery(
      spark: SparkSession,
      plan: LogicalPlan,
      cascade: Boolean,
      blocking: Boolean = false): Unit = {
    val shouldRemove: LogicalPlan => Boolean =
      if (cascade) {
        _.exists(_.sameResult(plan))
      } else {
        _.sameResult(plan)
      }
    val plansToUncache = cachedData.filter(cd => shouldRemove(cd.plan))
    this.synchronized {
      cachedData = cachedData.filterNot(cd => plansToUncache.exists(_ eq cd))
    }
    plansToUncache.foreach { _.cachedRepresentation.cacheBuilder.clearCache(blocking) }

    // 删除缓存查询后重新编译依赖的缓存查询。
    if (!cascade) {
      recacheByCondition(spark, cd => {
        // 如果缓存缓冲区已经加载，我们不需要重新编译缓存计划，
        // 因为它不再依赖已取消缓存的计划，它只会从缓存缓冲区生成数据。
        // 请注意，`CachedRDDBuilder.isCachedColumnBuffersLoaded` 调用是非阻塞的状态测试，可能不会返回最准确的缓存缓冲区状态。因此最坏的情况可能是：
        // 1) 缓冲区已加载，但 `isCachedColumnBuffersLoaded` 返回 false，那么我们
        //    将清除缓冲区并重新编译计划。这是低效的，但不影响正确性。
        // 2) 缓冲区已清除，但 `isCachedColumnBuffersLoaded` 返回 true，那么我们
        //    将保持原样。这意味着物理计划已经在其他线程中重新编译了。
        val cacheAlreadyLoaded = cd.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded
        cd.plan.exists(_.sameResult(plan)) && !cacheAlreadyLoaded
      })
    }
  }

  // 分析给定缓存数据中的列统计信息
  private[sql] def analyzeColumnCacheQuery(
      sparkSession: SparkSession,
      cachedData: CachedData,
      column: Seq[Attribute]): Unit = {
    val relation = cachedData.cachedRepresentation
    val (rowCount, newColStats) =
      CommandUtils.computeColumnStats(sparkSession, relation, column)
    relation.updateStats(rowCount, newColStats)
  }

  /**
   * 尝试重新缓存引用给定计划的所有缓存条目。
   */
  def recacheByPlan(spark: SparkSession, plan: LogicalPlan): Unit = {
    recacheByCondition(spark, _.plan.exists(_.sameResult(plan)))
  }

  /**
   *  重新缓存满足给定 `condition` 的所有缓存条目。
   */
  private def recacheByCondition(
      spark: SparkSession,
      condition: CachedData => Boolean): Unit = {
    val needToRecache = cachedData.filter(condition)

    this.synchronized {
      // 先把要重新缓存的LogicalPlan 踢出去。
      cachedData = cachedData.filterNot(cd => needToRecache.exists(_ eq cd))
    }
    needToRecache.foreach { cd =>
      // 清理当前的缓存
      cd.cachedRepresentation.cacheBuilder.clearCache()
      val sessionWithConfigsOff = getOrCloneSessionWithConfigsOff(spark)
      // 重新构建 InMemoryRelation
      val newCache = sessionWithConfigsOff.withActive {
        val qe = sessionWithConfigsOff.sessionState.executePlan(cd.plan)
        InMemoryRelation(cd.cachedRepresentation.cacheBuilder, qe)
      }
      val recomputedPlan = cd.copy(cachedRepresentation = newCache)
      this.synchronized {
        if (lookupCachedData(recomputedPlan.plan).nonEmpty) {
          logWarning("While recaching, data was already added to cache.")
        } else {
          cachedData = recomputedPlan +: cachedData
        }
      }
    }
  }

  /** 可选地返回给定 [[Dataset]] 的缓存数据 */
  def lookupCachedData(query: Dataset[_]): Option[CachedData] = {
    lookupCachedData(query.logicalPlan)
  }

  /** 可选地返回给定 [[LogicalPlan]] 的缓存数据。 */
  def lookupCachedData(plan: LogicalPlan): Option[CachedData] = {
    cachedData.find(cd => plan.sameResult(cd.plan))
  }

  /** 在可能的情况下，将给定逻辑计划的部分替换为缓存版本。. */
  def useCachedData(plan: LogicalPlan): LogicalPlan = {
    val newPlan = plan transformDown {
      case command: IgnoreCachedData => command

      case currentFragment =>
        lookupCachedData(currentFragment).map { cached =>
          // After cache lookup, we should still keep the hints from the input plan.
          val hints = EliminateResolvedHint.extractHintsFromPlan(currentFragment)._2
          val cachedPlan = cached.cachedRepresentation.withOutput(currentFragment.output)
          // The returned hint list is in top-down order, we should create the hint nodes from
          // right to left.
          hints.foldRight[LogicalPlan](cachedPlan) { case (hint, p) =>
            ResolvedHint(p, hint)
          }
        }.getOrElse(currentFragment)
    }

    newPlan.transformAllExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
      case s: SubqueryExpression => s.withNewPlan(useCachedData(s.plan))
    }
  }

  /**
   * 尝试重新缓存在其逻辑计划中包含一个或多个 `HadoopFsRelation` 节点中的 `resourcePath` 的所有缓存条目。
   */
  def recacheByPath(spark: SparkSession, resourcePath: String): Unit = {
    val path = new Path(resourcePath)
    val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
    recacheByPath(spark, path, fs)
  }

  /**
   * 尝试重新缓存在其逻辑计划中包含一个或多个 `HadoopFsRelation` 节点中的 `resourcePath` 的所有缓存条目。
   */
  def recacheByPath(spark: SparkSession, resourcePath: Path, fs: FileSystem): Unit = {
    val qualifiedPath = fs.makeQualified(resourcePath)
    recacheByCondition(spark, _.plan.exists(lookupAndRefresh(_, fs, qualifiedPath)))
  }

  /**
   * 遍历给定的 `plan` 并在计划中任何 [[HadoopFsRelation]] 节点的[[org.apache.spark.sql.execution.datasources.FileIndex]] 中搜索 `qualifiedPath` 的出现。
   * 如果找到，我们刷新元数据并返回 true。否则，此方法返回 false。
   */
  private def lookupAndRefresh(plan: LogicalPlan, fs: FileSystem, qualifiedPath: Path): Boolean = {
    plan match {
      case lr: LogicalRelation => lr.relation match {
        case hr: HadoopFsRelation =>
          refreshFileIndexIfNecessary(hr.location, fs, qualifiedPath)
        case _ => false
      }

      case DataSourceV2Relation(fileTable: FileTable, _, _, _, _) =>
        refreshFileIndexIfNecessary(fileTable.fileIndex, fs, qualifiedPath)

      case _ => false
    }
  }

  /**
   * 如果给定 [[FileIndex]] 的任何根路径以 `qualifiedPath` 开头，则刷新它。
   * @return [[FileIndex]] 是否被刷新。
   */
  private def refreshFileIndexIfNecessary(
      fileIndex: FileIndex,
      fs: FileSystem,
      qualifiedPath: Path): Boolean = {
    val prefixToInvalidate = qualifiedPath.toString
    val needToRefresh = fileIndex.rootPaths
      .map(_.makeQualified(fs.getUri, fs.getWorkingDirectory).toString)
      .exists(_.startsWith(prefixToInvalidate))
    if (needToRefresh) fileIndex.refresh()
    needToRefresh
  }

  /**
   * 如果启用了 CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING，则只返回原始会话。
   */
  private def getOrCloneSessionWithConfigsOff(session: SparkSession): SparkSession = {
    if (session.sessionState.conf.getConf(SQLConf.CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING)) {
      session
    } else {
      SparkSession.getOrCloneSessionWithConfigsOff(session, forceDisableConfigs)
    }
  }
}
