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

package org.apache.spark.sql.execution.columnar

import org.apache.commons.lang3.StringUtils

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{logical, QueryPlan}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.columnar.{CachedBatch, CachedBatchSerializer, SimpleMetricsCachedBatch, SimpleMetricsCachedBatchSerializer}
import org.apache.spark.sql.execution.{ColumnarToRowTransition, InputAdapter, QueryExecution, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.{BooleanType, ByteType, DoubleType, FloatType, IntegerType, LongType, ShortType, StructType, UserDefinedType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.util.{LongAccumulator, Utils}

/**
 * The default implementation of CachedBatch.
 *
 * @param numRows The total number of rows in this batch
 * @param buffers The buffers for serialized columns
 * @param stats The stat of columns
 */
case class DefaultCachedBatch(numRows: Int, buffers: Array[Array[Byte]], stats: InternalRow)
  extends SimpleMetricsCachedBatch

/**
 * The default implementation of CachedBatchSerializer.
 */
class DefaultCachedBatchSerializer extends SimpleMetricsCachedBatchSerializer {
  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = false

  override def convertColumnarBatchToCachedBatch(
      input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] =
    throw new IllegalStateException("Columnar input is not supported")

  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    val batchSize = conf.columnBatchSize
    val useCompression = conf.useCompression
    convertForCacheInternal(input, schema, batchSize, useCompression)
  }

  def convertForCacheInternal(
      input: RDD[InternalRow],              // 上游输入RDD
      output: Seq[Attribute],               // 输出属性
      batchSize: Int,                       // 批次大小   spark.sql.inMemoryColumnarStorage.batchSize
      useCompression: Boolean): RDD[CachedBatch] = {      // 是否开启压缩

    input.mapPartitionsInternal { rowIterator =>

      new Iterator[DefaultCachedBatch] {
        def next(): DefaultCachedBatch = {

          // 使用 Columnar -> ColumnBuilder
          val columnBuilders = output.map { attribute =>
            ColumnBuilder(attribute.dataType, batchSize, attribute.name, useCompression)
          }.toArray

          var rowCount = 0
          var totalSize = 0L

          // 插入一个batch 批次大小
          while (rowIterator.hasNext && rowCount < batchSize
              && totalSize < ColumnBuilder.MAX_BATCH_SIZE_IN_BYTE) {

            // 提取一行数据
            val row = rowIterator.next()

            assert(row.numFields == columnBuilders.length,
              s"Row column number mismatch, expected ${output.size} columns, but got ${row.numFields}. Row content: $row")

            var i = 0
            totalSize = 0
            while (i < row.numFields) {
              columnBuilders(i).appendFrom(row, i)                       // 将当前行对应列数据放置到 Columnar 中
              totalSize += columnBuilders(i).columnStats.sizeInBytes     // 记录当前列的数据大小
              i += 1
            }
            rowCount += 1
          }

          // 获取每一列的状态信息
          val stats = InternalRow.fromSeq(columnBuilders.flatMap(_.columnStats.collectedStatistics).toSeq)

          DefaultCachedBatch(
            rowCount,
            columnBuilders.map { builder =>
              JavaUtils.bufferToArray(builder.build())
            },       // 构建列式存储
            stats
          )
        }

        def hasNext: Boolean = rowIterator.hasNext
      }
    }
  }

  override def supportsColumnarOutput(schema: StructType): Boolean = schema.fields.forall(f =>
    f.dataType match {
      // More types can be supported, but this is to match the original implementation that
      // only supported primitive types "for ease of review"
      case BooleanType | ByteType | ShortType | IntegerType | LongType |
           FloatType | DoubleType => true
      case _ => false
    })

  override def vectorTypes(attributes: Seq[Attribute], conf: SQLConf): Option[Seq[String]] =
    Option(Seq.fill(attributes.length)(
      if (!conf.offHeapColumnVectorEnabled) {
        classOf[OnHeapColumnVector].getName
      } else {
        classOf[OffHeapColumnVector].getName
      }
    ))

  override def convertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {
    val offHeapColumnVectorEnabled = conf.offHeapColumnVectorEnabled
    val outputSchema = StructType.fromAttributes(selectedAttributes)
    val columnIndices =
      selectedAttributes.map(a => cacheAttributes.map(o => o.exprId).indexOf(a.exprId)).toArray

    def createAndDecompressColumn(cb: CachedBatch): ColumnarBatch = {
      val cachedColumnarBatch = cb.asInstanceOf[DefaultCachedBatch]
      val rowCount = cachedColumnarBatch.numRows
      val taskContext = Option(TaskContext.get())
      val columnVectors = if (!offHeapColumnVectorEnabled || taskContext.isEmpty) {
        OnHeapColumnVector.allocateColumns(rowCount, outputSchema)
      } else {
        OffHeapColumnVector.allocateColumns(rowCount, outputSchema)
      }
      val columnarBatch = new ColumnarBatch(columnVectors.asInstanceOf[Array[ColumnVector]])
      columnarBatch.setNumRows(rowCount)

      for (i <- selectedAttributes.indices) {
        ColumnAccessor.decompress(
          cachedColumnarBatch.buffers(columnIndices(i)),
          columnarBatch.column(i).asInstanceOf[WritableColumnVector],
          outputSchema.fields(i).dataType, rowCount)
      }
      taskContext.foreach(_.addTaskCompletionListener[Unit](_ => columnarBatch.close()))
      columnarBatch
    }

    input.map(createAndDecompressColumn)
  }

  override def convertCachedBatchToInternalRow(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow] = {
    // Find the ordinals and data types of the requested columns.
    val (requestedColumnIndices, requestedColumnDataTypes) =
      selectedAttributes.map { a =>
        cacheAttributes.map(_.exprId).indexOf(a.exprId) -> a.dataType
      }.unzip

    val columnTypes = requestedColumnDataTypes.map {
      case udt: UserDefinedType[_] => udt.sqlType
      case other => other
    }.toArray

    input.mapPartitionsInternal { cachedBatchIterator =>
      val columnarIterator = GenerateColumnAccessor.generate(columnTypes)
      columnarIterator.initialize(cachedBatchIterator.asInstanceOf[Iterator[DefaultCachedBatch]],
        columnTypes,
        requestedColumnIndices.toArray)
      columnarIterator
    }
  }
}

private[sql]
case class CachedRDDBuilder(
    serializer: CachedBatchSerializer,   // 序列化器
    storageLevel: StorageLevel,          // 缓存级别
    @transient cachedPlan: SparkPlan,    // 当前缓存的查询计划树
    tableName: Option[String]) {         // 当前缓存明明

  @transient @volatile private var _cachedColumnBuffers: RDD[CachedBatch] = null
  @transient @volatile private var _cachedColumnBuffersAreLoaded: Boolean = false

  val sizeInBytesStats: LongAccumulator = cachedPlan.session.sparkContext.longAccumulator  // 字节数据量统计
  val rowCountStats: LongAccumulator = cachedPlan.session.sparkContext.longAccumulator     // 数据行数统计

  // 当前缓存名称
  val cachedName = tableName.map(n => s"In-memory table $n")
    .getOrElse(StringUtils.abbreviate(cachedPlan.toString, 1024))

  def cachedColumnBuffers: RDD[CachedBatch] = {
    if (_cachedColumnBuffers == null) {
      synchronized {
        if (_cachedColumnBuffers == null) {
          _cachedColumnBuffers = buildBuffers()
        }
      }
    }
    _cachedColumnBuffers
  }

  def clearCache(blocking: Boolean = false): Unit = {
    if (_cachedColumnBuffers != null) {
      synchronized {
        if (_cachedColumnBuffers != null) {
          _cachedColumnBuffers.unpersist(blocking)
          _cachedColumnBuffers = null
        }
      }
    }
  }

  def isCachedColumnBuffersLoaded: Boolean = {
    if (_cachedColumnBuffers != null) {
      synchronized {
        return _cachedColumnBuffers != null && isCachedRDDLoaded
      }
    }
    false
  }

  private def isCachedRDDLoaded: Boolean = {
      _cachedColumnBuffersAreLoaded || {
        val bmMaster = SparkEnv.get.blockManager.master
        val rddLoaded = _cachedColumnBuffers.partitions.forall { partition =>
          bmMaster.getBlockStatus(RDDBlockId(_cachedColumnBuffers.id, partition.index), false)
            .exists { case(_, blockStatus) => blockStatus.isCached }
        }
        if (rddLoaded) {
          _cachedColumnBuffersAreLoaded = rddLoaded
        }
        rddLoaded
    }
  }

  private def buildBuffers(): RDD[CachedBatch] = {
    val cb = if (cachedPlan.supportsColumnar &&
        serializer.supportsColumnarInput(cachedPlan.output)) {
      serializer.convertColumnarBatchToCachedBatch(
        cachedPlan.executeColumnar(),
        cachedPlan.output,
        storageLevel,
        cachedPlan.conf)
    } else {
      serializer.convertInternalRowToCachedBatch(
        cachedPlan.execute(),
        cachedPlan.output,
        storageLevel,
        cachedPlan.conf)
    }

    val cached = cb.map { batch =>
      sizeInBytesStats.add(batch.sizeInBytes)
      rowCountStats.add(batch.numRows)
      batch
    }.persist(storageLevel)   // 数据存储缓存

    cached.setName(cachedName)
    cached
  }
}

object InMemoryRelation {

  private[this] var ser: Option[CachedBatchSerializer] = None
  private[this] def getSerializer(sqlConf: SQLConf): CachedBatchSerializer = synchronized {
    if (ser.isEmpty) {
      val serName = sqlConf.getConf(StaticSQLConf.SPARK_CACHE_SERIALIZER)
      val serClass = Utils.classForName(serName)
      val instance = serClass.getConstructor().newInstance().asInstanceOf[CachedBatchSerializer]
      ser = Some(instance)
    }
    ser.get
  }

  /* Visible for testing */
  private[columnar] def clearSerializer(): Unit = synchronized { ser = None }

  def convertToColumnarIfPossible(plan: SparkPlan): SparkPlan = plan match {
    case gen: WholeStageCodegenExec => gen.child match {
      case c2r: ColumnarToRowTransition => c2r.child match {
        case ia: InputAdapter => ia.child
        case _ => plan
      }
      case _ => plan
    }
    case c2r: ColumnarToRowTransition => // This matches when whole stage code gen is disabled.
      c2r.child
    case _ => plan
  }

  def apply(
      storageLevel: StorageLevel,                        // 存储级别
      qe: QueryExecution,                                // 当前的 catalyst 执行包装器
      tableName: Option[String]): InMemoryRelation = {   // 当前LogicalPlan 的缓存名称

    val optimizedPlan = qe.optimizedPlan                                        //  获取对应的优化树 - command类型会立即触发执行
    // CachedBatchSerializer
    val serializer = getSerializer(optimizedPlan.conf)                          // 获取序列化器 spark.sql.cache.serializer

    // 获取该计划的物理树
    val child = if (serializer.supportsColumnarInput(optimizedPlan.output)) {   // 判断是否支持列式读取
      convertToColumnarIfPossible(qe.executedPlan)                              //
    } else {
      qe.executedPlan
    }

    val cacheBuilder = CachedRDDBuilder(serializer, storageLevel, child, tableName)
    val relation = new InMemoryRelation(child.output, cacheBuilder, optimizedPlan.outputOrdering)
    relation.statsOfPlanToCache = optimizedPlan.stats
    relation
  }

  /**
   * This API is intended only to be used for testing.
   */
  def apply(
      serializer: CachedBatchSerializer,
      storageLevel: StorageLevel,
      child: SparkPlan,
      tableName: Option[String],
      optimizedPlan: LogicalPlan): InMemoryRelation = {
    val cacheBuilder = CachedRDDBuilder(serializer, storageLevel, child, tableName)
    val relation = new InMemoryRelation(child.output, cacheBuilder, optimizedPlan.outputOrdering)
    relation.statsOfPlanToCache = optimizedPlan.stats
    relation
  }

  def apply(cacheBuilder: CachedRDDBuilder, qe: QueryExecution): InMemoryRelation = {
    val optimizedPlan = qe.optimizedPlan
    val newBuilder = if (cacheBuilder.serializer.supportsColumnarInput(optimizedPlan.output)) {
      cacheBuilder.copy(cachedPlan = convertToColumnarIfPossible(qe.executedPlan))
    } else {
      cacheBuilder.copy(cachedPlan = qe.executedPlan)
    }
    val relation = new InMemoryRelation(
      newBuilder.cachedPlan.output, newBuilder, optimizedPlan.outputOrdering)
    relation.statsOfPlanToCache = optimizedPlan.stats
    relation
  }

  def apply(
      output: Seq[Attribute],
      cacheBuilder: CachedRDDBuilder,
      outputOrdering: Seq[SortOrder],
      statsOfPlanToCache: Statistics): InMemoryRelation = {
    val relation = InMemoryRelation(output, cacheBuilder, outputOrdering)
    relation.statsOfPlanToCache = statsOfPlanToCache
    relation
  }
}

case class InMemoryRelation(
    output: Seq[Attribute],                              // 输出属性
    @transient cacheBuilder: CachedRDDBuilder,           // 当前CacheRDD 的构建工具
    override val outputOrdering: Seq[SortOrder])         // 指定是否要保持排序的列
  extends logical.LeafNode with MultiInstanceRelation {  // 叶子结点

  @volatile var statsOfPlanToCache: Statistics = null

  override def innerChildren: Seq[SparkPlan] = Seq(cachedPlan)

  override def doCanonicalize(): logical.LogicalPlan =
    copy(output = output.map(QueryPlan.normalizeExpressions(_, cachedPlan.output)),
      cacheBuilder,
      outputOrdering)

  @transient val partitionStatistics = new PartitionStatistics(output)

  def cachedPlan: SparkPlan = cacheBuilder.cachedPlan

  private[sql] def updateStats(
      rowCount: Long,
      newColStats: Map[Attribute, ColumnStat]): Unit = this.synchronized {
    val newStats = statsOfPlanToCache.copy(
      rowCount = Some(rowCount),
      attributeStats = AttributeMap((statsOfPlanToCache.attributeStats ++ newColStats).toSeq)
    )
    statsOfPlanToCache = newStats
  }

  override def computeStats(): Statistics = {
    if (!cacheBuilder.isCachedColumnBuffersLoaded) {
      // Underlying columnar RDD hasn't been materialized, use the stats from the plan to cache.
      statsOfPlanToCache
    } else {
      statsOfPlanToCache.copy(
        sizeInBytes = cacheBuilder.sizeInBytesStats.value.longValue,
        rowCount = Some(cacheBuilder.rowCountStats.value.longValue)
      )
    }
  }

  def withOutput(newOutput: Seq[Attribute]): InMemoryRelation =
    InMemoryRelation(newOutput, cacheBuilder, outputOrdering, statsOfPlanToCache)

  override def newInstance(): this.type = {
    InMemoryRelation(
      output.map(_.newInstance()),
      cacheBuilder,
      outputOrdering,
      statsOfPlanToCache).asInstanceOf[this.type]
  }

  // override `clone` since the default implementation won't carry over mutable states.
  override def clone(): LogicalPlan = {
    val cloned = this.copy()
    cloned.statsOfPlanToCache = this.statsOfPlanToCache
    cloned
  }

  override def simpleString(maxFields: Int): String =
    s"InMemoryRelation [${truncatedString(output, ", ", maxFields)}], ${cacheBuilder.storageLevel}"
}
