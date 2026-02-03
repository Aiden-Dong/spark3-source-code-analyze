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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.memory.SparkOutOfMemoryError
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.execution.{UnsafeFixedWidthAggregationMap, UnsafeKVExternalSorter}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.KVIterator

/**
 * 一个用于计算聚合函数的迭代器，操作 [[UnsafeRow]] 对象。.
 *
 * 该迭代器首先使用基于哈希的聚合来处理输入行。它使用哈希映射来存储分组及其对应的聚合缓冲区。
 * 如果该映射无法从内存管理器分配内存，它会将映射溢出到磁盘并创建一个新的映射。
 * 处理完所有输入后，使用外部排序器将所有溢出文件合并在一起，并执行基于排序的聚合。
 *
 * 处理过程包含以下步骤：
 * • **步骤 0**: 执行基于哈希的聚合
 * • **步骤 1**: 根据分组表达式的值对哈希映射的所有条目进行排序，并将它们溢出到磁盘
 * • **步骤 2**: 基于溢出的已排序映射条目创建外部排序器，并重置映射
 * • **步骤 3**: 从外部排序器获取已排序的 [[KVIterator]]
 * • **步骤 4**: 重复步骤 0，直到没有更多输入
 * • **步骤 5**: 在已排序的迭代器上初始化基于排序的聚合
 *
 * 然后，该迭代器以基于排序的聚合方式工作。
 *
 * 该类的代码组织如下：
 * • **第 1 部分**: 初始化聚合函数
 * • **第 2 部分**: 用于设置聚合缓冲区值、处理来自 inputIter 的输入行以及生成输出行的方法和字段
 * • **第 3 部分**: 基于哈希的聚合使用的方法和字段
 * • **第 4 部分**: 当我们切换到基于排序的聚合时使用的方法和字段
 * • **第 5 部分**: 基于排序的聚合使用的方法和字段
 * • **第 6 部分**: 加载输入并处理输入行
 * • **第 7 部分**: 该迭代器的公共方法
 * • **第 8 部分**: 当没有输入且没有分组表达式时用于生成结果的实用函数
 *
 * @param partIndex 分区索引
 * @param groupingExpressions  分组键的表达式
 * @param aggregateExpressions 包含模式为 [[Partial]]、[[PartialMerge]] 或 [[Final]] 的 [[AggregateFunction]] 的 [[AggregateExpression]]
 * @param aggregateAttributes 当聚合表达式的输出存储在最终聚合缓冲区中时的属性
 * @param resultExpressions 用于生成输出行的表达式
 * @param newMutableProjection 用于创建可变投影的函数.
 * @param originalInputAttributes 表示来自 inputIter 的输入行的属性
 * @param inputIter 包含输入 [[UnsafeRow]] 的迭代器.
 */
class TungstenAggregationIterator(
    partIndex: Int,
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    originalInputAttributes: Seq[Attribute],
    inputIter: Iterator[InternalRow],
    testFallbackStartsAt: Option[(Int, Int)],
    numOutputRows: SQLMetric,
    peakMemory: SQLMetric,
    spillSize: SQLMetric,
    avgHashProbe: SQLMetric,
    numTasksFallBacked: SQLMetric)
  extends AggregationIterator(
    partIndex,
    groupingExpressions,
    originalInputAttributes,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    newMutableProjection) with Logging {

  ///////////////////////////////////////////////////////////////////////////
  // Part 1: Initializing aggregate functions.
  ///////////////////////////////////////////////////////////////////////////

  // Remember spill data size of this task before execute this operator so that we can
  // figure out how many bytes we spilled for this operator.
  private val spillSizeBefore = TaskContext.get().taskMetrics().memoryBytesSpilled

  ///////////////////////////////////////////////////////////////////////////
  // Part 2: Methods and fields used by setting aggregation buffer values,
  //         processing input rows from inputIter, and generating output
  //         rows.
  ///////////////////////////////////////////////////////////////////////////

  // 创建一个新的聚合缓冲区并初始化缓冲区值。
  // 此函数最多只能调用两次（创建哈希映射时和为基于排序的聚合创建重用缓冲区时）。
  // • 创建空的聚合缓冲区模板
  // • 初始化声明式聚合函数的缓冲区值
  // • 初始化命令式聚合函数的缓冲区值
  private def createNewAggregationBuffer(): UnsafeRow = {
    val bufferSchema = aggregateFunctions.flatMap(_.aggBufferAttributes)
    val buffer: UnsafeRow = UnsafeProjection.create(bufferSchema.map(_.dataType))
      .apply(new GenericInternalRow(bufferSchema.length))
    // Initialize declarative aggregates' buffer values
    expressionAggInitialProjection.target(buffer)(EmptyRow)
    // Initialize imperative aggregates' buffer values
    aggregateFunctions.collect { case f: ImperativeAggregate => f }.foreach(_.initialize(buffer))
    buffer
  }

  // 创建用于生成输出行的函数.
  override protected def generateResultProjection(): (UnsafeRow, InternalRow) => UnsafeRow = {
    val modes = aggregateExpressions.map(_.mode).distinct
    if (modes.nonEmpty && !modes.contains(Final) && !modes.contains(Complete)) {
      // Fast path for partial aggregation, UnsafeRowJoiner is usually faster than projection
      val groupingAttributes = groupingExpressions.map(_.toAttribute)
      val bufferAttributes = aggregateFunctions.flatMap(_.aggBufferAttributes)
      val groupingKeySchema = StructType.fromAttributes(groupingAttributes)
      val bufferSchema = StructType.fromAttributes(bufferAttributes)
      val unsafeRowJoiner = GenerateUnsafeRowJoiner.create(groupingKeySchema, bufferSchema)

      (currentGroupingKey: UnsafeRow, currentBuffer: InternalRow) => {
        unsafeRowJoiner.join(currentGroupingKey, currentBuffer.asInstanceOf[UnsafeRow])
      }
    } else {
      super.generateResultProjection()
    }
  }

  // An aggregation buffer containing initial buffer values. It is used to
  // initialize other aggregation buffers.
  private[this] val initialAggregationBuffer: UnsafeRow = createNewAggregationBuffer()

  ///////////////////////////////////////////////////////////////////////////
  // Part 3: Methods and fields used by hash-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  // This is the hash map used for hash-based aggregation. It is backed by an
  // UnsafeFixedWidthAggregationMap and it is used to store
  // all groups and their corresponding aggregation buffers for hash-based aggregation.
  private[this] val hashMap = new UnsafeFixedWidthAggregationMap(
    initialAggregationBuffer,
    StructType.fromAttributes(aggregateFunctions.flatMap(_.aggBufferAttributes)),
    StructType.fromAttributes(groupingExpressions.map(_.toAttribute)),
    TaskContext.get(),
    1024 * 16, // initial capacity
    TaskContext.get().taskMemoryManager().pageSizeBytes
  )

  // 用于读取和处理输入行的函数。处理输入行时，首先使用基于哈希的聚合，将分组和缓冲区放入hashMap中。
  // 如果内存不足，将使用多个哈希映射，每个满了就溢出，然后使用排序合并这些溢出，最后进行基于排序的聚合。
  private def processInputs(fallbackStartsAt: (Int, Int)): Unit = {
    if (groupingExpressions.isEmpty) {
      // // 如果没有分组表达式，我们可以一遍又一遍地重用同一个缓冲区
      val groupingKey = groupingProjection.apply(null)
      val buffer: UnsafeRow = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        processRow(buffer, newInput)
      }
    } else {
      var i = 0
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        val groupingKey = groupingProjection.apply(newInput)   // 获取当前数据的keyh
        var buffer: UnsafeRow = null
        if (i < fallbackStartsAt._2) {
          buffer = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
        }

        if (buffer == null) {   // 内存不足时spill处理
          val sorter = hashMap.destructAndCreateExternalSorter()

          if (externalSorter == null) {
            externalSorter = sorter
          } else {
            externalSorter.merge(sorter)  // 合并多个spill文件
          }
          i = 0
          buffer = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
          if (buffer == null) {
            // failed to allocate the first page
            // scalastyle:off throwerror
            throw new SparkOutOfMemoryError("No enough memory for aggregation")
            // scalastyle:on throwerror
          }
        }
        // 将当前行的值累加到缓冲区
        processRow(buffer, newInput)
        i += 1
      }

      if (externalSorter != null) {
        val sorter = hashMap.destructAndCreateExternalSorter()
        externalSorter.merge(sorter)
        hashMap.free()

        switchToSortBasedAggregation()
      }
    }
  }

  // The iterator created from hashMap. It is used to generate output rows when we
  // are using hash-based aggregation.
  private[this] var aggregationBufferMapIterator: KVIterator[UnsafeRow, UnsafeRow] = null

  // Indicates if aggregationBufferMapIterator still has key-value pairs.
  private[this] var mapIteratorHasNext: Boolean = false

  ///////////////////////////////////////////////////////////////////////////
  // Part 4: Methods and fields used when we switch to sort-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  // This sorter is used for sort-based aggregation. It is initialized as soon as
  // we switch from hash-based to sort-based aggregation. Otherwise, it is not used.
  private[this] var externalSorter: UnsafeKVExternalSorter = null

  /**
   * Switch to sort-based aggregation when the hash-based approach is unable to acquire memory.
   */
  private def switchToSortBasedAggregation(): Unit = {
    logInfo("falling back to sort based aggregation.")

    // Basically the value of the KVIterator returned by externalSorter
    // will be just aggregation buffer, so we rewrite the aggregateExpressions to reflect it.
    val newExpressions = aggregateExpressions.map {
      case agg @ AggregateExpression(_, Partial, _, _, _) =>
        agg.copy(mode = PartialMerge)
      case agg @ AggregateExpression(_, Complete, _, _, _) =>
        agg.copy(mode = Final)
      case other => other
    }
    val newFunctions = initializeAggregateFunctions(newExpressions, 0)
    val newInputAttributes = newFunctions.flatMap(_.inputAggBufferAttributes)
    sortBasedProcessRow = generateProcessRow(newExpressions, newFunctions, newInputAttributes)

    // Step 5: Get the sorted iterator from the externalSorter.
    sortedKVIterator = externalSorter.sortedIterator()

    // Step 6: Pre-load the first key-value pair from the sorted iterator to make
    // hasNext idempotent.
    sortedInputHasNewGroup = sortedKVIterator.next()

    // Copy the first key and value (aggregation buffer).
    if (sortedInputHasNewGroup) {
      val key = sortedKVIterator.getKey
      val value = sortedKVIterator.getValue
      nextGroupingKey = key.copy()
      currentGroupingKey = key.copy()
      firstRowInNextGroup = value.copy()
    }

    // Step 7: set sortBased to true.
    sortBased = true
    numTasksFallBacked += 1
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 5: Methods and fields used by sort-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  // Indicates if we are using sort-based aggregation. Because we first try to use
  // hash-based aggregation, its initial value is false.
  private[this] var sortBased: Boolean = false

  // The KVIterator containing input rows for the sort-based aggregation. It will be
  // set in switchToSortBasedAggregation when we switch to sort-based aggregation.
  private[this] var sortedKVIterator: UnsafeKVExternalSorter#KVSorterIterator = null

  // The grouping key of the current group.
  private[this] var currentGroupingKey: UnsafeRow = null

  // The grouping key of next group.
  private[this] var nextGroupingKey: UnsafeRow = null

  // The first row of next group.
  private[this] var firstRowInNextGroup: UnsafeRow = null

  // Indicates if we has new group of rows from the sorted input iterator.
  private[this] var sortedInputHasNewGroup: Boolean = false

  // The aggregation buffer used by the sort-based aggregation.
  private[this] val sortBasedAggregationBuffer: UnsafeRow = createNewAggregationBuffer()

  // The function used to process rows in a group
  private[this] var sortBasedProcessRow: (InternalRow, InternalRow) => Unit = null

  // Processes rows in the current group. It will stop when it find a new group.
  private def processCurrentSortedGroup(): Unit = {
    // First, we need to copy nextGroupingKey to currentGroupingKey.
    currentGroupingKey.copyFrom(nextGroupingKey)
    // Now, we will start to find all rows belonging to this group.
    // We create a variable to track if we see the next group.
    var findNextPartition = false
    // firstRowInNextGroup is the first row of this group. We first process it.
    sortBasedProcessRow(sortBasedAggregationBuffer, firstRowInNextGroup)

    // The search will stop when we see the next group or there is no
    // input row left in the iter.
    // Pre-load the first key-value pair to make the condition of the while loop
    // has no action (we do not trigger loading a new key-value pair
    // when we evaluate the condition).
    var hasNext = sortedKVIterator.next()
    while (!findNextPartition && hasNext) {
      // Get the grouping key and value (aggregation buffer).
      val groupingKey = sortedKVIterator.getKey
      val inputAggregationBuffer = sortedKVIterator.getValue

      // Check if the current row belongs the current input row.
      if (currentGroupingKey.equals(groupingKey)) {
        sortBasedProcessRow(sortBasedAggregationBuffer, inputAggregationBuffer)

        hasNext = sortedKVIterator.next()
      } else {
        // We find a new group.
        findNextPartition = true
        // copyFrom will fail when
        nextGroupingKey.copyFrom(groupingKey)
        firstRowInNextGroup.copyFrom(inputAggregationBuffer)
      }
    }
    // We have not seen a new group. It means that there is no new row in the input
    // iter. The current group is the last group of the sortedKVIterator.
    if (!findNextPartition) {
      sortedInputHasNewGroup = false
      sortedKVIterator.close()
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 6: Loads input rows and setup aggregationBufferMapIterator if we
  //         have not switched to sort-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Start processing input rows.
   */
  processInputs(testFallbackStartsAt.getOrElse((Int.MaxValue, Int.MaxValue)))

  // If we did not switch to sort-based aggregation in processInputs,
  // we pre-load the first key-value pair from the map (to make hasNext idempotent).
  if (!sortBased) {
    // First, set aggregationBufferMapIterator.
    aggregationBufferMapIterator = hashMap.iterator()
    // Pre-load the first key-value pair from the aggregationBufferMapIterator.
    mapIteratorHasNext = aggregationBufferMapIterator.next()
    // If the map is empty, we just free it.
    if (!mapIteratorHasNext) {
      hashMap.free()
    }
  }

  TaskContext.get().addTaskCompletionListener[Unit](_ => {
    // At the end of the task, update the task's peak memory usage. Since we destroy
    // the map to create the sorter, their memory usages should not overlap, so it is safe
    // to just use the max of the two.
    val mapMemory = hashMap.getPeakMemoryUsedBytes
    val sorterMemory = Option(externalSorter).map(_.getPeakMemoryUsedBytes).getOrElse(0L)
    val maxMemory = Math.max(mapMemory, sorterMemory)
    val metrics = TaskContext.get().taskMetrics()
    peakMemory.set(maxMemory)
    spillSize.set(metrics.memoryBytesSpilled - spillSizeBefore)
    metrics.incPeakExecutionMemory(maxMemory)

    // Updating average hashmap probe
    avgHashProbe.set(hashMap.getAvgHashProbeBucketListIterations)
  })

  ///////////////////////////////////////////////////////////////////////////
  // Part 7: Iterator's public methods.
  ///////////////////////////////////////////////////////////////////////////

  override final def hasNext: Boolean = {
    (sortBased && sortedInputHasNewGroup) || (!sortBased && mapIteratorHasNext)
  }

  override final def next(): UnsafeRow = {
    if (hasNext) {
      val res = if (sortBased) {
        // Process the current group.
        processCurrentSortedGroup()
        // Generate output row for the current group.
        val outputRow = generateOutput(currentGroupingKey, sortBasedAggregationBuffer)
        // Initialize buffer values for the next group.
        sortBasedAggregationBuffer.copyFrom(initialAggregationBuffer)

        outputRow
      } else {
        // We did not fall back to sort-based aggregation.
        val result =
          generateOutput(
            aggregationBufferMapIterator.getKey,
            aggregationBufferMapIterator.getValue)

        // Pre-load next key-value pair form aggregationBufferMapIterator to make hasNext
        // idempotent.
        mapIteratorHasNext = aggregationBufferMapIterator.next()

        if (!mapIteratorHasNext) {
          // If there is no input from aggregationBufferMapIterator, we copy current result.
          val resultCopy = result.copy()
          // Then, we free the map.
          hashMap.free()

          resultCopy
        } else {
          result
        }
      }

      numOutputRows += 1
      res
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 8: Utility functions
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Generate an output row when there is no input and there is no grouping expression.
   */
  def outputForEmptyGroupingKeyWithoutInput(): UnsafeRow = {
    if (groupingExpressions.isEmpty) {
      sortBasedAggregationBuffer.copyFrom(initialAggregationBuffer)
      // We create an output row and copy it. So, we can free the map.
      val resultCopy =
        generateOutput(UnsafeRow.createFromByteArray(0, 0), sortBasedAggregationBuffer).copy()
      hashMap.free()
      resultCopy
    } else {
      throw new IllegalStateException(
        "This method should not be called when groupingExpressions is not empty.")
    }
  }
}
