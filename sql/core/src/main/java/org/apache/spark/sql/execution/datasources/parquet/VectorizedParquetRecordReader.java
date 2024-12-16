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

package org.apache.spark.sql.execution.datasources.parquet;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import scala.collection.JavaConverters;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * 一个专门的 RecordReader，使用 Parquet 列 API 直接读取到 InternalRows 或 ColumnarBatches。
 * 这在某种程度上基于 parquet-mr 的 ColumnReader。
 *
 * TODO: 处理需要超过 8 字节的十进制数，INT96 类型，模式不匹配等问题。
 * 这些都可以通过代码生成高效且轻松地处理。
 *
 * 该类可以返回 InternalRows 或 ColumnarBatches。如果启用了整个阶段的代码生成，
 * 该类会返回 ColumnarBatches，从而显著提高性能。
 * TODO: 使该类始终返回 ColumnarBatches。
 */
public class VectorizedParquetRecordReader extends SpecificParquetRecordReaderBase<Object> {

  // 向量化批次的容量。
  private int capacity;

  /**
   * Batch of rows that we assemble and the current index we've returned. Every time this
   * batch is used up (batchIdx == numBatched), we populated the batch.
   * 我们组装的行批次以及当前返回的索引。每次使用完这个批次（batchIdx == numBatched）时，我们会填充该批次。
   */
  private int batchIdx = 0;     // 当前正在读取的批次内偏移
  private int numBatched = 0;   // 当前批次读取的总的数据量

  // 封装可写的列向量以及其他与 Parquet 相关的信息，如重复级别/定义级别。

  private ParquetColumnVector[] columnVectors;

  private long rowsReturned;                   // 标识已经消费的行数
  private long totalCountLoadedSoFar = 0;      // 标识已经从 parquet 读取出来的行数(包含未消费的)

  // 对于每个叶子列，如果它在集合中，意味着该列在文件中缺失，我们将返回 NULL 值。
  private Set<ParquetColumn> missingColumns;

  // 时间戳 INT96 值应转换为的时区。如果不进行转换，则为 null。
  // 此项用于解决不同引擎在写入时间戳值时的兼容性问题。
  private final ZoneId convertTz;

  // 将日期/时间戳从儒略历重基到公历（Proleptic Gregorian）的模式。
  private final String datetimeRebaseMode;

  // 进行日期/时间戳重基操作的时区 ID。
  private final String datetimeRebaseTz;

  /**
   * The mode of rebasing INT96 timestamp from Julian to Proleptic Gregorian calendar.
   */
  private final String int96RebaseMode;
  // The time zone Id in which rebasing of INT96 is performed
  private final String int96RebaseTz;

  /**
   * 用于批量解码的 columnBatch 对象。它在第一次使用时创建，并触发批量解码。
   * 不允许在批量接口与逐行 RecordReader API 之间交替调用。
   * 只有在开发阶段通过额外标志启用此功能。目前仍在进行中，当前不支持的情况将导致难以诊断的错误。
   * 该功能应仅在开发时启用，以便开发此特性。
   *
   * 设置此项时，代码将在 RecordReader API 中早期分支。使用 MR 解码器和向量化解码器的路径之间没有共享代码。
   *
   * TODO：
   *  - 实现 v2 页面格式（确保创建正确的解码器）。
   */
  private ColumnarBatch columnarBatch;

  /**
   * If true, this class returns batches instead of rows.
   */
  private boolean returnColumnarBatch;

  /**
   * The memory mode of the columnarBatch
   */
  private final MemoryMode MEMORY_MODE;

  public VectorizedParquetRecordReader(
      ZoneId convertTz,
      String datetimeRebaseMode,
      String datetimeRebaseTz,
      String int96RebaseMode,
      String int96RebaseTz,
      boolean useOffHeap,
      int capacity) {
    this.convertTz = convertTz;
    this.datetimeRebaseMode = datetimeRebaseMode;
    this.datetimeRebaseTz = datetimeRebaseTz;
    this.int96RebaseMode = int96RebaseMode;
    this.int96RebaseTz = int96RebaseTz;
    MEMORY_MODE = useOffHeap ? MemoryMode.OFF_HEAP : MemoryMode.ON_HEAP;
    this.capacity = capacity;
  }

  // For test only.
  public VectorizedParquetRecordReader(boolean useOffHeap, int capacity) {
    this(
      null,
      "CORRECTED",
      "UTC",
      "LEGACY",
      ZoneId.systemDefault().getId(),
      useOffHeap,
      capacity);
  }

  /**
   * Implementation of RecordReader API.
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException, UnsupportedOperationException {
    super.initialize(inputSplit, taskAttemptContext);
    initializeInternal();
  }

  /**
   * Utility API that will read all the data in path. This circumvents the need to create Hadoop
   * objects to use this class. `columns` can contain the list of columns to project.
   */
  @Override
  public void initialize(String path, List<String> columns) throws IOException,
      UnsupportedOperationException {
    super.initialize(path, columns);
    initializeInternal();
  }

  @VisibleForTesting
  @Override
  public void initialize(
      MessageType fileSchema,
      MessageType requestedSchema,
      ParquetRowGroupReader rowGroupReader,
      int totalRowCount) throws IOException {
    super.initialize(fileSchema, requestedSchema, rowGroupReader, totalRowCount);
    initializeInternal();
  }

  @Override
  public void close() throws IOException {
    if (columnarBatch != null) {
      columnarBatch.close();
      columnarBatch = null;
    }
    super.close();
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    resultBatch();

    if (returnColumnarBatch) return nextBatch();

    if (batchIdx >= numBatched) {   // 标识已经读取完当前批次
      if (!nextBatch()) return false;
    }
    ++batchIdx;  // 更新索引位置
    return true;
  }

  @Override
  public Object getCurrentValue() {
    if (returnColumnarBatch) return columnarBatch;
    return columnarBatch.getRow(batchIdx - 1);
  }

  @Override
  public float getProgress() {
    return (float) rowsReturned / totalRowCount;
  }

  // Creates a columnar batch that includes the schema from the data files and the additional
  // partition columns appended to the end of the batch.
  // For example, if the data contains two columns, with 2 partition columns:
  // Columns 0,1: data columns
  // Column 2: partitionValues[0]
  // Column 3: partitionValues[1]
  private void initBatch(
      MemoryMode memMode,
      StructType partitionColumns,
      InternalRow partitionValues) {
    StructType batchSchema = new StructType();
    for (StructField f: sparkSchema.fields()) {
      batchSchema = batchSchema.add(f);
    }
    if (partitionColumns != null) {
      for (StructField f : partitionColumns.fields()) {
        batchSchema = batchSchema.add(f);
      }
    }

    WritableColumnVector[] vectors;
    if (memMode == MemoryMode.OFF_HEAP) {
      vectors = OffHeapColumnVector.allocateColumns(capacity, batchSchema);
    } else {
      vectors = OnHeapColumnVector.allocateColumns(capacity, batchSchema);
    }
    columnarBatch = new ColumnarBatch(vectors);

    columnVectors = new ParquetColumnVector[sparkSchema.fields().length];
    for (int i = 0; i < columnVectors.length; i++) {
      columnVectors[i] = new ParquetColumnVector(parquetColumn.children().apply(i),
        vectors[i], capacity, memMode, missingColumns);
    }

    if (partitionColumns != null) {
      int partitionIdx = sparkSchema.fields().length;
      for (int i = 0; i < partitionColumns.fields().length; i++) {
        ColumnVectorUtils.populate(vectors[i + partitionIdx], partitionValues, i);
        vectors[i + partitionIdx].setIsConstant();
      }
    }
  }

  private void initBatch() {
    initBatch(MEMORY_MODE, null, null);
  }

  public void initBatch(StructType partitionColumns, InternalRow partitionValues) {
    initBatch(MEMORY_MODE, partitionColumns, partitionValues);
  }

  /**
   * Returns the ColumnarBatch object that will be used for all rows returned by this reader.
   * This object is reused. Calling this enables the vectorized reader. This should be called
   * before any calls to nextKeyValue/nextBatch.
   */
  public ColumnarBatch resultBatch() {
    if (columnarBatch == null) initBatch();
    return columnarBatch;
  }

  /**
   * Can be called before any rows are returned to enable returning columnar batches directly.
   */
  public void enableReturningBatches() {
    returnColumnarBatch = true;
  }

  // 前进到下一批行。如果没有更多行，则返回 false。
  public boolean nextBatch() throws IOException {
    // 新的 batch ， 需要初始化数据集
    for (ParquetColumnVector vector : columnVectors) {
      vector.reset();
    }
    columnarBatch.setNumRows(0);

    // 标识消费的记录数已经达到了当前文件的总记录数
    if (rowsReturned >= totalRowCount) return false;

    checkEndOfRowGroup();

    int num = (int) Math.min(capacity, totalCountLoadedSoFar - rowsReturned);

    for (ParquetColumnVector cv : columnVectors) {
      for (ParquetColumnVector leafCv : cv.getLeaves()) {
        VectorizedColumnReader columnReader = leafCv.getColumnReader();
        if (columnReader != null) {
          columnReader.readBatch(num, leafCv.getValueVector(),
            leafCv.getRepetitionLevelVector(), leafCv.getDefinitionLevelVector());
        }
      }
      cv.assemble();
    }

    rowsReturned += num;
    columnarBatch.setNumRows(num);
    numBatched = num;
    batchIdx = 0;
    return true;
  }

  private void initializeInternal() throws IOException, UnsupportedOperationException {
    missingColumns = new HashSet<>();
    for (ParquetColumn column : JavaConverters.seqAsJavaList(parquetColumn.children())) {
      checkColumn(column);
    }
  }

  /**
   * Check whether a column from requested schema is missing from the file schema, or whether it
   * conforms to the type of the file schema.
   */
  private void checkColumn(ParquetColumn column) throws IOException {
    String[] path = JavaConverters.seqAsJavaList(column.path()).toArray(new String[0]);
    if (containsPath(fileSchema, path)) {
      if (column.isPrimitive()) {
        ColumnDescriptor desc = column.descriptor().get();
        ColumnDescriptor fd = fileSchema.getColumnDescription(desc.getPath());
        if (!fd.equals(desc)) {
          throw new UnsupportedOperationException("Schema evolution not supported.");
        }
      } else {
        for (ParquetColumn childColumn : JavaConverters.seqAsJavaList(column.children())) {
          checkColumn(childColumn);
        }
      }
    } else { // A missing column which is either primitive or complex
      if (column.required()) {
        // Column is missing in data but the required data is non-nullable. This file is invalid.
        throw new IOException("Required column is missing in data file. Col: " +
          Arrays.toString(path));
      }
      missingColumns.add(column);
    }
  }

  /**
   * Checks whether the given 'path' exists in 'parquetType'. The difference between this and
   * {@link MessageType#containsPath(String[])} is that the latter only support paths to leaf
   * nodes, while this support paths both to leaf and non-leaf nodes.
   */
  private boolean containsPath(Type parquetType, String[] path) {
    return containsPath(parquetType, path, 0);
  }

  private boolean containsPath(Type parquetType, String[] path, int depth) {
    if (path.length == depth) return true;
    if (parquetType instanceof GroupType) {
      String fieldName = path[depth];
      GroupType parquetGroupType = (GroupType) parquetType;
      if (parquetGroupType.containsField(fieldName)) {
        return containsPath(parquetGroupType.getType(fieldName), path, depth + 1);
      }
    }
    return false;
  }

  private void checkEndOfRowGroup() throws IOException {
    // 标识还有数据没有消费完
    if (rowsReturned != totalCountLoadedSoFar) return;

    PageReadStore pages = reader.readNextRowGroup();

    if (pages == null) {
      throw new IOException("expecting more rows but reached last block. Read "
          + rowsReturned + " out of " + totalRowCount);
    }

    for (ParquetColumnVector cv : columnVectors) {
      initColumnReader(pages, cv);
    }

    // 当前page 的总记录数
    totalCountLoadedSoFar += pages.getRowCount();
  }

  private void initColumnReader(PageReadStore pages, ParquetColumnVector cv) throws IOException {
    if (!missingColumns.contains(cv.getColumn())) {
      if (cv.getColumn().isPrimitive()) {
        ParquetColumn column = cv.getColumn();
        VectorizedColumnReader reader = new VectorizedColumnReader(
                column.descriptor().get(),
                column.required(),
                pages,
                convertTz,
                datetimeRebaseMode,
                datetimeRebaseTz,
                int96RebaseMode, int96RebaseTz, writerVersion);
        cv.setColumnReader(reader);
      } else {
        // Not in missing columns and is a complex type: this must be a struct
        for (ParquetColumnVector childCv : cv.getChildren()) {
          initColumnReader(pages, childCv);
        }
      }
    }
  }
}
