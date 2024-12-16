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

package org.apache.spark.shuffle.sort;

import java.util.Comparator;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.collection.Sorter;
import org.apache.spark.util.collection.unsafe.sort.RadixSort;

final class ShuffleInMemorySorter {

  private static final class SortComparator implements Comparator<PackedRecordPointer> {
    @Override
    public int compare(PackedRecordPointer left, PackedRecordPointer right) {
      return Integer.compare(left.getPartitionId(), right.getPartitionId());
    }
  }
  private static final SortComparator SORT_COMPARATOR = new SortComparator();

  private final MemoryConsumer consumer;

  /**
   * 一个由 {@link PackedRecordPointer} 编码的记录指针和分区 ID 的数组。排序操作在此数组上进行，而不是直接操作记录。
   *
   * 只有数组的一部分将用于存储指针，其余部分将保留为排序的临时缓冲区。
   */
  private LongArray array;

  /**
   * 是否使用基数排序对内存中的分区 ID 进行排序。基数排序速度更快，但在添加指针时需要额外保留内存。
   */
  private final boolean useRadixSort;

  /**
   * 指针数组中可以插入新记录的位置。
   */
  private int pos = 0;

  /**
   * 可以插入多少记录，因为数组的一部分应该留作排序使用。
   */
  private int usableCapacity = 0;

  private final int initialSize;

  ShuffleInMemorySorter(MemoryConsumer consumer, int initialSize, boolean useRadixSort) {
    this.consumer = consumer;
    assert (initialSize > 0);
    this.initialSize = initialSize;
    this.useRadixSort = useRadixSort;
    this.array = consumer.allocateArray(initialSize);   // 分配指定内存空间数组
    this.usableCapacity = getUsableCapacity();
  }

  // 如果使用 useRadixSort 则预留一半空间，否则预留1/3, 用于排序
  private int getUsableCapacity() {

    return (int) (array.size() / (useRadixSort ? 2 : 1.5));
  }

  public void free() {
    if (array != null) {
      consumer.freeArray(array);
      array = null;
    }
  }

  public int numRecords() {
    return pos;
  }

  public void reset() {
    // Reset `pos` here so that `spill` triggered by the below `allocateArray` will be no-op.
    pos = 0;
    if (consumer != null) {
      consumer.freeArray(array);
      // As `array` has been released, we should set it to  `null` to avoid accessing it before
      // `allocateArray` returns. `usableCapacity` is also set to `0` to avoid any codes writing
      // data to `ShuffleInMemorySorter` when `array` is `null` (e.g., in
      // ShuffleExternalSorter.growPointerArrayIfNecessary, we may try to access
      // `ShuffleInMemorySorter` when `allocateArray` throws SparkOutOfMemoryError).
      array = null;
      usableCapacity = 0;
      array = consumer.allocateArray(initialSize);
      usableCapacity = getUsableCapacity();
    }
  }

  public void expandPointerArray(LongArray newArray) {
    assert(newArray.size() > array.size());
    Platform.copyMemory(
      array.getBaseObject(),
      array.getBaseOffset(),
      newArray.getBaseObject(),
      newArray.getBaseOffset(),
      pos * 8L
    );
    consumer.freeArray(array);
    array = newArray;
    usableCapacity = getUsableCapacity();
  }

  // 表示是否还有足够的空间来存储数据
  public boolean hasSpaceForAnotherRecord() {
    return pos < usableCapacity;
  }

  public long getMemoryUsage() {
    return array.size() * 8;
  }

  /**
   * 插入一个待排序的记录。
   *
   * @param recordPointer 指向记录的指针，由任务内存管理器编码。
   *                      由于排序器使用了某些指针压缩技术，排序只能在指向数据页面前 {@link PackedRecordPointer#MAXIMUM_PAGE_SIZE_BYTES} 字节内的位置的指针上进行操作。
   * @param partitionId 分区 ID，必须小于或等于 {@link PackedRecordPointer#MAXIMUM_PARTITION_ID}。
   */
  public void insertRecord(long recordPointer, int partitionId) {
    if (!hasSpaceForAnotherRecord()) {
      throw new IllegalStateException("There is no space for new record");
    }
    array.set(pos, PackedRecordPointer.packPointer(recordPointer, partitionId));
    pos++;
  }

  /**
   * An iterator-like class that's used instead of Java's Iterator in order to facilitate inlining.
   */
  public static final class ShuffleSorterIterator {

    private final LongArray pointerArray;
    private final int limit;
    final PackedRecordPointer packedRecordPointer = new PackedRecordPointer();
    private int position = 0;

    ShuffleSorterIterator(int numRecords, LongArray pointerArray, int startingPosition) {
      this.limit = numRecords + startingPosition;
      this.pointerArray = pointerArray;
      this.position = startingPosition;
    }

    public boolean hasNext() {
      return position < limit;
    }

    public void loadNext() {
      packedRecordPointer.set(pointerArray.get(position));
      position++;
    }
  }

  /**
   * Return an iterator over record pointers in sorted order.
   */
  public ShuffleSorterIterator getSortedIterator() {
    int offset = 0;
    if (useRadixSort) {
      offset = RadixSort.sort(
        array, pos,
        PackedRecordPointer.PARTITION_ID_START_BYTE_INDEX,
        PackedRecordPointer.PARTITION_ID_END_BYTE_INDEX, false, false);
    } else {
      MemoryBlock unused = new MemoryBlock(
        array.getBaseObject(),
        array.getBaseOffset() + pos * 8L,
        (array.size() - pos) * 8L);
      LongArray buffer = new LongArray(unused);
      Sorter<PackedRecordPointer, LongArray> sorter =
        new Sorter<>(new ShuffleSortDataFormat(buffer));

      sorter.sort(array, 0, pos, SORT_COMPARATOR);
    }
    return new ShuffleSorterIterator(pos, array, offset);
  }
}
