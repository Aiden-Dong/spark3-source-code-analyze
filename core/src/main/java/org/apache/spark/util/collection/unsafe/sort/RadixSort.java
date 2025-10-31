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

package org.apache.spark.util.collection.unsafe.sort;

import com.google.common.primitives.Ints;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;


/**********************************************************************
 * <pre>
 *  ========= RadixSort 基数排序工作原理演示 =========
 *
 *  假设我们要排序这些数字: [170, 45, 75, 90, 2, 802, 24, 66]
 *
 * 步骤1: 按个位数排序 (最低有效位)
 * 原始数组:  [170, 45, 75, 90, 2, 802, 24, 66]
 * 个位数:    [ 0,  5,  5,  0, 2,   2,  4,  6]
 *
 * 按个位数分桶:
 * 桶0: [170, 90]
 * 桶1: []
 * 桶2: [2, 802]
 * 桶3: []
 * 桶4: [24]
 * 桶5: [45, 75]
 * 桶6: [66]
 * 桶7: []
 * 桶8: []
 * 桶9: []
 *
 * 收集结果: [170, 90, 2, 802, 24, 45, 75, 66]
 *
 * 步骤2: 按十位数排序
 * 当前数组:  [170, 90, 2, 802, 24, 45, 75, 66]
 * 十位数:    [ 7,  9, 0,   0,  2,  4,  7,  6]
 *
 * 按十位数分桶:
 * 桶0: [2, 802]
 * 桶1: []
 * 桶2: [24]
 * 桶3: []
 * 桶4: [45]
 * 桶5: []
 * 桶6: [66]
 * 桶7: [170, 75]
 * 桶8: []
 * 桶9: [90]
 *
 * 收集结果: [2, 802, 24, 45, 66, 170, 75, 90]
 *
 * 步骤3: 按百位数排序 (最高有效位)
 * 当前数组:  [2, 802, 24, 45, 66, 170, 75, 90]
 * 百位数:    [0,   8,  0,  0,  0,   1,  0,  0]
 *
 * 按百位数分桶:
 * 桶0: [2, 24, 45, 66, 75, 90]
 * 桶1: [170]
 * 桶2: []
 * 桶3: []
 * 桶4: []
 * 桶5: []
 * 桶6: []
 * 桶7: []
 * 桶8: [802]
 * 桶9: []
 *
 * 最终结果: [2, 24, 45, 66, 75, 90, 170, 802]
 * ======================================================
 *
 * ============== Spark RadixSort 实现特点 ===============
 *
 * 1. 字节级排序 (Byte-level Sorting)
 *    ┌─────────────────────────────────────────────────────────────┐
 *    │  64位长整型 (Long)                                           │
 *    │  ┌───┬───┬───┬───┬───┬───┬───┬───┐                          │
 *    │  │ 7 │ 6 │ 5 │ 4 │ 3 │ 2 │ 1 │ 0 │ <- 字节索引               │
 *    │  └───┴───┴───┴───┴───┴───┴───┴───┘                          │
 *    │    ↑                           ↑                            │
 *    │   MSB                         LSB                           │
 *    │  (最高位)                   (最低位)                          │
 *    └─────────────────────────────────────────────────────────────┘
 *
 * 2. 内存布局优化
 *    原始数组:     [data1][data2][data3][data4][空间][空间][空间][空间]
 *                   ↑                              ↑
 *                   输入区域                        输出区域
 *
 *    排序过程中会在输入和输出区域之间交替使用
 *
 * 3. 计数排序 (Counting Sort) 每个字节
 *    对于每个字节位置，创建256个桶 (0x00 到 0xFF)
 *
 *    字节值:  0x00  0x01  0x02  ...  0xFE  0xFF
 *    计数:    [ 3 ][ 1 ][ 0 ]  ...  [ 2 ][ 1 ]
 *             ↓     ↓     ↓          ↓     ↓
 *    偏移:    [ 0 ][ 3 ][ 4 ]  ...  [98 ][100]
 *
 * 4. 有符号数处理
 *    对于有符号数，负数 (0x80-0xFF) 排在前面:
 *
 *    无符号: 0x00, 0x01, ..., 0x7F, 0x80, 0x81, ..., 0xFF
 *    有符号: 0x80, 0x81, ..., 0xFF, 0x00, 0x01, ..., 0x7F
 *            (负数)                  (正数)
 *
 * 5. 键前缀数组优化
 *    普通排序: [key1][key2][key3]...
 *    键前缀:   [key1][prefix1][key2][prefix2][key3][prefix3]...
 *             只对 prefix 部分排序，key 跟随移动
 * ======================================================
 *
 * =================== 字节级基数排序示例 ===================
 *
 * 假设排序这些64位数字: [0x1234, 0x5678, 0x12AB, 0x00CD]
 *
 * 二进制表示:
 * 0x1234 = 0000 0000 0000 0000 0000 0000 0000 0000 0001 0010 0011 0100
 * 0x5678 = 0000 0000 0000 0000 0000 0000 0000 0000 0101 0110 0111 1000
 * 0x12AB = 0000 0000 0000 0000 0000 0000 0000 0000 0001 0010 1010 1011
 * 0x00CD = 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 1100 1101
 *
 * 字节分解 (从右到左，字节0到字节7):
 *         字节7 字节6 字节5 字节4 字节3 字节2 字节1 字节0
 * 0x1234:  00    00    00    00    00    00    12    34
 * 0x5678:  00    00    00    00    00    00    56    78
 * 0x12AB:  00    00    00    00    00    00    12    AB
 * 0x00CD:  00    00    00    00    00    00    00    CD
 *
 * 步骤1: 按字节0排序 (最低字节)
 * 字节0值: [34, 78, AB, CD]
 * 排序后:  [0x1234, 0x5678, 0x12AB, 0x00CD] (34 < 78 < AB < CD)
 *
 * 步骤2: 按字节1排序
 * 字节1值: [12, 56, 12, 00]
 * 分桶:
 *   桶00: [0x00CD]
 *   桶12: [0x1234, 0x12AB] (保持相对顺序)
 *   桶56: [0x5678]
 * 排序后: [0x00CD, 0x1234, 0x12AB, 0x5678]
 *
 * 字节2-7都是00，跳过...
 *
 * 最终结果: [0x00CD, 0x1234, 0x12AB, 0x5678]
 * 即:       [205, 4660, 4779, 22136]
 *
 * === 优化特性 ===
 *
 * 1. 跳过相同字节, 如果某个字节位置所有数字都相同，直接跳过该轮排序
 * 2. 原地排序, 使用数组的后半部分作为临时空间，避免额外内存分配
 * 3. 缓存友好, 按字节顺序访问，提高CPU缓存命中率
 *
 *
 * </pre>
 *
 * @param
 * @return
 * @throw
 *====================================================================*/
public class RadixSort {

  /**
   * 使用最低有效位基数排序对给定的长整型数组进行排序。此例程假设数组末尾有至少等于记录数量的额外空间。排序是破坏性的，
   * 可能会重新定位数组中数据的位置。
   *
   * @param array 长整型元素数组，后面跟着至少同样多的空槽位。
   * @param numRecords 数组中数据记录的数量。
   * @param startByteIndex 从最低有效字节开始计数，每个长整型排序的第一个字节（范围[0, 7]）。   ---- 这里是因为，有些场景(比如只排分区部分)只排序部分数据
   * @param endByteIndex 从最低有效字节开始计数，每个长整型排序的最后一个字节（范围[0, 7]）。必须大于 startByteIndex。
   * @param desc 是否为降序（二进制顺序）排序。
   * @param signed 是否为有符号（二进制补码）排序。
   *
   * @return 给定数组中已排序数据的起始索引。为了效率，我们返回这个索引而不是总是将数据复制回位置零。
   */
  public static int sort(LongArray array, long numRecords,     // 排序数据体
                         int startByteIndex, int endByteIndex,  // 排序字节范围
                         boolean desc, boolean signed) {        // 排序方式

    assert startByteIndex >= 0 : "startByteIndex (" + startByteIndex + ") should >= 0";
    assert endByteIndex <= 7 : "endByteIndex (" + endByteIndex + ") should <= 7";
    assert endByteIndex > startByteIndex;
    // 必须要留出1倍的剩余空间用于排序
    assert numRecords * 2 <= array.size();

    long inIndex = 0;
    long outIndex = numRecords;
    if (numRecords > 0) {

      // 预计算每个字节位置的值分布统计, 如果某字节位置所有值相同，对应数组为null
      long[][] counts = getCounts(array, numRecords, startByteIndex, endByteIndex);

      for (int i = startByteIndex; i <= endByteIndex; i++) {   // 按位开始基数排序
        if (counts[i] != null) {

          // 对当前字节位置执行计数排序
          sortAtByte(array, numRecords, counts[i], i, inIndex, outIndex, desc, signed && i == endByteIndex);

          long tmp = inIndex;
          inIndex = outIndex;
          outIndex = tmp;
        }
      }
    }
    return Ints.checkedCast(inIndex);
  }

  /**
   * 通过将数据复制到指定字节偏移处每个字节值的目标偏移位置来执行部分排序。
   *
   * @param array 要部分排序的数组。
   * @param numRecords 数组中数据记录的数量。
   * @param counts 每个字节值的计数。此例程会破坏性地修改此数组。
   * @param byteIdx 从最低有效字节开始计数，要排序的长整型中的字节。
   * @param inIndex 数组中输入数据所在的起始索引。
   * @param outIndex 应写入已排序输出数据的起始索引。
   * @param desc 是否为降序（二进制顺序）排序。
   * @param signed 是否为有符号（二进制补码）排序（仅适用于最后一个字节）。
   */
  private static void sortAtByte(
      LongArray array, long numRecords, long[] counts, int byteIdx, long inIndex, long outIndex,
      boolean desc, boolean signed) {
    assert counts.length == 256;
    long[] offsets = transformCountsToOffsets(
      counts, numRecords, array.getBaseOffset() + outIndex * 8L, 8, desc, signed);
    Object baseObject = array.getBaseObject();
    long baseOffset = array.getBaseOffset() + inIndex * 8L;
    long maxOffset = baseOffset + numRecords * 8L;
    for (long offset = baseOffset; offset < maxOffset; offset += 8) {
      long value = Platform.getLong(baseObject, offset);
      int bucket = (int)((value >>> (byteIdx * 8)) & 0xff);
      Platform.putLong(baseObject, offsets[bucket], value);
      offsets[bucket] += 8;
    }
  }

  /**
   * 计算给定数组中每个字节的值直方图。
   *
   * @param array 要计算记录数的数组。
   * @param numRecords 数组中数据记录的数量。
   * @param startByteIndex 要计算计数的第一个字节（之前的字节被跳过）。
   * @param endByteIndex 要计算计数的最后一个字节。
   *
   * @return 八个256字节计数数组的数组，从最低有效字节开始，每个字节一个。 如果该字节不需要排序，数组将为null。
   */
  private static long[][] getCounts(
      LongArray array, long numRecords, int startByteIndex, int endByteIndex) {
    long[][] counts = new long[8][];
    // 优化：进行快速预扫描以确定我们可以跳过哪些字节索引进行排序。
    // 如果特定索引处的所有字节值都相同，我们就不需要对其进行计数。
    long bitwiseMax = 0;
    long bitwiseMin = -1L;
    long maxOffset = array.getBaseOffset() + numRecords * 8L;
    Object baseObject = array.getBaseObject();
    for (long offset = array.getBaseOffset(); offset < maxOffset; offset += 8) {
      long value = Platform.getLong(baseObject, offset);
      bitwiseMax |= value;
      bitwiseMin &= value;
    }
    long bitsChanged = bitwiseMin ^ bitwiseMax;
    // Compute counts for each byte index.
    for (int i = startByteIndex; i <= endByteIndex; i++) {
      if (((bitsChanged >>> (i * 8)) & 0xff) != 0) {
        counts[i] = new long[256];
        // TODO(ekl) consider computing all the counts in one pass.
        for (long offset = array.getBaseOffset(); offset < maxOffset; offset += 8) {
          counts[i][(int)((Platform.getLong(baseObject, offset) >>> (i * 8)) & 0xff)]++;
        }
      }
    }
    return counts;
  }

  /**
   * 将计数转换为排序类型的适当unsafe输出偏移量。
   *
   * @param counts 每个字节值的计数。此例程会破坏性地修改此数组。
   * @param numRecords 原始数据数组中数据记录的数量。
   * @param outputOffset 从基础数组对象开始的输出偏移量（以字节为单位）。
   * @param bytesPerRecord 每条记录的大小（普通排序为8，键前缀排序为16）。
   * @param desc 是否为降序（二进制顺序）排序。
   * @param signed 是否为有符号（二进制补码）排序。
   *
   * @return 输入的计数数组。
   */
  private static long[] transformCountsToOffsets(
      long[] counts, long numRecords, long outputOffset, long bytesPerRecord,
      boolean desc, boolean signed) {
    assert counts.length == 256;
    int start = signed ? 128 : 0;  // output the negative records first (values 129-255).
    if (desc) {
      long pos = numRecords;
      for (int i = start; i < start + 256; i++) {
        pos -= counts[i & 0xff];
        counts[i & 0xff] = outputOffset + pos * bytesPerRecord;
      }
    } else {
      long pos = 0;
      for (int i = start; i < start + 256; i++) {
        long tmp = counts[i & 0xff];
        counts[i & 0xff] = outputOffset + pos * bytesPerRecord;
        pos += tmp;
      }
    }
    return counts;
  }

  /**
   * sort() 方法针对键前缀数组的特化版本。在这种类型的数组中，每条记录由两个长整型组成，只对其中的第二个进行排序。
   *
   * @param startIndex 数组中开始排序的起始索引。此参数在普通的 sort() 实现中不受支持。
   */
  public static int sortKeyPrefixArray(
      LongArray array,
      long startIndex,
      long numRecords,
      int startByteIndex,
      int endByteIndex,
      boolean desc,
      boolean signed) {
    assert startByteIndex >= 0 : "startByteIndex (" + startByteIndex + ") should >= 0";
    assert endByteIndex <= 7 : "endByteIndex (" + endByteIndex + ") should <= 7";
    assert endByteIndex > startByteIndex;
    assert numRecords * 4 <= array.size();
    long inIndex = startIndex;
    long outIndex = startIndex + numRecords * 2L;
    if (numRecords > 0) {
      long[][] counts = getKeyPrefixArrayCounts(
        array, startIndex, numRecords, startByteIndex, endByteIndex);
      for (int i = startByteIndex; i <= endByteIndex; i++) {
        if (counts[i] != null) {
          sortKeyPrefixArrayAtByte(
            array, numRecords, counts[i], i, inIndex, outIndex,
            desc, signed && i == endByteIndex);
          long tmp = inIndex;
          inIndex = outIndex;
          outIndex = tmp;
        }
      }
    }
    return Ints.checkedCast(inIndex);
  }

  /**
   * getCounts() 方法针对键前缀数组的特化版本。我们可能可以通过添加一些参数
   * 将其与 getCounts 合并，但在基准测试中这似乎会影响性能。
   */
  private static long[][] getKeyPrefixArrayCounts(
      LongArray array, long startIndex, long numRecords, int startByteIndex, int endByteIndex) {
    long[][] counts = new long[8][];
    long bitwiseMax = 0;
    long bitwiseMin = -1L;
    long baseOffset = array.getBaseOffset() + startIndex * 8L;
    long limit = baseOffset + numRecords * 16L;
    Object baseObject = array.getBaseObject();
    for (long offset = baseOffset; offset < limit; offset += 16) {
      long value = Platform.getLong(baseObject, offset + 8);
      bitwiseMax |= value;
      bitwiseMin &= value;
    }
    long bitsChanged = bitwiseMin ^ bitwiseMax;
    for (int i = startByteIndex; i <= endByteIndex; i++) {
      if (((bitsChanged >>> (i * 8)) & 0xff) != 0) {
        counts[i] = new long[256];
        for (long offset = baseOffset; offset < limit; offset += 16) {
          counts[i][(int)((Platform.getLong(baseObject, offset + 8) >>> (i * 8)) & 0xff)]++;
        }
      }
    }
    return counts;
  }

  /**
   * sortAtByte() 方法针对键前缀数组的特化版本。
   */
  private static void sortKeyPrefixArrayAtByte(
      LongArray array, long numRecords, long[] counts, int byteIdx, long inIndex, long outIndex,
      boolean desc, boolean signed) {
    assert counts.length == 256;
    long[] offsets = transformCountsToOffsets(
      counts, numRecords, array.getBaseOffset() + outIndex * 8L, 16, desc, signed);
    Object baseObject = array.getBaseObject();
    long baseOffset = array.getBaseOffset() + inIndex * 8L;
    long maxOffset = baseOffset + numRecords * 16L;
    for (long offset = baseOffset; offset < maxOffset; offset += 16) {
      long key = Platform.getLong(baseObject, offset);
      long prefix = Platform.getLong(baseObject, offset + 8);
      int bucket = (int)((prefix >>> (byteIdx * 8)) & 0xff);
      long dest = offsets[bucket];
      Platform.putLong(baseObject, dest, key);
      Platform.putLong(baseObject, dest + 8, prefix);
      offsets[bucket] += 16;
    }
  }
}
