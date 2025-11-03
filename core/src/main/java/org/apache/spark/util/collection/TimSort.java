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

/*
 * Based on TimSort.java from the Android Open Source Project
 *
 *  Copyright (C) 2008 The Android Open Source Project
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.spark.util.collection;

import java.util.Comparator;

/******************************************************
 *
 * TimSort 排序过程演示
 * ===================
 *
 * 初始数组: [5, 2, 4, 6, 1, 3, 8, 7, 9]
 *
 * 步骤 1: 识别自然有序序列 (runs)
 * ┌─────────────────────────────────────┐
 * │ [5] [2,4,6] [1,3] [8] [7,9]         │
 * │  ↑    ↑      ↑    ↑    ↑            │
 * │ run1  run2   run3 run4 run5         │
 * └─────────────────────────────────────┘
 *
 * 步骤 2: 扩展短序列到最小长度 (minRun=4)
 * ┌─────────────────────────────────────┐
 * │ [2,4,5,6] [1,3,8,9] [7]             │
 * │    ↑         ↑      ↑               │
 * │  扩展后    合并后   剩余               │
 * └─────────────────────────────────────┘
 *
 * 步骤 3: 使用栈维护合并不变式
 * 栈状态变化:
 * ┌──────┐    ┌──────┐    ┌──────┐
 * │ [7]  │    │ [7]  │    │      │
 * ├──────┤ -> ├──────┤ -> ├──────┤
 * │[1,3, │    │[1,2, │    │[1,2, │
 * │8,9]  │    │3,4,5,│    │3,4,5,│
 * ├──────┤    │6,8,9]│    │6,7,8,│
 * │[2,4, │    │      │    │9]    │
 * │5,6]  │    │      │    │      │
 * └──────┘    └──────┘    └──────┘
 *
 * 最终结果: [1, 2, 3, 4, 5, 6, 7, 8, 9]
 *
 * TimSort 关键特性:
 * • 识别已有序序列，减少不必要的比较
 * • 对部分有序数据性能优异
 * • 稳定排序，相等元素顺序不变
 * • 自适应算法，根据数据特征调整策略
 *
 * 详细合并过程演示
 * ===============
 *
 * 假设有两个已排序的 runs 需要合并:
 * Run A: [1, 3, 5, 7]
 * Run B: [2, 4, 6, 8]
 *
 * 合并过程 (Galloping Mode):
 * ┌─────────────────────────────────────┐
 * │ Step 1: 比较 1 vs 2                  │
 * │ [1] 3 5 7    2 4 6 8                │
 * │  ↑           ↑                      │
 * │ 选择1        当前                    │
 * │ 结果: [1]                           │
 * └─────────────────────────────────────┘
 *
 * ┌─────────────────────────────────────┐
 * │ Step 2: 比较 3 vs 2                  │
 * │ 1 [3] 5 7    [2] 4 6 8              │
 * │    ↑          ↑                     │
 * │   当前        选择2                  │
 * │ 结果: [1, 2]                         │
 * └─────────────────────────────────────┘
 *
 * ┌─────────────────────────────────────┐
 * │ Step 3: 比较 3 vs 4                │
 * │ 1 [3] 5 7    2 [4] 6 8             │
 * │    ↑            ↑                  │
 * │   选择3         当前                │
 * │ 结果: [1, 2, 3]                    │
 * └─────────────────────────────────────┘
 *
 * 继续此过程...
 * 最终结果: [1, 2, 3, 4, 5, 6, 7, 8]
 *
 * TimSort 优化策略:
 * ┌─────────────────────────────────────┐
 * │ • Binary Insertion Sort (小数组)     │
 * │ • Run Detection (识别有序序列)        │
 * │ • Galloping Mode (连续选择优化)       │
 * │ • Merge Stack (维护合并不变式)        │
 * └─────────────────────────────────────┘
 *
 * Android TimSort 类的移植版本，采用"稳定的、自适应的、迭代式归并排序"。
 * 更多详细信息请参见 sort() 方法的注释。
 *
 * 这个类保持了 Java 和原始风格，以便与 Android 源代码非常接近匹配， 从而易于验证正确性。
 * 该类是包私有的。我们提供了一个简单的 Scala 包装器
 * {@link org.apache.spark.util.collection.Sorter}，可供 org.apache.spark 包使用。
 *
 * 移植的目的是泛化排序接口，以接受除简单数组（每个元素单独排序）之外的输入数据格式。
 * 例如，AppendOnlyMap 使用它来排序具有 [key, value, key, value] 形式交替元素的数组。
 * 这种泛化带来的开销很小 -- 更多信息请参见 SortDataFormat。
 *
 * 我们允许键重用以防止创建许多键对象 -- 请参见 SortDataFormat。
 *
 * @see org.apache.spark.util.collection.SortDataFormat
 * @see org.apache.spark.util.collection.Sorter
 */
class TimSort<K, Buffer> {

  /**
   * 这是将被合并的最小序列长度。较短的序列将通过调用 binarySort 来延长。
   * 如果整个数组小于此长度，则不会执行合并操作。
   *
   * 此常量应该是 2 的幂。在 Tim Peter 的 C 实现中是 64，但经验证明
   * 在此实现中 32 效果更好。如果你将此常量设置为非 2 的幂的数字，
   * 你需要更改 minRunLength 计算。
   *
   * 如果你减少此常量，必须更改 TimSort 构造函数中的 stackLen 计算，
   * 否则会有 ArrayOutOfBounds 异常的风险。有关作为被排序数组长度和
   * 最小合并序列长度函数所需的最小栈长度的讨论，请参见 listsort.txt。
   */
  private static final int MIN_MERGE = 32;

  private final SortDataFormat<K, Buffer> s;

  public TimSort(SortDataFormat<K, Buffer> sortDataFormat) {
    this.s = sortDataFormat;
  }

  /**
   * 一个稳定的、自适应的、迭代式归并排序，在部分有序数组上运行时需要的比较次数
   * 远少于 n lg(n)，同时在随机数组上运行时提供与传统归并排序相当的性能。
   * 像所有合适的归并排序一样，此排序是稳定的，运行时间为 O(n log n)（最坏情况）。
   * 在最坏情况下，此排序需要 n/2 个对象引用的临时存储空间；在最好情况下，
   * 它只需要少量常数空间。
   *
   * 此实现改编自 Tim Peters 为 Python 编写的列表排序，详细描述见：
   *
   *   http://svn.python.org/projects/python/trunk/Objects/listsort.txt
   *
   * Tim 的 C 代码可在此处找到：
   *
   *   http://svn.python.org/projects/python/trunk/Objects/listobject.c
   *
   * 底层技术在此论文中有描述（可能有更早的起源）：
   *
   *  "Optimistic Sorting and Information Theoretic Complexity"
   *  Peter McIlroy
   *  SODA (Fourth Annual ACM-SIAM Symposium on Discrete Algorithms),
   *  pp 467-474, Austin, Texas, 25-27 January 1993.
   *
   * 虽然此类的 API 仅包含静态方法，但它是（私有地）可实例化的；
   * TimSort 实例保存正在进行的排序状态，假设输入数组足够大以保证
   * 完整的 TimSort。小数组使用二分插入排序就地排序。
   *
   * @author Josh Bloch
   */

  public void sort(Buffer a, int lo, int hi, Comparator<? super K> c) {
    assert c != null;

    int nRemaining  = hi - lo;
    if (nRemaining < 2)
      return;  // Arrays of size 0 and 1 are always sorted

    // If array is small, do a "mini-TimSort" with no merges
    if (nRemaining < MIN_MERGE) {
      int initRunLen = countRunAndMakeAscending(a, lo, hi, c);
      binarySort(a, lo, hi, lo + initRunLen, c);
      return;
    }

    /**
     * March over the array once, left to right, finding natural runs,
     * extending short natural runs to minRun elements, and merging runs
     * to maintain stack invariant.
     */
    SortState sortState = new SortState(a, c, hi - lo);
    int minRun = minRunLength(nRemaining);
    do {
      // Identify next run
      int runLen = countRunAndMakeAscending(a, lo, hi, c);

      // If run is short, extend to min(minRun, nRemaining)
      if (runLen < minRun) {
        int force = nRemaining <= minRun ? nRemaining : minRun;
        binarySort(a, lo, lo + force, lo + runLen, c);
        runLen = force;
      }

      // Push run onto pending-run stack, and maybe merge
      sortState.pushRun(lo, runLen);
      sortState.mergeCollapse();

      // Advance to find next run
      lo += runLen;
      nRemaining -= runLen;
    } while (nRemaining != 0);

    // Merge all remaining runs to complete sort
    assert lo == hi;
    sortState.mergeForceCollapse();
    assert sortState.stackSize == 1;
  }

  /**
   * Sorts the specified portion of the specified array using a binary
   * insertion sort.  This is the best method for sorting small numbers
   * of elements.  It requires O(n log n) compares, but O(n^2) data
   * movement (worst case).
   *
   * If the initial part of the specified range is already sorted,
   * this method can take advantage of it: the method assumes that the
   * elements from index {@code lo}, inclusive, to {@code start},
   * exclusive are already sorted.
   *
   * @param a the array in which a range is to be sorted
   * @param lo the index of the first element in the range to be sorted
   * @param hi the index after the last element in the range to be sorted
   * @param start the index of the first element in the range that is
   *        not already known to be sorted ({@code lo <= start <= hi})
   * @param c comparator to used for the sort
   */
  @SuppressWarnings("fallthrough")
  private void binarySort(Buffer a, int lo, int hi, int start, Comparator<? super K> c) {
    assert lo <= start && start <= hi;
    if (start == lo)
      start++;

    K key0 = s.newKey();
    K key1 = s.newKey();

    Buffer pivotStore = s.allocate(1);
    for ( ; start < hi; start++) {
      s.copyElement(a, start, pivotStore, 0);
      K pivot = s.getKey(pivotStore, 0, key0);

      // Set left (and right) to the index where a[start] (pivot) belongs
      int left = lo;
      int right = start;
      assert left <= right;
      /*
       * Invariants:
       *   pivot >= all in [lo, left).
       *   pivot <  all in [right, start).
       */
      while (left < right) {
        int mid = (left + right) >>> 1;
        if (c.compare(pivot, s.getKey(a, mid, key1)) < 0)
          right = mid;
        else
          left = mid + 1;
      }
      assert left == right;

      /*
       * The invariants still hold: pivot >= all in [lo, left) and
       * pivot < all in [left, start), so pivot belongs at left.  Note
       * that if there are elements equal to pivot, left points to the
       * first slot after them -- that's why this sort is stable.
       * Slide elements over to make room for pivot.
       */
      int n = start - left;  // The number of elements to move
      // Switch is just an optimization for arraycopy in default case
      switch (n) {
        case 2:  s.copyElement(a, left + 1, a, left + 2);
        case 1:  s.copyElement(a, left, a, left + 1);
          break;
        default: s.copyRange(a, left, a, left + 1, n);
      }
      s.copyElement(pivotStore, 0, a, left);
    }
  }

  /**
   * Returns the length of the run beginning at the specified position in
   * the specified array and reverses the run if it is descending (ensuring
   * that the run will always be ascending when the method returns).
   *
   * A run is the longest ascending sequence with:
   *
   *    a[lo] <= a[lo + 1] <= a[lo + 2] <= ...
   *
   * or the longest descending sequence with:
   *
   *    a[lo] >  a[lo + 1] >  a[lo + 2] >  ...
   *
   * For its intended use in a stable mergesort, the strictness of the
   * definition of "descending" is needed so that the call can safely
   * reverse a descending sequence without violating stability.
   *
   * @param a the array in which a run is to be counted and possibly reversed
   * @param lo index of the first element in the run
   * @param hi index after the last element that may be contained in the run.
  It is required that {@code lo < hi}.
   * @param c the comparator to used for the sort
   * @return  the length of the run beginning at the specified position in
   *          the specified array
   */
  private int countRunAndMakeAscending(Buffer a, int lo, int hi, Comparator<? super K> c) {
    assert lo < hi;
    int runHi = lo + 1;
    if (runHi == hi)
      return 1;

    K key0 = s.newKey();
    K key1 = s.newKey();

    // Find end of run, and reverse range if descending
    if (c.compare(s.getKey(a, runHi++, key0), s.getKey(a, lo, key1)) < 0) { // Descending
      while (runHi < hi && c.compare(s.getKey(a, runHi, key0), s.getKey(a, runHi - 1, key1)) < 0)
        runHi++;
      reverseRange(a, lo, runHi);
    } else {                              // Ascending
      while (runHi < hi && c.compare(s.getKey(a, runHi, key0), s.getKey(a, runHi - 1, key1)) >= 0)
        runHi++;
    }

    return runHi - lo;
  }

  /**
   * Reverse the specified range of the specified array.
   *
   * @param a the array in which a range is to be reversed
   * @param lo the index of the first element in the range to be reversed
   * @param hi the index after the last element in the range to be reversed
   */
  private void reverseRange(Buffer a, int lo, int hi) {
    hi--;
    while (lo < hi) {
      s.swap(a, lo, hi);
      lo++;
      hi--;
    }
  }

  /**
   * Returns the minimum acceptable run length for an array of the specified
   * length. Natural runs shorter than this will be extended with
   * {@link #binarySort}.
   *
   * Roughly speaking, the computation is:
   *
   *  If n < MIN_MERGE, return n (it's too small to bother with fancy stuff).
   *  Else if n is an exact power of 2, return MIN_MERGE/2.
   *  Else return an int k, MIN_MERGE/2 <= k <= MIN_MERGE, such that n/k
   *   is close to, but strictly less than, an exact power of 2.
   *
   * For the rationale, see listsort.txt.
   *
   * @param n the length of the array to be sorted
   * @return the length of the minimum run to be merged
   */
  private int minRunLength(int n) {
    assert n >= 0;
    int r = 0;      // Becomes 1 if any 1 bits are shifted off
    while (n >= MIN_MERGE) {
      r |= (n & 1);
      n >>= 1;
    }
    return n + r;
  }

  private class SortState {

    /**
     * The Buffer being sorted.
     */
    private final Buffer a;

    /**
     * Length of the sort Buffer.
     */
    private final int aLength;

    /**
     * The comparator for this sort.
     */
    private final Comparator<? super K> c;

    /**
     * When we get into galloping mode, we stay there until both runs win less
     * often than MIN_GALLOP consecutive times.
     */
    private static final int  MIN_GALLOP = 7;

    /**
     * This controls when we get *into* galloping mode.  It is initialized
     * to MIN_GALLOP.  The mergeLo and mergeHi methods nudge it higher for
     * random data, and lower for highly structured data.
     */
    private int minGallop = MIN_GALLOP;

    /**
     * Maximum initial size of tmp array, which is used for merging.  The array
     * can grow to accommodate demand.
     *
     * Unlike Tim's original C version, we do not allocate this much storage
     * when sorting smaller arrays.  This change was required for performance.
     */
    private static final int INITIAL_TMP_STORAGE_LENGTH = 256;

    /**
     * Temp storage for merges.
     */
    private Buffer tmp; // Actual runtime type will be Object[], regardless of T

    /**
     * Length of the temp storage.
     */
    private int tmpLength = 0;

    /**
     * A stack of pending runs yet to be merged.  Run i starts at
     * address base[i] and extends for len[i] elements.  It's always
     * true (so long as the indices are in bounds) that:
     *
     *     runBase[i] + runLen[i] == runBase[i + 1]
     *
     * so we could cut the storage for this, but it's a minor amount,
     * and keeping all the info explicit simplifies the code.
     */
    private int stackSize = 0;  // Number of pending runs on stack
    private final int[] runBase;
    private final int[] runLen;

    /**
     * Creates a TimSort instance to maintain the state of an ongoing sort.
     *
     * @param a the array to be sorted
     * @param c the comparator to determine the order of the sort
     */
    private SortState(Buffer a, Comparator<? super K> c, int len) {
      this.aLength = len;
      this.a = a;
      this.c = c;

      // Allocate temp storage (which may be increased later if necessary)
      tmpLength = len < 2 * INITIAL_TMP_STORAGE_LENGTH ? len >>> 1 : INITIAL_TMP_STORAGE_LENGTH;
      tmp = s.allocate(tmpLength);

      /*
       * Allocate runs-to-be-merged stack (which cannot be expanded).  The
       * stack length requirements are described in listsort.txt.  The C
       * version always uses the same stack length (85), but this was
       * measured to be too expensive when sorting "mid-sized" arrays (e.g.,
       * 100 elements) in Java.  Therefore, we use smaller (but sufficiently
       * large) stack lengths for smaller arrays.  The "magic numbers" in the
       * computation below must be changed if MIN_MERGE is decreased.  See
       * the MIN_MERGE declaration above for more information.
       * The maximum value of 49 allows for an array up to length
       * Integer.MAX_VALUE-4, if array is filled by the worst case stack size
       * increasing scenario. More explanations are given in section 4 of:
       * http://envisage-project.eu/wp-content/uploads/2015/02/sorting.pdf
       */
      int stackLen = (len <    120  ?  5 :
                      len <   1542  ? 10 :
                      len < 119151  ? 24 : 49);
      runBase = new int[stackLen];
      runLen = new int[stackLen];
    }

    /**
     * Pushes the specified run onto the pending-run stack.
     *
     * @param runBase index of the first element in the run
     * @param runLen  the number of elements in the run
     */
    private void pushRun(int runBase, int runLen) {
      this.runBase[stackSize] = runBase;
      this.runLen[stackSize] = runLen;
      stackSize++;
    }

    /**
     * Examines the stack of runs waiting to be merged and merges adjacent runs
     * until the stack invariants are reestablished:
     *
     *     1. runLen[i - 3] > runLen[i - 2] + runLen[i - 1]
     *     2. runLen[i - 2] > runLen[i - 1]
     *
     * This method is called each time a new run is pushed onto the stack,
     * so the invariants are guaranteed to hold for i < stackSize upon
     * entry to the method.
     *
     * Thanks to Stijn de Gouw, Jurriaan Rot, Frank S. de Boer,
     * Richard Bubel and Reiner Hahnle, this is fixed with respect to
     * the analysis in "On the Worst-Case Complexity of TimSort" by
     * Nicolas Auger, Vincent Jug, Cyril Nicaud, and Carine Pivoteau.
     */
    private void mergeCollapse() {
      while (stackSize > 1) {
        int n = stackSize - 2;
        if (n > 0 && runLen[n-1] <= runLen[n] + runLen[n+1] ||
            n > 1 && runLen[n-2] <= runLen[n] + runLen[n-1]) {
          if (runLen[n - 1] < runLen[n + 1])
            n--;
        } else if (n < 0 || runLen[n] > runLen[n + 1]) {
          break; // Invariant is established
        }
        mergeAt(n);
      }
    }

    /**
     * Merges all runs on the stack until only one remains.  This method is
     * called once, to complete the sort.
     */
    private void mergeForceCollapse() {
      while (stackSize > 1) {
        int n = stackSize - 2;
        if (n > 0 && runLen[n - 1] < runLen[n + 1])
          n--;
        mergeAt(n);
      }
    }

    /**
     * Merges the two runs at stack indices i and i+1.  Run i must be
     * the penultimate or antepenultimate run on the stack.  In other words,
     * i must be equal to stackSize-2 or stackSize-3.
     *
     * @param i stack index of the first of the two runs to merge
     */
    private void mergeAt(int i) {
      assert stackSize >= 2;
      assert i >= 0;
      assert i == stackSize - 2 || i == stackSize - 3;

      int base1 = runBase[i];
      int len1 = runLen[i];
      int base2 = runBase[i + 1];
      int len2 = runLen[i + 1];
      assert len1 > 0 && len2 > 0;
      assert base1 + len1 == base2;

      /*
       * Record the length of the combined runs; if i is the 3rd-last
       * run now, also slide over the last run (which isn't involved
       * in this merge).  The current run (i+1) goes away in any case.
       */
      runLen[i] = len1 + len2;
      if (i == stackSize - 3) {
        runBase[i + 1] = runBase[i + 2];
        runLen[i + 1] = runLen[i + 2];
      }
      stackSize--;

      K key0 = s.newKey();

      /*
       * Find where the first element of run2 goes in run1. Prior elements
       * in run1 can be ignored (because they're already in place).
       */
      int k = gallopRight(s.getKey(a, base2, key0), a, base1, len1, 0, c);
      assert k >= 0;
      base1 += k;
      len1 -= k;
      if (len1 == 0)
        return;

      /*
       * Find where the last element of run1 goes in run2. Subsequent elements
       * in run2 can be ignored (because they're already in place).
       */
      len2 = gallopLeft(s.getKey(a, base1 + len1 - 1, key0), a, base2, len2, len2 - 1, c);
      assert len2 >= 0;
      if (len2 == 0)
        return;

      // Merge remaining runs, using tmp array with min(len1, len2) elements
      if (len1 <= len2)
        mergeLo(base1, len1, base2, len2);
      else
        mergeHi(base1, len1, base2, len2);
    }

    /**
     * Locates the position at which to insert the specified key into the
     * specified sorted range; if the range contains an element equal to key,
     * returns the index of the leftmost equal element.
     *
     * @param key the key whose insertion point to search for
     * @param a the array in which to search
     * @param base the index of the first element in the range
     * @param len the length of the range; must be > 0
     * @param hint the index at which to begin the search, 0 <= hint < n.
     *     The closer hint is to the result, the faster this method will run.
     * @param c the comparator used to order the range, and to search
     * @return the int k,  0 <= k <= n such that a[b + k - 1] < key <= a[b + k],
     *    pretending that a[b - 1] is minus infinity and a[b + n] is infinity.
     *    In other words, key belongs at index b + k; or in other words,
     *    the first k elements of a should precede key, and the last n - k
     *    should follow it.
     */
    private int gallopLeft(K key, Buffer a, int base, int len, int hint, Comparator<? super K> c) {
      assert len > 0 && hint >= 0 && hint < len;
      int lastOfs = 0;
      int ofs = 1;
      K key0 = s.newKey();

      if (c.compare(key, s.getKey(a, base + hint, key0)) > 0) {
        // Gallop right until a[base+hint+lastOfs] < key <= a[base+hint+ofs]
        int maxOfs = len - hint;
        while (ofs < maxOfs && c.compare(key, s.getKey(a, base + hint + ofs, key0)) > 0) {
          lastOfs = ofs;
          ofs = (ofs << 1) + 1;
          if (ofs <= 0)   // int overflow
            ofs = maxOfs;
        }
        if (ofs > maxOfs)
          ofs = maxOfs;

        // Make offsets relative to base
        lastOfs += hint;
        ofs += hint;
      } else { // key <= a[base + hint]
        // Gallop left until a[base+hint-ofs] < key <= a[base+hint-lastOfs]
        final int maxOfs = hint + 1;
        while (ofs < maxOfs && c.compare(key, s.getKey(a, base + hint - ofs, key0)) <= 0) {
          lastOfs = ofs;
          ofs = (ofs << 1) + 1;
          if (ofs <= 0)   // int overflow
            ofs = maxOfs;
        }
        if (ofs > maxOfs)
          ofs = maxOfs;

        // Make offsets relative to base
        int tmp = lastOfs;
        lastOfs = hint - ofs;
        ofs = hint - tmp;
      }
      assert -1 <= lastOfs && lastOfs < ofs && ofs <= len;

      /*
       * Now a[base+lastOfs] < key <= a[base+ofs], so key belongs somewhere
       * to the right of lastOfs but no farther right than ofs.  Do a binary
       * search, with invariant a[base + lastOfs - 1] < key <= a[base + ofs].
       */
      lastOfs++;
      while (lastOfs < ofs) {
        int m = lastOfs + ((ofs - lastOfs) >>> 1);

        if (c.compare(key, s.getKey(a, base + m, key0)) > 0)
          lastOfs = m + 1;  // a[base + m] < key
        else
          ofs = m;          // key <= a[base + m]
      }
      assert lastOfs == ofs;    // so a[base + ofs - 1] < key <= a[base + ofs]
      return ofs;
    }

    /**
     * Like gallopLeft, except that if the range contains an element equal to
     * key, gallopRight returns the index after the rightmost equal element.
     *
     * @param key the key whose insertion point to search for
     * @param a the array in which to search
     * @param base the index of the first element in the range
     * @param len the length of the range; must be > 0
     * @param hint the index at which to begin the search, 0 <= hint < n.
     *     The closer hint is to the result, the faster this method will run.
     * @param c the comparator used to order the range, and to search
     * @return the int k,  0 <= k <= n such that a[b + k - 1] <= key < a[b + k]
     */
    private int gallopRight(K key, Buffer a, int base, int len, int hint, Comparator<? super K> c) {
      assert len > 0 && hint >= 0 && hint < len;

      int ofs = 1;
      int lastOfs = 0;
      K key1 = s.newKey();

      if (c.compare(key, s.getKey(a, base + hint, key1)) < 0) {
        // Gallop left until a[b+hint - ofs] <= key < a[b+hint - lastOfs]
        int maxOfs = hint + 1;
        while (ofs < maxOfs && c.compare(key, s.getKey(a, base + hint - ofs, key1)) < 0) {
          lastOfs = ofs;
          ofs = (ofs << 1) + 1;
          if (ofs <= 0)   // int overflow
            ofs = maxOfs;
        }
        if (ofs > maxOfs)
          ofs = maxOfs;

        // Make offsets relative to b
        int tmp = lastOfs;
        lastOfs = hint - ofs;
        ofs = hint - tmp;
      } else { // a[b + hint] <= key
        // Gallop right until a[b+hint + lastOfs] <= key < a[b+hint + ofs]
        int maxOfs = len - hint;
        while (ofs < maxOfs && c.compare(key, s.getKey(a, base + hint + ofs, key1)) >= 0) {
          lastOfs = ofs;
          ofs = (ofs << 1) + 1;
          if (ofs <= 0)   // int overflow
            ofs = maxOfs;
        }
        if (ofs > maxOfs)
          ofs = maxOfs;

        // Make offsets relative to b
        lastOfs += hint;
        ofs += hint;
      }
      assert -1 <= lastOfs && lastOfs < ofs && ofs <= len;

      /*
       * Now a[b + lastOfs] <= key < a[b + ofs], so key belongs somewhere to
       * the right of lastOfs but no farther right than ofs.  Do a binary
       * search, with invariant a[b + lastOfs - 1] <= key < a[b + ofs].
       */
      lastOfs++;
      while (lastOfs < ofs) {
        int m = lastOfs + ((ofs - lastOfs) >>> 1);

        if (c.compare(key, s.getKey(a, base + m, key1)) < 0)
          ofs = m;          // key < a[b + m]
        else
          lastOfs = m + 1;  // a[b + m] <= key
      }
      assert lastOfs == ofs;    // so a[b + ofs - 1] <= key < a[b + ofs]
      return ofs;
    }

    /**
     * Merges two adjacent runs in place, in a stable fashion.  The first
     * element of the first run must be greater than the first element of the
     * second run (a[base1] > a[base2]), and the last element of the first run
     * (a[base1 + len1-1]) must be greater than all elements of the second run.
     *
     * For performance, this method should be called only when len1 <= len2;
     * its twin, mergeHi should be called if len1 >= len2.  (Either method
     * may be called if len1 == len2.)
     *
     * @param base1 index of first element in first run to be merged
     * @param len1  length of first run to be merged (must be > 0)
     * @param base2 index of first element in second run to be merged
     *        (must be aBase + aLen)
     * @param len2  length of second run to be merged (must be > 0)
     */
    private void mergeLo(int base1, int len1, int base2, int len2) {
      assert len1 > 0 && len2 > 0 && base1 + len1 == base2;

      // Copy first run into temp array
      Buffer a = this.a; // For performance
      Buffer tmp = ensureCapacity(len1);
      s.copyRange(a, base1, tmp, 0, len1);

      int cursor1 = 0;       // Indexes into tmp array
      int cursor2 = base2;   // Indexes int a
      int dest = base1;      // Indexes int a

      // Move first element of second run and deal with degenerate cases
      s.copyElement(a, cursor2++, a, dest++);
      if (--len2 == 0) {
        s.copyRange(tmp, cursor1, a, dest, len1);
        return;
      }
      if (len1 == 1) {
        s.copyRange(a, cursor2, a, dest, len2);
        s.copyElement(tmp, cursor1, a, dest + len2); // Last elt of run 1 to end of merge
        return;
      }

      K key0 = s.newKey();
      K key1 = s.newKey();

      Comparator<? super K> c = this.c;  // Use local variable for performance
      int minGallop = this.minGallop;    //  "    "       "     "      "
      outer:
      while (true) {
        int count1 = 0; // Number of times in a row that first run won
        int count2 = 0; // Number of times in a row that second run won

        /*
         * Do the straightforward thing until (if ever) one run starts
         * winning consistently.
         */
        do {
          assert len1 > 1 && len2 > 0;
          if (c.compare(s.getKey(a, cursor2, key0), s.getKey(tmp, cursor1, key1)) < 0) {
            s.copyElement(a, cursor2++, a, dest++);
            count2++;
            count1 = 0;
            if (--len2 == 0)
              break outer;
          } else {
            s.copyElement(tmp, cursor1++, a, dest++);
            count1++;
            count2 = 0;
            if (--len1 == 1)
              break outer;
          }
        } while ((count1 | count2) < minGallop);

        /*
         * One run is winning so consistently that galloping may be a
         * huge win. So try that, and continue galloping until (if ever)
         * neither run appears to be winning consistently anymore.
         */
        do {
          assert len1 > 1 && len2 > 0;
          count1 = gallopRight(s.getKey(a, cursor2, key0), tmp, cursor1, len1, 0, c);
          if (count1 != 0) {
            s.copyRange(tmp, cursor1, a, dest, count1);
            dest += count1;
            cursor1 += count1;
            len1 -= count1;
            if (len1 <= 1) // len1 == 1 || len1 == 0
              break outer;
          }
          s.copyElement(a, cursor2++, a, dest++);
          if (--len2 == 0)
            break outer;

          count2 = gallopLeft(s.getKey(tmp, cursor1, key0), a, cursor2, len2, 0, c);
          if (count2 != 0) {
            s.copyRange(a, cursor2, a, dest, count2);
            dest += count2;
            cursor2 += count2;
            len2 -= count2;
            if (len2 == 0)
              break outer;
          }
          s.copyElement(tmp, cursor1++, a, dest++);
          if (--len1 == 1)
            break outer;
          minGallop--;
        } while (count1 >= MIN_GALLOP | count2 >= MIN_GALLOP);
        if (minGallop < 0)
          minGallop = 0;
        minGallop += 2;  // Penalize for leaving gallop mode
      }  // End of "outer" loop
      this.minGallop = minGallop < 1 ? 1 : minGallop;  // Write back to field

      if (len1 == 1) {
        assert len2 > 0;
        s.copyRange(a, cursor2, a, dest, len2);
        s.copyElement(tmp, cursor1, a, dest + len2); //  Last elt of run 1 to end of merge
      } else if (len1 == 0) {
        throw new IllegalArgumentException(
            "Comparison method violates its general contract!");
      } else {
        assert len2 == 0;
        assert len1 > 1;
        s.copyRange(tmp, cursor1, a, dest, len1);
      }
    }

    /**
     * Like mergeLo, except that this method should be called only if
     * len1 >= len2; mergeLo should be called if len1 <= len2.  (Either method
     * may be called if len1 == len2.)
     *
     * @param base1 index of first element in first run to be merged
     * @param len1  length of first run to be merged (must be > 0)
     * @param base2 index of first element in second run to be merged
     *        (must be aBase + aLen)
     * @param len2  length of second run to be merged (must be > 0)
     */
    private void mergeHi(int base1, int len1, int base2, int len2) {
      assert len1 > 0 && len2 > 0 && base1 + len1 == base2;

      // Copy second run into temp array
      Buffer a = this.a; // For performance
      Buffer tmp = ensureCapacity(len2);
      s.copyRange(a, base2, tmp, 0, len2);

      int cursor1 = base1 + len1 - 1;  // Indexes into a
      int cursor2 = len2 - 1;          // Indexes into tmp array
      int dest = base2 + len2 - 1;     // Indexes into a

      K key0 = s.newKey();
      K key1 = s.newKey();

      // Move last element of first run and deal with degenerate cases
      s.copyElement(a, cursor1--, a, dest--);
      if (--len1 == 0) {
        s.copyRange(tmp, 0, a, dest - (len2 - 1), len2);
        return;
      }
      if (len2 == 1) {
        dest -= len1;
        cursor1 -= len1;
        s.copyRange(a, cursor1 + 1, a, dest + 1, len1);
        s.copyElement(tmp, cursor2, a, dest);
        return;
      }

      Comparator<? super K> c = this.c;  // Use local variable for performance
      int minGallop = this.minGallop;    //  "    "       "     "      "
      outer:
      while (true) {
        int count1 = 0; // Number of times in a row that first run won
        int count2 = 0; // Number of times in a row that second run won

        /*
         * Do the straightforward thing until (if ever) one run
         * appears to win consistently.
         */
        do {
          assert len1 > 0 && len2 > 1;
          if (c.compare(s.getKey(tmp, cursor2, key0), s.getKey(a, cursor1, key1)) < 0) {
            s.copyElement(a, cursor1--, a, dest--);
            count1++;
            count2 = 0;
            if (--len1 == 0)
              break outer;
          } else {
            s.copyElement(tmp, cursor2--, a, dest--);
            count2++;
            count1 = 0;
            if (--len2 == 1)
              break outer;
          }
        } while ((count1 | count2) < minGallop);

        /*
         * One run is winning so consistently that galloping may be a
         * huge win. So try that, and continue galloping until (if ever)
         * neither run appears to be winning consistently anymore.
         */
        do {
          assert len1 > 0 && len2 > 1;
          count1 = len1 - gallopRight(s.getKey(tmp, cursor2, key0), a, base1, len1, len1 - 1, c);
          if (count1 != 0) {
            dest -= count1;
            cursor1 -= count1;
            len1 -= count1;
            s.copyRange(a, cursor1 + 1, a, dest + 1, count1);
            if (len1 == 0)
              break outer;
          }
          s.copyElement(tmp, cursor2--, a, dest--);
          if (--len2 == 1)
            break outer;

          count2 = len2 - gallopLeft(s.getKey(a, cursor1, key0), tmp, 0, len2, len2 - 1, c);
          if (count2 != 0) {
            dest -= count2;
            cursor2 -= count2;
            len2 -= count2;
            s.copyRange(tmp, cursor2 + 1, a, dest + 1, count2);
            if (len2 <= 1)  // len2 == 1 || len2 == 0
              break outer;
          }
          s.copyElement(a, cursor1--, a, dest--);
          if (--len1 == 0)
            break outer;
          minGallop--;
        } while (count1 >= MIN_GALLOP | count2 >= MIN_GALLOP);
        if (minGallop < 0)
          minGallop = 0;
        minGallop += 2;  // Penalize for leaving gallop mode
      }  // End of "outer" loop
      this.minGallop = minGallop < 1 ? 1 : minGallop;  // Write back to field

      if (len2 == 1) {
        assert len1 > 0;
        dest -= len1;
        cursor1 -= len1;
        s.copyRange(a, cursor1 + 1, a, dest + 1, len1);
        s.copyElement(tmp, cursor2, a, dest); // Move first elt of run2 to front of merge
      } else if (len2 == 0) {
        throw new IllegalArgumentException(
            "Comparison method violates its general contract!");
      } else {
        assert len1 == 0;
        assert len2 > 0;
        s.copyRange(tmp, 0, a, dest - (len2 - 1), len2);
      }
    }

    /**
     * Ensures that the external array tmp has at least the specified
     * number of elements, increasing its size if necessary.  The size
     * increases exponentially to ensure amortized linear time complexity.
     *
     * @param minCapacity the minimum required capacity of the tmp array
     * @return tmp, whether or not it grew
     */
    private Buffer ensureCapacity(int minCapacity) {
      if (tmpLength < minCapacity) {
        // Compute smallest power of 2 > minCapacity
        int newSize = minCapacity;
        newSize |= newSize >> 1;
        newSize |= newSize >> 2;
        newSize |= newSize >> 4;
        newSize |= newSize >> 8;
        newSize |= newSize >> 16;
        newSize++;

        if (newSize < 0) // Not bloody likely!
          newSize = minCapacity;
        else
          newSize = Math.min(newSize, aLength >>> 1);

        tmp = s.allocate(newSize);
        tmpLength = newSize;
      }
      return tmp;
    }
  }
}
