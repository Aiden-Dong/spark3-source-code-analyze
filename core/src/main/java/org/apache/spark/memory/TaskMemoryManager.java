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

package org.apache.spark.memory;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.Utils;

/**
 * Manages the memory allocated by an individual task.
 * <p>
 * Most of the complexity in this class deals with encoding of off-heap addresses into 64-bit longs.
 * In off-heap mode, memory can be directly addressed with 64-bit longs. In on-heap mode, memory is
 * addressed by the combination of a base Object reference and a 64-bit offset within that object.
 * This is a problem when we want to store pointers to data structures inside of other structures,
 * such as record pointers inside hashmaps or sorting buffers. Even if we decided to use 128 bits
 * to address memory, we can't just store the address of the base object since it's not guaranteed
 * to remain stable as the heap gets reorganized due to GC.
 * <p>
 * Instead, we use the following approach to encode record pointers in 64-bit longs: for off-heap
 * mode, just store the raw address, and for on-heap mode use the upper 13 bits of the address to
 * store a "page number" and the lower 51 bits to store an offset within this page. These page
 * numbers are used to index into a "page table" array inside of the MemoryManager in order to
 * retrieve the base object.
 * <p>
 * This allows us to address 8192 pages. In on-heap mode, the maximum page size is limited by the
 * maximum size of a long[] array, allowing us to address 8192 * (2^31 - 1) * 8 bytes, which is
 * approximately 140 terabytes of memory.
 */
public class TaskMemoryManager {

  private static final Logger logger = LoggerFactory.getLogger(TaskMemoryManager.class);

  /** The number of bits used to address the page table. */
  private static final int PAGE_NUMBER_BITS = 13;

  /** The number of bits used to encode offsets in data pages. */
  @VisibleForTesting
  static final int OFFSET_BITS = 64 - PAGE_NUMBER_BITS;  // 51

  /** The number of entries in the page table. */
  private static final int PAGE_TABLE_SIZE = 1 << PAGE_NUMBER_BITS;

  /**
   * Maximum supported data page size (in bytes). In principle, the maximum addressable page size is
   * (1L &lt;&lt; OFFSET_BITS) bytes, which is 2+ petabytes. However, the on-heap allocator's
   * maximum page size is limited by the maximum amount of data that can be stored in a long[]
   * array, which is (2^31 - 1) * 8 bytes (or about 17 gigabytes). Therefore, we cap this at 17
   * gigabytes.
   */
  public static final long MAXIMUM_PAGE_SIZE_BYTES = ((1L << 31) - 1) * 8L;

  /** Bit mask for the lower 51 bits of a long. */
  private static final long MASK_LONG_LOWER_51_BITS = 0x7FFFFFFFFFFFFL;

  /**
   * Similar to an operating system's page table, this array maps page numbers into base object
   * pointers, allowing us to translate between the hashtable's internal 64-bit address
   * representation and the baseObject+offset representation which we use to support both on- and
   * off-heap addresses. When using an off-heap allocator, every entry in this map will be `null`.
   * When using an on-heap allocator, the entries in this map will point to pages' base objects.
   * Entries are added to this map as new data pages are allocated.
   */
  private final MemoryBlock[] pageTable = new MemoryBlock[PAGE_TABLE_SIZE];

  /**
   * Bitmap for tracking free pages.
   */
  private final BitSet allocatedPages = new BitSet(PAGE_TABLE_SIZE);

  private final MemoryManager memoryManager;

  private final long taskAttemptId;

  /**
   * 跟踪我们是在堆上还是在堆外。
   *   对于堆外情况，我们会短路大部分这些方法，而不进行任何掩码或查找。
   *   由于这个分支应该被 JIT 良好预测，因此这额外的间接层/抽象希望不会太昂贵。
   */
  final MemoryMode tungstenMemoryMode;

  /**
   * Tracks spillable memory consumers.
   */
  @GuardedBy("this")
  private final HashSet<MemoryConsumer> consumers;

  /**
   * The amount of memory that is acquired but not used.
   */
  private volatile long acquiredButNotUsed = 0L;

  /**
   * Construct a new TaskMemoryManager.
   */
  public TaskMemoryManager(MemoryManager memoryManager, long taskAttemptId) {
    this.tungstenMemoryMode = memoryManager.tungstenMemoryMode();
    this.memoryManager = memoryManager;
    this.taskAttemptId = taskAttemptId;
    this.consumers = new HashSet<>();
  }

  /**
   * 为消费者获取 N 字节的内存。如果内存不足，它将调用消费者的 spill() 以释放更多内存。
   *
   * @return 成功授予的字节数（<= N）。
   */
  public long acquireExecutionMemory(long required, MemoryConsumer requestingConsumer) {
    assert(required >= 0);
    assert(requestingConsumer != null);
    MemoryMode mode = requestingConsumer.getMode();
    // 如果我们在堆外分配 Tungsten 页面，并在这里收到请求以分配堆内内存，那么溢出可能没有意义，因为这只会释放堆外内存。
    // 然而，这个情况可能会发生变化，因此现在进行这个优化可能风险较大，以防我们在进行更改时忘记撤销它。
    synchronized (this) {
      long got = memoryManager.acquireExecutionMemory(required, taskAttemptId, mode);

      // 首先尝试从其他消费者那里释放内存，然后我们可以减少溢出的频率，避免产生过多的溢出文件。
      if (got < required) {
        logger.debug("Task {} need to spill {} for {}", taskAttemptId,
          Utils.bytesToString(required - got), requestingConsumer);
        // 我们需要调用消费者的 spill() 以释放更多内存。我们希望优化以下两点：
        // * 尽量减少溢出调用的次数，以减少溢出文件的数量并避免生成小的溢出文件。
        // * 避免溢出比所需更多的数据——如果我们只需要一点额外内存，可能不希望溢出尽可能多的数据。
        //   许多消费者溢出的数据量超过了请求的量，所以我们可以在决策时考虑这一点。
        // 我们使用一种启发式方法，选择至少拥有 `required` 字节内存的最小内存消费者，以平衡这些因素。
        //   如果请求较少且较大，该方法可能表现良好，但如果有很多小请求，则可能导致许多小溢出。

        // 根据内存使用情况构建消费者的映射，以优先考虑溢出。为当前消费者（如果存在）分配一个名义内存使用量为 0，使其始终在优先级顺序中排在最后。该映射将包括所有以前获得过内存的消费者。
        TreeMap<Long, List<MemoryConsumer>> sortedConsumers = new TreeMap<>();
        for (MemoryConsumer c: consumers) {
          if (c.getUsed() > 0 && c.getMode() == mode) {
            long key = c == requestingConsumer ? 0 : c.getUsed();
            List<MemoryConsumer> list = sortedConsumers.computeIfAbsent(key, k -> new ArrayList<>(1));
            list.add(c);
          }
        }
        // Iteratively spill consumers until we've freed enough memory or run out of consumers.
        while (got < required && !sortedConsumers.isEmpty()) {
          // Get the consumer using the least memory more than the remaining required memory.
          Map.Entry<Long, List<MemoryConsumer>> currentEntry =
            sortedConsumers.ceilingEntry(required - got);
          // No consumer has enough memory on its own, start with spilling the biggest consumer.
          if (currentEntry == null) {
            currentEntry = sortedConsumers.lastEntry();
          }
          List<MemoryConsumer> cList = currentEntry.getValue();
          got += trySpillAndAcquire(requestingConsumer, required - got, cList, cList.size() - 1);
          if (cList.isEmpty()) {
            sortedConsumers.remove(currentEntry.getKey());
          }
        }
      }

      consumers.add(requestingConsumer);
      logger.debug("Task {} acquired {} for {}", taskAttemptId, Utils.bytesToString(got),
              requestingConsumer);
      return got;
    }
  }

  /**
   * Try to acquire as much memory as possible from `cList[idx]`, up to `requested` bytes by
   * spilling and then acquiring the freed memory. If no more memory can be spilled from
   * `cList[idx]`, remove it from the list.
   *
   * @return number of bytes acquired (<= requested)
   * @throws RuntimeException if task is interrupted
   * @throws SparkOutOfMemoryError if an IOException occurs during spilling
   */
  private long trySpillAndAcquire(
      MemoryConsumer requestingConsumer,
      long requested,
      List<MemoryConsumer> cList,
      int idx) {
    MemoryMode mode = requestingConsumer.getMode();
    MemoryConsumer consumerToSpill = cList.get(idx);
    logger.debug("Task {} try to spill {} from {} for {}", taskAttemptId,
      Utils.bytesToString(requested), consumerToSpill, requestingConsumer);
    try {
      long released = consumerToSpill.spill(requested, requestingConsumer);
      if (released > 0) {
        logger.debug("Task {} spilled {} of requested {} from {} for {}", taskAttemptId,
          Utils.bytesToString(released), Utils.bytesToString(requested), consumerToSpill,
          requestingConsumer);

        // When our spill handler releases memory, `ExecutionMemoryPool#releaseMemory()` will
        // immediately notify other tasks that memory has been freed, and they may acquire the
        // newly-freed memory before we have a chance to do so (SPARK-35486). Therefore we may
        // not be able to acquire all the memory that was just spilled. In that case, we will
        // try again in the next loop iteration.
        return memoryManager.acquireExecutionMemory(requested, taskAttemptId, mode);
      } else {
        cList.remove(idx);
        return 0;
      }
    } catch (ClosedByInterruptException e) {
      // This called by user to kill a task (e.g: speculative task).
      logger.error("error while calling spill() on " + consumerToSpill, e);
      throw new RuntimeException(e.getMessage());
    } catch (IOException e) {
      logger.error("error while calling spill() on " + consumerToSpill, e);
      // checkstyle.off: RegexpSinglelineJava
      throw new SparkOutOfMemoryError("error while calling spill() on " + consumerToSpill + " : "
        + e.getMessage());
      // checkstyle.on: RegexpSinglelineJava
    }
  }

  /**
   * Release N bytes of execution memory for a MemoryConsumer.
   */
  public void releaseExecutionMemory(long size, MemoryConsumer consumer) {
    logger.debug("Task {} release {} from {}", taskAttemptId, Utils.bytesToString(size), consumer);
    memoryManager.releaseExecutionMemory(size, taskAttemptId, consumer.getMode());
  }

  /**
   * Dump the memory usage of all consumers.
   */
  public void showMemoryUsage() {
    logger.info("Memory used in task " + taskAttemptId);
    synchronized (this) {
      long memoryAccountedForByConsumers = 0;
      for (MemoryConsumer c: consumers) {
        long totalMemUsage = c.getUsed();
        memoryAccountedForByConsumers += totalMemUsage;
        if (totalMemUsage > 0) {
          logger.info("Acquired by " + c + ": " + Utils.bytesToString(totalMemUsage));
        }
      }
      long memoryNotAccountedFor =
        memoryManager.getExecutionMemoryUsageForTask(taskAttemptId) - memoryAccountedForByConsumers;
      logger.info(
        "{} bytes of memory were used by task {} but are not associated with specific consumers",
        memoryNotAccountedFor, taskAttemptId);
      logger.info(
        "{} bytes of memory are used for execution and {} bytes of memory are used for storage",
        memoryManager.executionMemoryUsed(), memoryManager.storageMemoryUsed());
    }
  }

  /**
   * Return the page size in bytes.
   */
  public long pageSizeBytes() {
    return memoryManager.pageSizeBytes();
  }

  /**
   * 分配一块内存，将在 MemoryManager 的页表中进行跟踪；这用于分配将在操作符之间共享的大块 Tungsten 内存。
   *
   * 如果没有足够的内存来分配页面，则返回 `null`。可能返回的页面包含的字节数少于请求的数量，因此调用者应验证返回页面的大小。
   *
   * @throws TooLargePageException
   */
  public MemoryBlock allocatePage(long size, MemoryConsumer consumer) {
    assert(consumer != null);
    assert(consumer.getMode() == tungstenMemoryMode);

    if (size > MAXIMUM_PAGE_SIZE_BYTES) {
      throw new TooLargePageException(size);
    }

    long acquired = acquireExecutionMemory(size, consumer);

    if (acquired <= 0) {
      return null;
    }

    final int pageNumber;
    synchronized (this) {
      pageNumber = allocatedPages.nextClearBit(0);
      if (pageNumber >= PAGE_TABLE_SIZE) {
        releaseExecutionMemory(acquired, consumer);
        throw new IllegalStateException(
          "Have already allocated a maximum of " + PAGE_TABLE_SIZE + " pages");
      }
      allocatedPages.set(pageNumber);
    }
    MemoryBlock page = null;
    try {
      // 分配内存资源
      page = memoryManager.tungstenMemoryAllocator().allocate(acquired);
    } catch (OutOfMemoryError e) {
      logger.warn("Failed to allocate a page ({} bytes), try again.", acquired);
      // there is no enough memory actually, it means the actual free memory is smaller than
      // MemoryManager thought, we should keep the acquired memory.
      synchronized (this) {
        acquiredButNotUsed += acquired;
        allocatedPages.clear(pageNumber);
      }
      // this could trigger spilling to free some pages.
      return allocatePage(size, consumer);
    }
    page.pageNumber = pageNumber;
    pageTable[pageNumber] = page;
    if (logger.isTraceEnabled()) {
      logger.trace("Allocate page number {} ({} bytes)", pageNumber, acquired);
    }
    return page;
  }

  /**
   * Free a block of memory allocated via {@link TaskMemoryManager#allocatePage}.
   */
  public void freePage(MemoryBlock page, MemoryConsumer consumer) {
    assert (page.pageNumber != MemoryBlock.NO_PAGE_NUMBER) :
      "Called freePage() on memory that wasn't allocated with allocatePage()";
    assert (page.pageNumber != MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER) :
      "Called freePage() on a memory block that has already been freed";
    assert (page.pageNumber != MemoryBlock.FREED_IN_TMM_PAGE_NUMBER) :
            "Called freePage() on a memory block that has already been freed";
    assert(allocatedPages.get(page.pageNumber));
    pageTable[page.pageNumber] = null;
    synchronized (this) {
      allocatedPages.clear(page.pageNumber);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Freed page number {} ({} bytes)", page.pageNumber, page.size());
    }
    long pageSize = page.size();
    // Clear the page number before passing the block to the MemoryAllocator's free().
    // Doing this allows the MemoryAllocator to detect when a TaskMemoryManager-managed
    // page has been inappropriately directly freed without calling TMM.freePage().
    page.pageNumber = MemoryBlock.FREED_IN_TMM_PAGE_NUMBER;
    memoryManager.tungstenMemoryAllocator().free(page);
    releaseExecutionMemory(pageSize, consumer);
  }

  /**
   * Given a memory page and offset within that page, encode this address into a 64-bit long.
   * This address will remain valid as long as the corresponding page has not been freed.
   *
   * @param page a data page allocated by {@link TaskMemoryManager#allocatePage}/
   * @param offsetInPage an offset in this page which incorporates the base offset. In other words,
   *                     this should be the value that you would pass as the base offset into an
   *                     UNSAFE call (e.g. page.baseOffset() + something).
   * @return an encoded page address.
   */
  public long encodePageNumberAndOffset(MemoryBlock page, long offsetInPage) {
    if (tungstenMemoryMode == MemoryMode.OFF_HEAP) {
      // In off-heap mode, an offset is an absolute address that may require a full 64 bits to
      // encode. Due to our page size limitation, though, we can convert this into an offset that's
      // relative to the page's base offset; this relative offset will fit in 51 bits.
      offsetInPage -= page.getBaseOffset();
    }
    return encodePageNumberAndOffset(page.pageNumber, offsetInPage);
  }

  @VisibleForTesting
  public static long encodePageNumberAndOffset(int pageNumber, long offsetInPage) {
    assert (pageNumber >= 0) : "encodePageNumberAndOffset called with invalid page";
    return (((long) pageNumber) << OFFSET_BITS) | (offsetInPage & MASK_LONG_LOWER_51_BITS);
  }

  @VisibleForTesting
  public static int decodePageNumber(long pagePlusOffsetAddress) {
    return (int) (pagePlusOffsetAddress >>> OFFSET_BITS);
  }

  private static long decodeOffset(long pagePlusOffsetAddress) {
    return (pagePlusOffsetAddress & MASK_LONG_LOWER_51_BITS);
  }

  /**
   * Get the page associated with an address encoded by
   * {@link TaskMemoryManager#encodePageNumberAndOffset(MemoryBlock, long)}
   */
  public Object getPage(long pagePlusOffsetAddress) {
    if (tungstenMemoryMode == MemoryMode.ON_HEAP) {
      final int pageNumber = decodePageNumber(pagePlusOffsetAddress);
      assert (pageNumber >= 0 && pageNumber < PAGE_TABLE_SIZE);
      final MemoryBlock page = pageTable[pageNumber];
      assert (page != null);
      assert (page.getBaseObject() != null);
      return page.getBaseObject();
    } else {
      return null;
    }
  }

  /**
   * Get the offset associated with an address encoded by
   * {@link TaskMemoryManager#encodePageNumberAndOffset(MemoryBlock, long)}
   */
  public long getOffsetInPage(long pagePlusOffsetAddress) {
    final long offsetInPage = decodeOffset(pagePlusOffsetAddress);
    if (tungstenMemoryMode == MemoryMode.ON_HEAP) {
      return offsetInPage;
    } else {
      // In off-heap mode, an offset is an absolute address. In encodePageNumberAndOffset, we
      // converted the absolute address into a relative address. Here, we invert that operation:
      final int pageNumber = decodePageNumber(pagePlusOffsetAddress);
      assert (pageNumber >= 0 && pageNumber < PAGE_TABLE_SIZE);
      final MemoryBlock page = pageTable[pageNumber];
      assert (page != null);
      return page.getBaseOffset() + offsetInPage;
    }
  }

  /**
   * Clean up all allocated memory and pages. Returns the number of bytes freed. A non-zero return
   * value can be used to detect memory leaks.
   */
  public long cleanUpAllAllocatedMemory() {
    synchronized (this) {
      for (MemoryConsumer c: consumers) {
        if (c != null && c.getUsed() > 0) {
          // In case of failed task, it's normal to see leaked memory
          logger.debug("unreleased " + Utils.bytesToString(c.getUsed()) + " memory from " + c);
        }
      }
      consumers.clear();

      for (MemoryBlock page : pageTable) {
        if (page != null) {
          logger.debug("unreleased page: " + page + " in task " + taskAttemptId);
          page.pageNumber = MemoryBlock.FREED_IN_TMM_PAGE_NUMBER;
          memoryManager.tungstenMemoryAllocator().free(page);
        }
      }
      Arrays.fill(pageTable, null);
    }

    // release the memory that is not used by any consumer (acquired for pages in tungsten mode).
    memoryManager.releaseExecutionMemory(acquiredButNotUsed, taskAttemptId, tungstenMemoryMode);

    return memoryManager.releaseAllExecutionMemoryForTask(taskAttemptId);
  }

  /**
   * Returns the memory consumption, in bytes, for the current task.
   */
  public long getMemoryConsumptionForThisTask() {
    return memoryManager.getExecutionMemoryUsageForTask(taskAttemptId);
  }

  /**
   * Returns Tungsten memory mode
   */
  public MemoryMode getTungstenMemoryMode() {
    return tungstenMemoryMode;
  }
}
