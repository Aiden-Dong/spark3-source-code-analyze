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
 * TaskMemoryManager 是Spark中负责管理单个任务内存分配的核心类。以下是其主要功能和设计要点：
 *
 * ### 核心职责
 * 1. 任务级内存管理 - 管理单个Task的内存分配和释放
 * 2. 页表管理 - 维护内存页的映射表，支持堆内和堆外内存
 * 3. 内存溢出协调 - 当内存不足时协调各个消费者进行溢出操作
 *
 * 1. 地址编码机制
 * • 使用64位long来编码内存地址
 * • 高13位存储页号(page number)，低51位存储页内偏移
 * • 支持最多8192个页面，每页最大17GB
 */
public class TaskMemoryManager {

  private static final Logger logger = LoggerFactory.getLogger(TaskMemoryManager.class);

  private static final int PAGE_NUMBER_BITS = 13;                            // 定义页号占用的位数(13位)，决定了最多可以管理8192个页面
  @VisibleForTesting
  static final int OFFSET_BITS = 64 - PAGE_NUMBER_BITS;                       // 页内偏移占用的位数(51位)，决定了单个页面的最大寻址空间
  private static final int PAGE_TABLE_SIZE = 1 << PAGE_NUMBER_BITS;           // 页表大小(8192)，即最多可管理的页面数量

  public static final long MAXIMUM_PAGE_SIZE_BYTES = ((1L << 31) - 1) * 8L;   // 单个页面的最大字节数(约17GB)，受限于Java数组的最大长度
  private static final long MASK_LONG_LOWER_51_BITS = 0x7FFFFFFFFFFFFL;       // 用于提取64位地址中低51位偏移量的位掩码

  // 页表数组，类似操作系统的页表
  //• 索引为页号，值为MemoryBlock对象
  //• 堆外模式时所有条目为null
  //• 堆内模式时指向页面的基础对象
  private final MemoryBlock[] pageTable = new MemoryBlock[PAGE_TABLE_SIZE];

  // 位图，跟踪哪些页面已被分配
  //• 每一位对应一个页面槽位
  //• 1表示已分配，0表示空闲
  private final BitSet allocatedPages = new BitSet(PAGE_TABLE_SIZE);

  // 引用全局内存管理器
  // 负责实际的内存分配/释放
  // 协调不同任务间的内存使用
  private final MemoryManager memoryManager;

  // 任务尝试ID，用于标识当前任务
  private final long taskAttemptId;

  // 作用: 当前使用的内存模式
  //• ON_HEAP: 堆内内存模式
  //• OFF_HEAP: 堆外内存模式
  //• 影响地址编码和内存分配策略
  final MemoryMode tungstenMemoryMode;

  // 作用: 跟踪所有内存消费者
  //• 存储当前任务中所有申请过内存的消费者
  //• 用于内存不足时的溢出决策
  //• 线程安全，需要同步访问
  @GuardedBy("this")
  private final HashSet<MemoryConsumer> consumers;

  // 跟踪已获取但未使用的内存量
  private volatile long acquiredButNotUsed = 0L;

  public TaskMemoryManager(MemoryManager memoryManager, long taskAttemptId) {
    this.tungstenMemoryMode = memoryManager.tungstenMemoryMode();
    this.memoryManager = memoryManager;
    this.taskAttemptId = taskAttemptId;
    this.consumers = new HashSet<>();
  }

  /**
   ***************************************************************************************
   * 为requestingConsumer申请指定数量的内存(required), 如果当前资源不足， 则为其他消费者调用 spill() 释放资源给他
   * @return 成功授予的字节数（<= N）。
   * **************************************************************************************
   */
  public long acquireExecutionMemory(long required, MemoryConsumer requestingConsumer) {
    assert(required >= 0);
    assert(requestingConsumer != null);

    // TODO-1: 获取消费者的内存模式
    MemoryMode mode = requestingConsumer.getMode();

    synchronized (this) {

      // TODO-2 : 申请执行内存
      long got = memoryManager.acquireExecutionMemory(required, taskAttemptId, mode);

      // TODO-3 : 如果申请到的内存大小小于请求的内存量(内存不足)
      //  首先尝试从其他消费者那里释放内存，然后我们可以减少溢出的频率，避免产生过多的溢出文件。
      if (got < required) {
        logger.debug("Task {} need to spill {} for {}", taskAttemptId, Utils.bytesToString(required - got), requestingConsumer);

        // 获取其他的内存消费者
        TreeMap<Long, List<MemoryConsumer>> sortedConsumers = new TreeMap<>();

        // 构建消费者优先级映射
        for (MemoryConsumer c: consumers) {
          if (c.getUsed() > 0 && c.getMode() == mode) {
            long key = c == requestingConsumer ? 0 : c.getUsed();
            List<MemoryConsumer> list = sortedConsumers.computeIfAbsent(key, k -> new ArrayList<>(1));
            list.add(c);
          }
        }

        while (got < required && !sortedConsumers.isEmpty()) {
          // 返回键值大于或等于给定键的最小条目
          Map.Entry<Long, List<MemoryConsumer>> currentEntry = sortedConsumers.ceilingEntry(required - got);
          if (currentEntry == null) {
            currentEntry = sortedConsumers.lastEntry();
          }
          List<MemoryConsumer> cList = currentEntry.getValue();
          // 释放一个资源差不多打的MemoryConsumer
          got += trySpillAndAcquire(requestingConsumer, required - got, cList, cList.size() - 1);
          if (cList.isEmpty()) {
            sortedConsumers.remove(currentEntry.getKey());
          }
        }
      }

      consumers.add(requestingConsumer);
      logger.debug("Task {} acquired {} for {}", taskAttemptId, Utils.bytesToString(got), requestingConsumer);

      return got;
    }
  }

  /**
   * 通过调用内存消费者的 spill 方法，释放资源，并在申请指定数量的内存资源
   */
  private long trySpillAndAcquire(MemoryConsumer requestingConsumer, long requested, List<MemoryConsumer> cList, int idx) {

    MemoryMode mode = requestingConsumer.getMode();   // 获取内存模式
    MemoryConsumer consumerToSpill = cList.get(idx);  // 获取待释放资源的

    logger.debug("Task {} try to spill {} from {} for {}", taskAttemptId, Utils.bytesToString(requested), consumerToSpill, requestingConsumer);

    try {
      long released = consumerToSpill.spill(requested, requestingConsumer);  // 释放当前消费者资源
      if (released > 0) {
        logger.debug("Task {} spilled {} of requested {} from {} for {}", taskAttemptId,
          Utils.bytesToString(released), Utils.bytesToString(requested), consumerToSpill,
          requestingConsumer);

        return memoryManager.acquireExecutionMemory(requested, taskAttemptId, mode);
      } else {
        cList.remove(idx);
        return 0;
      }
    } catch (ClosedByInterruptException e) {
      logger.error("error while calling spill() on " + consumerToSpill, e);
      throw new RuntimeException(e.getMessage());
    } catch (IOException e) {
      logger.error("error while calling spill() on " + consumerToSpill, e);
      throw new SparkOutOfMemoryError("error while calling spill() on " + consumerToSpill + " : " + e.getMessage());
    }
  }


  /***
   * **************************************************************************************
   * 释放当前消费者内存资源
   * **************************************************************************************
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

      long memoryNotAccountedFor = memoryManager.getExecutionMemoryUsageForTask(taskAttemptId) - memoryAccountedForByConsumers;

      logger.info("{} bytes of memory were used by task {} but are not associated with specific consumers",
        memoryNotAccountedFor, taskAttemptId);

      logger.info("{} bytes of memory are used for execution and {} bytes of memory are used for storage",
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
      pageNumber = allocatedPages.nextClearBit(0);  // 找到最近的没有分配的一个物理快

      if (pageNumber >= PAGE_TABLE_SIZE) {
        releaseExecutionMemory(acquired, consumer);
        throw new IllegalStateException("Have already allocated a maximum of " + PAGE_TABLE_SIZE + " pages");
      }

      allocatedPages.set(pageNumber);
    }

    MemoryBlock page = null;

    try {
      // 分配内存资源
      page = memoryManager.tungstenMemoryAllocator().allocate(acquired);  // 分配一个page
    } catch (OutOfMemoryError e) {
      logger.warn("Failed to allocate a page ({} bytes), try again.", acquired);

      synchronized (this) {
        acquiredButNotUsed += acquired;
        allocatedPages.clear(pageNumber);
      }

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
   * 释放一个物理快
   */
  public void freePage(MemoryBlock page, MemoryConsumer consumer) {

    assert (page.pageNumber != MemoryBlock.NO_PAGE_NUMBER) : "Called freePage() on memory that wasn't allocated with allocatePage()";
    assert (page.pageNumber != MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER) : "Called freePage() on a memory block that has already been freed";
    assert (page.pageNumber != MemoryBlock.FREED_IN_TMM_PAGE_NUMBER) : "Called freePage() on a memory block that has already been freed";
    assert(allocatedPages.get(page.pageNumber));

    pageTable[page.pageNumber] = null;

    synchronized (this) {
      allocatedPages.clear(page.pageNumber);
    }

    if (logger.isTraceEnabled()) {
      logger.trace("Freed page number {} ({} bytes)", page.pageNumber, page.size());
    }

    long pageSize = page.size();

    page.pageNumber = MemoryBlock.FREED_IN_TMM_PAGE_NUMBER;
    memoryManager.tungstenMemoryAllocator().free(page);
    releaseExecutionMemory(pageSize, consumer);
  }

  // 将块号加块内地址转为 long 类型
  public long encodePageNumberAndOffset(MemoryBlock page, long offsetInPage) {
    if (tungstenMemoryMode == MemoryMode.OFF_HEAP) {
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
