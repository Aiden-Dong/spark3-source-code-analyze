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

package org.apache.spark.memory

import java.lang.management.{ManagementFactory, PlatformManagedObject}
import javax.annotation.concurrent.GuardedBy

import scala.util.Try

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.memory.MemoryStore
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.memory.MemoryAllocator
import org.apache.spark.util.Utils

/**
 * 一个抽象的内存管理器，用于管理执行内存和存储内存之间的共享方式。
 * 在此上下文中，执行内存是指用于洗牌（shuffles）、连接（joins）、排序（sorts）和聚合（aggregations）计算的内存，而存储内存是指用于缓存和在集群内传播内部数据的内存。
 * 每个JVM（Java虚拟机）中存在一个MemoryManager。
 */
private[spark] abstract class MemoryManager(
    conf: SparkConf,
    numCores: Int,
    onHeapStorageMemory: Long,
    onHeapExecutionMemory: Long) extends Logging {

  require(onHeapExecutionMemory > 0, "onHeapExecutionMemory must be > 0")

  /********************************************************************************************
   *                                   内存资源池                                              *
   ********************************************************************************************/

  @GuardedBy("this") protected val onHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.ON_HEAP)        // 堆内存储内存池
  @GuardedBy("this") protected val onHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.ON_HEAP)     // 堆内执行内存池

  onHeapStorageMemoryPool.incrementPoolSize(onHeapStorageMemory)
  onHeapExecutionMemoryPool.incrementPoolSize(onHeapExecutionMemory)

  @GuardedBy("this") protected val offHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.OFF_HEAP)       // 堆外存储内存池
  @GuardedBy("this") protected val offHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.OFF_HEAP)   // 堆外执行内存池

  protected[this] val maxOffHeapMemory = conf.get(MEMORY_OFFHEAP_SIZE)  // spark.memory.offHeap.size
  protected[this] val offHeapStorageMemory = (maxOffHeapMemory * conf.get(MEMORY_STORAGE_FRACTION)).toLong
  offHeapExecutionMemoryPool.incrementPoolSize(maxOffHeapMemory - offHeapStorageMemory)
  offHeapStorageMemoryPool.incrementPoolSize(offHeapStorageMemory)

  def maxOnHeapStorageMemory: Long           // 最大堆内存储内存
  def maxOffHeapStorageMemory: Long          // 最大堆外存储内存

  /********************************************************************************************
   *                                   内存管理(申请/释放)                                      *
   ********************************************************************************************/

  /**
   * 设置内存存储，用于淘汰缓存块
   * 必须在构造后设置，因为初始化顺序约束
   */
  final def setMemoryStore(store: MemoryStore): Unit = synchronized {
    onHeapStorageMemoryPool.setMemoryStore(store)
    offHeapStorageMemoryPool.setMemoryStore(store)
  }

  /**
   * 为缓存指定块申请 N 字节的存储内存.
   *
   * val success = memoryManager.acquireStorageMemory(blockId, 1024 * 1024, MemoryMode.ON_HEAP)
   *
   * @return 是否成功授予了全部 N 字节内存.
   */
  def acquireStorageMemory(blockId: BlockId, numBytes: Long, memoryMode: MemoryMode): Boolean

  /**
   * 释放 N 字节的存储内存
   */
  def releaseStorageMemory(numBytes: Long, memoryMode: MemoryMode): Unit = synchronized {
    memoryMode match {
      case MemoryMode.ON_HEAP => onHeapStorageMemoryPool.releaseMemory(numBytes)
      case MemoryMode.OFF_HEAP => offHeapStorageMemoryPool.releaseMemory(numBytes)
    }
  }

  /**
   * 为展开(unroll)指定块申请内存，这是存储内存的特殊情况
   * - 用于渐进式反序列化大数据集时的临时内存
   * - 可以与普通存储内存有不同的管理策略
   *
   * @return whether all N bytes were successfully granted.
   */
  def acquireUnrollMemory(blockId: BlockId, numBytes: Long, memoryMode: MemoryMode): Boolean

  /**
   * 释放 N 字节的内存.
   */
  final def releaseUnrollMemory(numBytes: Long, memoryMode: MemoryMode): Unit = synchronized {
    releaseStorageMemory(numBytes, memoryMode)
  }


  /**
   * 为当前任务申请执行内存，返回实际获得的字节数
   *
   * val granted = memoryManager.acquireExecutionMemory(requestedBytes, taskId, MemoryMode.ON_HEAP)
   *
   * 尝试为当前任务获取最多 numBytes 字节的执行内存，并返回实际获得的字节数，如果无法分配则返回 0。
   * 在某些情况下，此调用可能会阻塞直到有足够的空闲内存，以确保每个任务在被迫溢写之前都有机会获得至少总内存池的 1/2N（其中 N 是活跃任务的数量）。
   * 当任务数量增加但某个较老的任务已经占用了大量内存时，就会发生这种情况。
   *
   * • (公平性保证)：确保新任务能获得最低内存保障（总内存的 1/2N）
   * • (阻塞机制)：必要时会等待，而不是立即失败
   * • (防止饥饿)：避免老任务占用过多内存导致新任务无法启动
   * • (溢写触发)：只有在获得基本内存保障后才会被迫溢写到磁盘
   */
  private[memory]
  def acquireExecutionMemory(numBytes: Long, taskAttemptId: Long, memoryMode: MemoryMode): Long

  /**
   * 释放指定任务的执行内存
   */
  private[memory]
  def releaseExecutionMemory(numBytes: Long, taskAttemptId: Long, memoryMode: MemoryMode): Unit = synchronized {
    memoryMode match {
      case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId)
      case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId)
    }
  }

  /**
   * 释放指定任务的所有执行内存，返回释放的字节数
   * 任务结束时调用
   * 确保内存不会泄漏
   */
  private[memory] def releaseAllExecutionMemoryForTask(taskAttemptId: Long): Long = synchronized {
    onHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId) +
      offHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId)
  }

  /**
   * 释放所有存储内存, 通常在应用程序关闭时调用.
   */
  final def releaseAllStorageMemory(): Unit = synchronized {
    onHeapStorageMemoryPool.releaseAllMemory()
    offHeapStorageMemoryPool.releaseAllMemory()
  }

  /****************************************************************************
   *                                   使用量查询                              *
   ****************************************************************************/

  // 当前执行内存使用量
  final def executionMemoryUsed: Long = synchronized {
    onHeapExecutionMemoryPool.memoryUsed + offHeapExecutionMemoryPool.memoryUsed
  }

  // 当前存储内存使用量
  final def storageMemoryUsed: Long = synchronized {
    onHeapStorageMemoryPool.memoryUsed + offHeapStorageMemoryPool.memoryUsed
  }

  // 堆内执行内存使用量
  final def onHeapExecutionMemoryUsed: Long = synchronized {
    onHeapExecutionMemoryPool.memoryUsed
  }

  // 堆外执行内存使用量
  final def offHeapExecutionMemoryUsed: Long = synchronized {
    offHeapExecutionMemoryPool.memoryUsed
  }

  //  堆内存储内存使用量
  final def onHeapStorageMemoryUsed: Long = synchronized {
    onHeapStorageMemoryPool.memoryUsed
  }

  //   堆外存储内存使用量
  final def offHeapStorageMemoryUsed: Long = synchronized {
    offHeapStorageMemoryPool.memoryUsed
  }

  // 指定任务的内存使用量
  private[memory] def getExecutionMemoryUsageForTask(taskAttemptId: Long): Long = synchronized {
    onHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId) +
      offHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId)
  }

  /********************************************************************************************
   *                                   Tungsten 内存管理                                       *
   ********************************************************************************************/

  /**
   * Tungsten 内存模式 (ON_HEAP/OFF_HEAP)
   * spark.memory.offHeap.enabled
   */
  final val tungstenMemoryMode: MemoryMode = {

    if (conf.get(MEMORY_OFFHEAP_ENABLED)) {
      require(conf.get(MEMORY_OFFHEAP_SIZE) > 0, "spark.memory.offHeap.size must be > 0 when spark.memory.offHeap.enabled == true")
      require(Platform.unaligned(), "No support for unaligned Unsafe. Set spark.memory.offHeap.enabled to false.")

      MemoryMode.OFF_HEAP
    } else {

      MemoryMode.ON_HEAP
    }
  }

  // Tungsten 内存分配器
  private[memory] final val tungstenMemoryAllocator: MemoryAllocator = {
    tungstenMemoryMode match {
      case MemoryMode.ON_HEAP => MemoryAllocator.HEAP
      case MemoryMode.OFF_HEAP => MemoryAllocator.UNSAFE
    }
  }

  /**
   * 动态计算默认页面大小
   *
   * 如果用户没有明确设置 "spark.buffer.pageSize"，我们通过查看可用于进程的核心数量和总内存量来确定默认值，然后将其除以一个安全因子。
   *
   * SPARK-37593 如果我们使用 G1GC，考虑 LONG_ARRAY_OFFSET 是更好的选择，
   * 以便请求的内存大小是 2 的幂，并且可以被 G1 堆区域大小整除，从而减少一个 G1 区域内的内存浪费
   */
  private lazy val defaultPageSizeBytes = {
    val minPageSize = 1L * 1024 * 1024   // 1MB
    val maxPageSize = 64L * minPageSize  // 64MB
    val cores = if (numCores > 0) numCores else Runtime.getRuntime.availableProcessors()
    // Because of rounding to next power of 2, we may have safetyFactor as 8 in worst case
    val safetyFactor = 16

    // 最大内存大小
    val maxTungstenMemory: Long = tungstenMemoryMode match {
      case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.poolSize
      case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.poolSize
    }

    // 内存大小 / core / 16
    val size = ByteArrayMethods.nextPowerOf2(maxTungstenMemory / cores / safetyFactor)
    val chosenPageSize = math.min(maxPageSize, math.max(minPageSize, size))
    if (isG1GC && tungstenMemoryMode == MemoryMode.ON_HEAP) {
      chosenPageSize - Platform.LONG_ARRAY_OFFSET
    } else {
      chosenPageSize
    }
  }

  val pageSizeBytes: Long = conf.get(BUFFER_PAGESIZE).getOrElse(defaultPageSizeBytes)


  private lazy val isG1GC: Boolean = {
    Try {
      val clazz = Utils.classForName("com.sun.management.HotSpotDiagnosticMXBean")
        .asInstanceOf[Class[_ <: PlatformManagedObject]]
      val vmOptionClazz = Utils.classForName("com.sun.management.VMOption")
      val hotSpotDiagnosticMXBean = ManagementFactory.getPlatformMXBean(clazz)
      val vmOptionMethod = clazz.getMethod("getVMOption", classOf[String])
      val valueMethod = vmOptionClazz.getMethod("getValue")

      val useG1GCObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "UseG1GC")
      val useG1GC = valueMethod.invoke(useG1GCObject).asInstanceOf[String]
      "true".equals(useG1GC)
    }.getOrElse(false)
  }
}
