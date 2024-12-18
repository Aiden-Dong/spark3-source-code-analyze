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

  // -- 与内存分配策略和记录管理相关的方法 ------------------------------

  @GuardedBy("this")
  protected val onHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.ON_HEAP)
  @GuardedBy("this")
  protected val offHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.OFF_HEAP)
  @GuardedBy("this")
  protected val onHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.ON_HEAP)
  @GuardedBy("this")
  protected val offHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.OFF_HEAP)

  onHeapStorageMemoryPool.incrementPoolSize(onHeapStorageMemory)
  onHeapExecutionMemoryPool.incrementPoolSize(onHeapExecutionMemory)

  protected[this] val maxOffHeapMemory = conf.get(MEMORY_OFFHEAP_SIZE)  // spark.memory.offHeap.size
  protected[this] val offHeapStorageMemory = (maxOffHeapMemory * conf.get(MEMORY_STORAGE_FRACTION)).toLong

  offHeapExecutionMemoryPool.incrementPoolSize(maxOffHeapMemory - offHeapStorageMemory)
  offHeapStorageMemoryPool.incrementPoolSize(offHeapStorageMemory)

  /**
   * Total available on heap memory for storage, in bytes. This amount can vary over time,
   * depending on the MemoryManager implementation.
   * In this model, this is equivalent to the amount of memory not occupied by execution.
   */
  def maxOnHeapStorageMemory: Long

  /**
   * Total available off heap memory for storage, in bytes. This amount can vary over time,
   * depending on the MemoryManager implementation.
   */
  def maxOffHeapStorageMemory: Long

  /**
   * Set the [[MemoryStore]] used by this manager to evict cached blocks.
   * This must be set after construction due to initialization ordering constraints.
   */
  final def setMemoryStore(store: MemoryStore): Unit = synchronized {
    onHeapStorageMemoryPool.setMemoryStore(store)
    offHeapStorageMemoryPool.setMemoryStore(store)
  }

  /**
   * Acquire N bytes of memory to cache the given block, evicting existing ones if necessary.
   *
   * @return whether all N bytes were successfully granted.
   */
  def acquireStorageMemory(blockId: BlockId, numBytes: Long, memoryMode: MemoryMode): Boolean

  /**
   * Acquire N bytes of memory to unroll the given block, evicting existing ones if necessary.
   *
   * This extra method allows subclasses to differentiate behavior between acquiring storage
   * memory and acquiring unroll memory. For instance, the memory management model in Spark
   * 1.5 and before places a limit on the amount of space that can be freed from unrolling.
   *
   * @return whether all N bytes were successfully granted.
   */
  def acquireUnrollMemory(blockId: BlockId, numBytes: Long, memoryMode: MemoryMode): Boolean

  /**
   * Try to acquire up to `numBytes` of execution memory for the current task and return the
   * number of bytes obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   */
  private[memory]
  def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long

  /**
   * Release numBytes of execution memory belonging to the given task.
   */
  private[memory]
  def releaseExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Unit = synchronized {
    memoryMode match {
      case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId)
      case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId)
    }
  }

  /**
   * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
   *
   * @return the number of bytes freed.
   */
  private[memory] def releaseAllExecutionMemoryForTask(taskAttemptId: Long): Long = synchronized {
    onHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId) +
      offHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId)
  }

  /**
   * Release N bytes of storage memory.
   */
  def releaseStorageMemory(numBytes: Long, memoryMode: MemoryMode): Unit = synchronized {
    memoryMode match {
      case MemoryMode.ON_HEAP => onHeapStorageMemoryPool.releaseMemory(numBytes)
      case MemoryMode.OFF_HEAP => offHeapStorageMemoryPool.releaseMemory(numBytes)
    }
  }

  /**
   * Release all storage memory acquired.
   */
  final def releaseAllStorageMemory(): Unit = synchronized {
    onHeapStorageMemoryPool.releaseAllMemory()
    offHeapStorageMemoryPool.releaseAllMemory()
  }

  /**
   * Release N bytes of unroll memory.
   */
  final def releaseUnrollMemory(numBytes: Long, memoryMode: MemoryMode): Unit = synchronized {
    releaseStorageMemory(numBytes, memoryMode)
  }

  /**
   * Execution memory currently in use, in bytes.
   */
  final def executionMemoryUsed: Long = synchronized {
    onHeapExecutionMemoryPool.memoryUsed + offHeapExecutionMemoryPool.memoryUsed
  }

  /**
   * Storage memory currently in use, in bytes.
   */
  final def storageMemoryUsed: Long = synchronized {
    onHeapStorageMemoryPool.memoryUsed + offHeapStorageMemoryPool.memoryUsed
  }

  /**
   *  On heap execution memory currently in use, in bytes.
   */
  final def onHeapExecutionMemoryUsed: Long = synchronized {
    onHeapExecutionMemoryPool.memoryUsed
  }

  /**
   *  Off heap execution memory currently in use, in bytes.
   */
  final def offHeapExecutionMemoryUsed: Long = synchronized {
    offHeapExecutionMemoryPool.memoryUsed
  }

  /**
   *  On heap storage memory currently in use, in bytes.
   */
  final def onHeapStorageMemoryUsed: Long = synchronized {
    onHeapStorageMemoryPool.memoryUsed
  }

  /**
   *  Off heap storage memory currently in use, in bytes.
   */
  final def offHeapStorageMemoryUsed: Long = synchronized {
    offHeapStorageMemoryPool.memoryUsed
  }

  /**
   * Returns the execution memory consumption, in bytes, for the given task.
   */
  private[memory] def getExecutionMemoryUsageForTask(taskAttemptId: Long): Long = synchronized {
    onHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId) +
      offHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId)
  }

  // -- Fields related to Tungsten managed memory -------------------------------------------------

  /**
   * Tracks whether Tungsten memory will be allocated on the JVM heap or off-heap using
   * sun.misc.Unsafe.
   */
  final val tungstenMemoryMode: MemoryMode = {
    if (conf.get(MEMORY_OFFHEAP_ENABLED)) {
      require(conf.get(MEMORY_OFFHEAP_SIZE) > 0,
        "spark.memory.offHeap.size must be > 0 when spark.memory.offHeap.enabled == true")
      require(Platform.unaligned(),
        "No support for unaligned Unsafe. Set spark.memory.offHeap.enabled to false.")
      MemoryMode.OFF_HEAP
    } else {
      MemoryMode.ON_HEAP
    }
  }

  /**
   * 默认页面大小（以字节为单位）。
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

  /**
   * Allocates memory for use by Unsafe/Tungsten code.
   */
  private[memory] final val tungstenMemoryAllocator: MemoryAllocator = {
    tungstenMemoryMode match {
      case MemoryMode.ON_HEAP => MemoryAllocator.HEAP
      case MemoryMode.OFF_HEAP => MemoryAllocator.UNSAFE
    }
  }

  /**
   * Return whether we are using G1GC or not
   */
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
