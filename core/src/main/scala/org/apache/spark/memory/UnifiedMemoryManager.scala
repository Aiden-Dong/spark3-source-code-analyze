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

import org.apache.spark.SparkConf
import org.apache.spark.internal.config
import org.apache.spark.internal.config.Tests._
import org.apache.spark.storage.BlockId

/*
 ***
 *  ┌─────────────────────────────────────────────────────────────────────────────┐
    │                        UnifiedMemoryManager 架构图                           │
    └─────────────────────────────────────────────────────────────────────────────┘

                    JVM 总内存 (例如: 4GB)
    ┌─────────────────────────────────────────────────────────────────┐
    │                    系统保留内存 (300MB)                           │
    └─────────────────────────────────────────────────────────────────┘
    ┌─────────────────────────────────────────────────────────────────┐
    │              可用内存 (3.7GB)                                    │
    │  ┌────────────────────────────────────────────────────────────┐ │
    │  │     Spark 内存区域 (3.7GB * 0.6 = 2.22GB)                   │ │
    │  │  ┌─────────────────────┬─────────────────────────────────┐ │ │
    │  │  │   存储内存区域       │        执行内存区域                │ │ │
    │  │  │  (默认 50%)         │       (默认 50%)                 │ │ │
    │  │  │   1.11GB           │        1.11GB                    │ │ │
    │  │  │                    │                                  │ │ │
    │  │  │ ┌─────────────────┐ │ ┌───────────────────────────────┐ │ │ │
    │  │  │ │ 缓存的 RDD      │◄┼►│ Shuffle/Join/Sort/Agg          │ │ │ │
    │  │  │ │ 广播变量        │ │ │ 任务执行内存                     │ │ │ │
    │  │  │ │ 临时数据        │ │ │ 代码生成                         │ │ │ │
    │  │  │ └─────────────────┘ │ └───────────────────────────────┘ │ │ │
    │  │  └─────────────────────┴───────────────────────────────────┘ │ │
    │  │                    ▲                                         │ │
    │  │                    │ 动态借用内存                              │ │
    │  │                    ▼                                         │ │
    │  └────────────────────────────────────── ───────────────────────┘ │
    │                                                                   │
    │              其他内存 (3.7GB * 0.4 = 1.48GB)                     │
    │                   (用户对象、元数据等)                             │
    └─────────────────────────────────────────────────────────────────┘
 */
private[spark] class UnifiedMemoryManager(conf: SparkConf,                  // spark 配置信息
                                          val maxHeapMemory: Long,          // 当前最大的堆内存
                                          onHeapStorageRegionSize: Long,    // 用于存储的堆内存大小 (spark.memory.storageFraction)
                                          numCores: Int)                    // CPU 核心数
  extends MemoryManager(conf, numCores, onHeapStorageRegionSize,
    maxHeapMemory - onHeapStorageRegionSize) {

  private def assertInvariants(): Unit = {
    assert(onHeapExecutionMemoryPool.poolSize + onHeapStorageMemoryPool.poolSize == maxHeapMemory)
    assert(
      offHeapExecutionMemoryPool.poolSize + offHeapStorageMemoryPool.poolSize == maxOffHeapMemory)
  }

  assertInvariants()

  override def maxOnHeapStorageMemory: Long = synchronized {
    maxHeapMemory - onHeapExecutionMemoryPool.memoryUsed
  }

  override def maxOffHeapStorageMemory: Long = synchronized {
    maxOffHeapMemory - offHeapExecutionMemoryPool.memoryUsed
  }

  /*
   ************************************************************
   * 申请计算内存 :
   *************************************************************
   */
  override private[memory] def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long = synchronized {
    assertInvariants()
    assert(numBytes >= 0)

    // (执行内存池， 存储内存池， 存储区域基础内存大小， 最大内存)
    val (executionPool, storagePool, storageRegionSize, maxMemory) = memoryMode match {
      case MemoryMode.ON_HEAP => (
        onHeapExecutionMemoryPool,
        onHeapStorageMemoryPool,
        onHeapStorageRegionSize,
        maxHeapMemory)
      case MemoryMode.OFF_HEAP => (
        offHeapExecutionMemoryPool,
        offHeapStorageMemoryPool,
        offHeapStorageMemory,
        maxOffHeapMemory)
    }

    /// 存储内存释放一定的内存空间给计算内存 min(请求内存 , max(存储剩余内存， 存储超出基准的内存))
    def maybeGrowExecutionPool(extraMemoryNeeded: Long): Unit = {   // 定义内存池扩展函数
      if (extraMemoryNeeded > 0) {   // // 只有需要额外内存时才执行

        val memoryReclaimableFromStorage = math.max(
          storagePool.memoryFree,                     // 当前存储池的剩余内存
          storagePool.poolSize - storageRegionSize)   // 存储池超出基准内存的部分

        if (memoryReclaimableFromStorage > 0) {
          val spaceToReclaim = storagePool.freeSpaceToShrinkPool(
            math.min(extraMemoryNeeded, memoryReclaimableFromStorage))
          storagePool.decrementPoolSize(spaceToReclaim)
          executionPool.incrementPoolSize(spaceToReclaim)
        }
      }
    }

    // 计算资源池能使用的最大资源
    def computeMaxExecutionPoolSize(): Long = {
      maxMemory - math.min(storagePool.memoryUsed, storageRegionSize)
    }

    executionPool.acquireMemory(
      numBytes, taskAttemptId, maybeGrowExecutionPool, () => computeMaxExecutionPoolSize)
  }

  /*
  ************************************************************
  * 申请存储内存 :
  *************************************************************
  */
  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    assertInvariants()
    assert(numBytes >= 0)
    // 计算内存池， 存储内存池， 最大内存
    val (executionPool, storagePool, maxMemory) = memoryMode match {
      case MemoryMode.ON_HEAP => (
        onHeapExecutionMemoryPool,
        onHeapStorageMemoryPool,
        maxOnHeapStorageMemory)
      case MemoryMode.OFF_HEAP => (
        offHeapExecutionMemoryPool,
        offHeapStorageMemoryPool,
        maxOffHeapStorageMemory)
    }
    if (numBytes > maxMemory) {
      logInfo(s"Will not store $blockId as the required space ($numBytes bytes) exceeds our memory limit ($maxMemory bytes)")
      return false
    }
    if (numBytes > storagePool.memoryFree) {   // 如果当前存储剩余的内存不足
      // 计算需要从执行内存池中扣掉的内存大小
      val memoryBorrowedFromExecution = Math.min(executionPool.memoryFree,  numBytes - storagePool.memoryFree)

      executionPool.decrementPoolSize(memoryBorrowedFromExecution)
      storagePool.incrementPoolSize(memoryBorrowedFromExecution)
    }
    storagePool.acquireMemory(blockId, numBytes)
  }

  /*
 ************************************************************
 * 申请展开内存内存 : 从存储内存里申请
 *************************************************************
 */
  override def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    acquireStorageMemory(blockId, numBytes, memoryMode)
  }
}

object UnifiedMemoryManager {


  private val RESERVED_SYSTEM_MEMORY_BYTES = 300 * 1024 * 1024

  def apply(conf: SparkConf, numCores: Int): UnifiedMemoryManager = {
    val maxMemory = getMaxMemory(conf)
    new UnifiedMemoryManager(
      conf,
      maxHeapMemory = maxMemory,
      onHeapStorageRegionSize =
        (maxMemory * conf.get(config.MEMORY_STORAGE_FRACTION)).toLong,
      numCores = numCores)
  }

  // 获取 MemoryManager 可以使用的最大内存
  private def getMaxMemory(conf: SparkConf): Long = {
    val systemMemory = conf.get(TEST_MEMORY)    // Runtime.getRuntime.maxMemory

    val reservedMemory = conf.getLong(TEST_RESERVED_MEMORY.key,
      if (conf.contains(IS_TESTING)) 0 else RESERVED_SYSTEM_MEMORY_BYTES)   // 300MB


    val minSystemMemory = (reservedMemory * 1.5).ceil.toLong

    if (systemMemory < minSystemMemory) {
      throw new IllegalArgumentException(s"System memory $systemMemory must " +
        s"be at least $minSystemMemory. Please increase heap size using the --driver-memory " +
        s"option or ${config.DRIVER_MEMORY.key} in Spark configuration.")
    }


    if (conf.contains(config.EXECUTOR_MEMORY)) {
      val executorMemory = conf.getSizeAsBytes(config.EXECUTOR_MEMORY.key)
      if (executorMemory < minSystemMemory) {
        throw new IllegalArgumentException(s"Executor memory $executorMemory must be at least " +
          s"$minSystemMemory. Please increase executor memory using the " +
          s"--executor-memory option or ${config.EXECUTOR_MEMORY.key} in Spark configuration.")
      }
    }
    val usableMemory = systemMemory - reservedMemory
    val memoryFraction = conf.get(config.MEMORY_FRACTION)    // spark.memory.fractions
    (usableMemory * memoryFraction).toLong
  }
}
