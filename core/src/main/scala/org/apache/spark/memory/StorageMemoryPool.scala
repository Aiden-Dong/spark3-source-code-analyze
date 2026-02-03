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

import javax.annotation.concurrent.GuardedBy

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.memory.MemoryStore

/**
 * 执行簿记操作，用于管理一个可调整大小的内存池，该内存池用于存储（缓存）。
 *
 * 内存申请流程:
 * 1. StorageMemoryPool.acquireMemory() - 检查是否有足够内存
 * 2. 如果内存不足，调用 MemoryStore.evictBlocksToFreeSpace() - 驱逐旧块
 * 3. MemoryStore 选择合适的块进行驱逐，释放内存
 * 4. StorageMemoryPool 更新内存使用统计
 *
 * 数据存储流程:
 * 1. MemoryStore.putBytes() - 存储数据块
 * 2. 调用 MemoryManager.acquireStorageMemory() - 申请内存
 * 3. StorageMemoryPool 检查并分配内存
 * 4. MemoryStore 将数据存入内存并更新映射表
 *
 * @param lock 用于同步的 [[MemoryManager]] 实例
 * @param memoryMode 此池跟踪的内存类型（堆内或堆外）
 *
 *                   应用请求缓存Block
 *                   │
 *                   ▼
 *                   ┌─────────────────┐
 *                   │StorageMemoryPool│ ◄─── 内存管理层 (Resource Management)
 *                   │                 │
 *                   │ 1. 检查可用内存  │
 *                   │ 2. 计算需释放量  │
 *                   │ 3. 调用驱逐方法  │
 *                   │ 4. 更新内存统计  │
 *                   └─────────┬───────┘
 *                   │
 *                   │ memoryStore.evictBlocksToFreeSpace()
 *                   ▼
 *                   ┌─────────────────┐
 *                   │   MemoryStore   │ ◄─── 数据存储层 (Data Storage)
 *                   │                 │
 *                   │ 1. 选择驱逐块    │
 *                   │ 2. 获取写锁      │
 *                   │ 3. 执行驱逐      │
 *                   │ 4. 存储新块      │
 *                   └─────────┬───────┘
 *                   │
 *                   │ 管理具体数据
 *                   ▼
 *                   ┌─────────────────────────────────────────────────────────┐
 *                   │                Block 数据结构                            │
 *                   │                                                         │
 *                   │  LinkedHashMap<BlockId, MemoryEntry>                   │
 *                   │  ┌──────────┬─────────────────────────────────────────┐ │
 *                   │  │ BlockId  │           MemoryEntry                   │ │
 *                   │  ├──────────┼─────────────────────────────────────────┤ │
 *                   │  │ rdd_1_0  │ DeserializedMemoryEntry(Array[T])      │ │
 *                   │  │ rdd_2_1  │ SerializedMemoryEntry(ByteBuffer)      │ │
 *                   │  │ broadcast│ SerializedMemoryEntry(ByteBuffer)      │ │
 *                   │  │ shuffle  │ DeserializedMemoryEntry(Array[T])      │ │
 *                   │  └──────────┴─────────────────────────────────────────┘ │
 *                   └─────────────────────────────────────────────────────────┘
 *
 *
 *
 *
 *                   ┌─────────────────────────────────────────────────────────────────────────────────┐
 *                   │                           内存驱逐 (Eviction) 详细流程                            │
 *                   └─────────────────────────────────────────────────────────────────────────────────┘
 *
 *                   新Block请求内存 (100MB)
 *                   │
 *                   ▼
 *                   ┌───────────────────┐    可用内存不足 (只有30MB)
 *                   │StorageMemoryPool  │ ──────────────────────────────┐
 *                   │acquireMemory()    │                               │
 *                   └───────────────────┘                               │
 *                   │                                           │
 *                   │ 需要释放70MB                                │
 *                   ▼                                           │
 *                   ┌───────────────────┐                               │
 *                   │MemoryStore        │                               │
 *                   │evictBlocksToFreeSpace()                           │
 *                   └───────┬───────────┘                               │
 *                   │                                           │
 *                   ▼                                           │
 *                   ┌─────────────────────────────────────────────────┐ │
 *                   │          选择驱逐候选块                          │ │
 *                   │                                                 │ │
 *                   │  遍历 LinkedHashMap (LRU顺序):                  │ │
 *                   │  ┌─────────┬──────────┬─────────┬─────────────┐  │ │
 *                   │  │Block-A  │ Block-B  │Block-C  │   Block-D   │  │ │
 *                   │  │(20MB)   │ (30MB)   │(40MB)   │   (50MB)    │  │ │
 *                   │  │ 可驱逐   │ 被锁定   │ 可驱逐   │   可驱逐     │  │ │
 *                   │  └─────────┴──────────┴─────────┴─────────────┘  │ │
 *                   │                                                 │ │
 *                   │  选中: Block-A + Block-C = 60MB < 70MB          │ │
 *                   │  继续: Block-A + Block-C + Block-D = 110MB ✓   │ │
 *                   └─────────────────────────────────────────────────┘ │
 *                   │                                           │
 *                   ▼                                           │
 *                   ┌─────────────────────────────────────────────────┐ │
 *                   │              执行驱逐操作                        │ │
 *                   │                                                 │ │
 *                   │  for each selectedBlock:                        │ │
 *                   │    1. blockInfoManager.lockForWriting()        │ │
 *                   │    2. 获取Block数据                              │ │
 *                   │    3. blockEvictionHandler.dropFromMemory()    │ │
 *                   │    4. 可能写入磁盘                               │ │
 *                   │    5. 从entries中移除                           │ │
 *                   │    6. 释放锁                                    │ │
 *                   └─────────────────────────────────────────────────┘ │
 *                   │                                           │
 *                   ▼                                           │
 *                   ┌───────────────────┐    释放了110MB                │
 *                   │StorageMemoryPool  │ ◄─────────────────────────────┘
 *                   │更新内存统计        │
 *                   │_memoryUsed -= 110MB│
 *                   └───────────────────┘
 *                   │
 *                   ▼
 *                   存储新Block (100MB) ✓
 */
private[memory] class StorageMemoryPool(
    lock: Object,
    memoryMode: MemoryMode
  ) extends MemoryPool(lock) with Logging {

  private[this] val poolName: String = memoryMode match {
    case MemoryMode.ON_HEAP => "on-heap storage"
    case MemoryMode.OFF_HEAP => "off-heap storage"
  }

  @GuardedBy("lock")
  private[this] var _memoryUsed: Long = 0L

  override def memoryUsed: Long = lock.synchronized {
    _memoryUsed
  }

  private var _memoryStore: MemoryStore = _
  def memoryStore: MemoryStore = {
    if (_memoryStore == null) {
      throw new IllegalStateException("memory store not initialized yet")
    }
    _memoryStore
  }

  /**
   * 设置此管理器用于驱逐缓存块的 [[MemoryStore]]。
   * 由于初始化顺序约束，这必须在构造后设置。
   */
  final def setMemoryStore(store: MemoryStore): Unit = {
    _memoryStore = store
  }

  /**
   * 获取 N 字节内存来缓存给定的块，必要时驱逐现有的块。
   * @return 是否成功授予了所有 N 字节.
   */
  def acquireMemory(blockId: BlockId, numBytes: Long): Boolean = lock.synchronized {
    val numBytesToFree = math.max(0, numBytes - memoryFree)
    acquireMemory(blockId, numBytes, numBytesToFree)
  }
  def acquireMemory(blockId: BlockId, numBytesToAcquire: Long,
                    numBytesToFree: Long): Boolean = lock.synchronized {

    assert(numBytesToAcquire >= 0)
    assert(numBytesToFree >= 0)
    assert(memoryUsed <= poolSize)

    if (numBytesToFree > 0) {
      memoryStore.evictBlocksToFreeSpace(Some(blockId), numBytesToFree, memoryMode)
    }

    val enoughMemory = numBytesToAcquire <= memoryFree
    if (enoughMemory) {
      _memoryUsed += numBytesToAcquire
    }
    enoughMemory
  }

  // 释放指定的内存容量
  def releaseMemory(size: Long): Unit = lock.synchronized {
    if (size > _memoryUsed) {
      logWarning(s"Attempted to release $size bytes of storage " +
        s"memory when we only have ${_memoryUsed} bytes")
      _memoryUsed = 0
    } else {
      _memoryUsed -= size
    }
  }

  // 释放所有的内存资源
  def releaseAllMemory(): Unit = lock.synchronized {
    _memoryUsed = 0
  }

  /**
   * 释放空间以将此存储内存池的大小缩小 `spaceToFree` 字节。
   * 注意：此方法实际上不会减少池大小，而是依赖调用者来执行此操作。
   */
  def freeSpaceToShrinkPool(spaceToFree: Long): Long = lock.synchronized {
    val spaceFreedByReleasingUnusedMemory = math.min(spaceToFree, memoryFree)
    val remainingSpaceToFree = spaceToFree - spaceFreedByReleasingUnusedMemory
    if (remainingSpaceToFree > 0) {
      // If reclaiming free memory did not adequately shrink the pool, begin evicting blocks:
      val spaceFreedByEviction =
        memoryStore.evictBlocksToFreeSpace(None, remainingSpaceToFree, memoryMode)
      // When a block is released, BlockManager.dropFromMemory() calls releaseMemory(), so we do
      // not need to decrement _memoryUsed here. However, we do need to decrement the pool size.
      spaceFreedByReleasingUnusedMemory + spaceFreedByEviction
    } else {
      spaceFreedByReleasingUnusedMemory
    }
  }
}
