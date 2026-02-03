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

import scala.collection.mutable

import org.apache.spark.internal.Logging

/**
 * 实现在任务之间共享可调整大小的内存池的策略和簿记操作。
 *
 * 尝试确保每个任务获得合理的内存份额，而不是某个任务首先占用大量内存， 然后导致其他任务反复溢出到磁盘。
 *
 * 如果有 N 个任务，它确保每个任务在必须溢出之前至少可以获得 1/2N 的内存， 最多获得 1/N 的内存。
 * 由于 N 是动态变化的，我们跟踪活跃任务集合，并在此集合发生变化时重新计算等待任务中的 1/2N 和 1/N。
 *
 * 这一切都通过同步访问可变状态并使用 wait() 和 notifyAll() 向调用者发出变化信号来完成。
 *
 * 在 Spark 1.6 之前，这种跨任务的内存仲裁由 ShuffleMemoryManager 执行。
 *
 * @param lock a [[MemoryManager]] 用于同步的 [[MemoryManager]] 实例
 * @param memoryMode 此池跟踪的内存类型（堆内或堆外）
 */
private[memory] class ExecutionMemoryPool(
    lock: Object,
    memoryMode: MemoryMode
  ) extends MemoryPool(lock) with Logging {

  private[this] val poolName: String = memoryMode match {
    case MemoryMode.ON_HEAP => "on-heap execution"
    case MemoryMode.OFF_HEAP => "off-heap execution"
  }

  // 从 taskAttemptId -> 内存消耗（以字节为单位）的映射
  @GuardedBy("lock")
  private val memoryForTask = new mutable.HashMap[Long, Long]()

  // 所有任务总的内存消耗
  override def memoryUsed: Long = lock.synchronized {
    memoryForTask.values.sum
  }

  // 获取指定任务的内存消耗
  def getMemoryUsageForTask(taskAttemptId: Long): Long = lock.synchronized {
    memoryForTask.getOrElse(taskAttemptId, 0L)
  }

  /**
   * 尝试为给定任务获取最多 `numBytes` 的内存，并返回获得的字节数；如果无法分配，则返回 0。
   *
   * 在某些情况下，此调用可能会阻塞，直到有足够的空闲内存，
   * 以确保每个任务在被迫溢出之前有机会逐步增加到总内存池的至少 1 / 2N（其中 N 是活跃任务的数量）。
   * 如果任务数量增加，但较旧的任务已经占用了大量内存，则可能会发生这种情况。
   *
   * @param numBytes 要获取的字节数
   * @param taskAttemptId 正在获取内存的任务尝试 ID
   * @param maybeGrowPool 回调从其他地方获取内存满足当前内存资源使用
   * @param computeMaxPoolSize 计算当前Execution 的最大可用内存
   *
   * @return 授予任务的字节数。
   */
  private[memory] def acquireMemory(numBytes: Long, taskAttemptId: Long,
      maybeGrowPool: Long => Unit = (additionalSpaceNeeded: Long) => (),
      computeMaxPoolSize: () => Long = () => poolSize): Long = lock.synchronized {

    assert(numBytes > 0, s"invalid number of bytes requested: $numBytes")

    // 将此任务添加到 taskMemory 映射中，这样我们就可以准确计算活跃任务的数量，
    // 让其他任务在调用 `acquireMemory` 时减少其内存使用
    if (!memoryForTask.contains(taskAttemptId)) {
      memoryForTask(taskAttemptId) = 0L
      lock.notifyAll()                          // 这将稍后导致等待的任务唤醒并再次检查 numTasks
    }

    // 持续循环，直到我们确定不想授予此请求（因为此任务将拥有超过 1/numActiveTasks 的内存）
    // 或者我们有足够的空闲内存来给它（我们总是让每个任务至少获得 1/(2 * numActiveTasks)）
    // TODO: 简化这个逻辑，将每个任务限制在自己的槽位
    while (true) {
      val numActiveTasks = memoryForTask.keys.size    // 获取当前活跃的任务数量
      val curMem = memoryForTask(taskAttemptId)       // 获取当前任务使用的内存数量

      // 表示如果Execution内存不足，则尝试从Storagememory中获取内存
      maybeGrowPool(numBytes - memoryFree)


      // (最大内存 / 2n,     最大内存 / n)
      val maxPoolSize = computeMaxPoolSize()
      val maxMemoryPerTask = maxPoolSize / numActiveTasks          // 每个任务最多分配的内存数
      val minMemoryPerTask = poolSize / (2 * numActiveTasks)       // 每个任务最少分配的内存数

      // 我们可以授予此任务多少内存；将其份额保持在 0 <= X <= 1/numActiveTasks 范围内
      val maxToGrant = math.min(numBytes, math.max(0, maxMemoryPerTask - curMem))
      // 只给它空闲的内存，如果它达到了 1/numTasks，可能没有内存可给
      val toGrant = math.min(maxToGrant, memoryFree)

      // 我们希望让每个任务在阻塞之前至少获得 1/(2 * numActiveTasks)；
      // 如果我们现在不能给它这么多，就等待其他任务释放内存
      //（如果较旧的任务在 N 增长之前分配了大量内存，就会发生这种情况）
      if (toGrant < numBytes && curMem + toGrant < minMemoryPerTask) {
        logInfo(s"TID $taskAttemptId waiting for at least 1/2N of $poolName pool to be free")
        lock.wait()
      } else {
        memoryForTask(taskAttemptId) += toGrant
        return toGrant
      }
    }
    0L  // Never reached
  }

  /**
   *  释放给定任务获取的 `numBytes` 内存。
   */
  def releaseMemory(numBytes: Long, taskAttemptId: Long): Unit = lock.synchronized {
    val curMem = memoryForTask.getOrElse(taskAttemptId, 0L)
    val memoryToFree = if (curMem < numBytes) {
      logWarning(
        s"Internal error: release called on $numBytes bytes but task only has $curMem bytes " +
          s"of memory from the $poolName pool")
      curMem
    } else {
      numBytes
    }
    if (memoryForTask.contains(taskAttemptId)) {
      memoryForTask(taskAttemptId) -= memoryToFree
      if (memoryForTask(taskAttemptId) <= 0) {
        memoryForTask.remove(taskAttemptId)
      }
    }
    lock.notifyAll() // 通知 acquireMemory() 中的等待者内存已被释放
  }

  /**
   * 释放给定任务的所有内存并将其标记为非活跃状态（例如，当任务结束时）
   * @return  释放的字节数.
   */
  def releaseAllMemoryForTask(taskAttemptId: Long): Long = lock.synchronized {
    val numBytesToFree = getMemoryUsageForTask(taskAttemptId)
    releaseMemory(numBytesToFree, taskAttemptId)
    numBytesToFree
  }

}
