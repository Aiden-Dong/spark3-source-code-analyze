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

package org.apache.spark.scheduler

import java.nio.ByteBuffer
import java.util.Properties

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config.APP_CALLER_CONTEXT
import org.apache.spark.internal.plugin.PluginContainer
import org.apache.spark.memory.{MemoryMode, TaskMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rdd.InputFileBlockHolder
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.util._

/**
 * 一个执行单元。在 Spark 中我们有两种类型的任务：
 *
 *  - [[org.apache.spark.scheduler.ShuffleMapTask]] Shuffle Map 任务
 *  - [[org.apache.spark.scheduler.ResultTask]] 结果任务
 *
 * 一个 Spark 作业由一个或多个阶段组成。作业的最后一个阶段由多个 ResultTask 组成，而早期的阶段由 ShuffleMapTask 组成。
 * ResultTask 执行任务并将任务输出发送回驱动程序应用。
 * ShuffleMapTask 执行任务并将任务输出分割到多个桶中（基于任务的分区器）。
 *
 * @param stageId 此任务所属阶段的 ID
 * @param stageAttemptId 此任务所属阶段的尝试 ID
 * @param partitionId RDD 中分区的索引号
 * @param localProperties 用户在驱动端设置的线程本地属性的副本
 * @param serializedTaskMetrics 在驱动端创建并序列化的 `TaskMetrics`，发送到执行器端
 *
 * 以下参数是可选的：
 * @param jobId 此任务所属作业的 ID
 * @param appId 此任务所属应用的 ID
 * @param appAttemptId 此任务所属应用的尝试 ID
 * @param isBarrier 此任务是否属于屏障阶段。对于屏障阶段，Spark 必须同时启动所有任务
 */
private[spark] abstract class Task[T](
    val stageId: Int,
    val stageAttemptId: Int,
    val partitionId: Int,
    @transient var localProperties: Properties = new Properties,
    // The default value is only used in tests.
    serializedTaskMetrics: Array[Byte] =
      SparkEnv.get.closureSerializer.newInstance().serialize(TaskMetrics.registered).array(),
    val jobId: Option[Int] = None,
    val appId: Option[String] = None,
    val appAttemptId: Option[String] = None,
    val isBarrier: Boolean = false) extends Serializable {

  @transient lazy val metrics: TaskMetrics =
    SparkEnv.get.closureSerializer.newInstance().deserialize(ByteBuffer.wrap(serializedTaskMetrics))

  /**
   * 由 [[org.apache.spark.executor.Executor]] 调用来运行此任务。
   *
   * @param taskAttemptId 在 SparkContext 内唯一的任务尝试标识符
   * @param attemptNumber 此任务已尝试的次数（第一次尝试为 0）
   * @param resources 此任务尝试可以访问的其他主机资源（如 GPU）
   * @return 任务的结果以及累加器的更新
   */
  final def run(
      taskAttemptId: Long,
      attemptNumber: Int,
      metricsSystem: MetricsSystem,
      cpus: Int,
      resources: Map[String, ResourceInformation],
      plugins: Option[PluginContainer]): T = {

    require(cpus > 0, "CPUs per task should be > 0")

    SparkEnv.get.blockManager.registerTask(taskAttemptId)
    // TODO SPARK-24874 允许基于分区创建 BarrierTaskContext，而不是基于阶段是否为屏障
    val taskContext = new TaskContextImpl(
      stageId,
      stageAttemptId, // stageAttemptId and stageAttemptNumber are semantically equal
      partitionId,
      taskAttemptId,
      attemptNumber,
      taskMemoryManager,
      localProperties,
      metricsSystem,
      metrics,
      cpus,
      resources)

    context = if (isBarrier) {
      new BarrierTaskContext(taskContext)
    } else {
      taskContext
    }

    InputFileBlockHolder.initialize()
    TaskContext.setTaskContext(context)
    taskThread = Thread.currentThread()

    if (_reasonIfKilled != null) {
      kill(interruptThread = false, _reasonIfKilled)
    }

    new CallerContext(
      "TASK",
      SparkEnv.get.conf.get(APP_CALLER_CONTEXT),
      appId,
      appAttemptId,
      jobId,
      Option(stageId),
      Option(stageAttemptId),
      Option(taskAttemptId),
      Option(attemptNumber)).setCurrentContext()

    plugins.foreach(_.onTaskStart())

    try {
      runTask(context)
    } catch {
      case e: Throwable =>
        // 捕获所有错误；运行任务失败回调，并重新抛出异常
        try {
          context.markTaskFailed(e)
        } catch {
          case t: Throwable =>
            e.addSuppressed(t)
        }
        context.markTaskCompleted(Some(e))
        throw e
    } finally {
      try {
        // 调用任务完成回调。如果 "markTaskCompleted" 被调用两次，第二次是无操作的
        context.markTaskCompleted(None)
      } finally {
        try {
          Utils.tryLogNonFatalError {
            // 释放此线程用于展开块的内存
            SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP)
            SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask(
              MemoryMode.OFF_HEAP)
            // 通知任何等待执行内存被释放的任务唤醒并尝试再次获取内存。
            // 这使得任务永远睡眠的情况变得不可能，因为没有其他任务来通知它。
            // 由于这样做是安全的但可能不是严格必要的，我们应该重新考虑是否可以在将来删除这个。
            val memoryManager = SparkEnv.get.memoryManager
            memoryManager.synchronized { memoryManager.notifyAll() }
          }
        } finally {
          // Though we unset the ThreadLocal here, the context member variable itself is still
          // queried directly in the TaskRunner to check for FetchFailedExceptions.
          TaskContext.unset()
          InputFileBlockHolder.unset()
        }
      }
    }
  }

  private var taskMemoryManager: TaskMemoryManager = _

  def setTaskMemoryManager(taskMemoryManager: TaskMemoryManager): Unit = {
    this.taskMemoryManager = taskMemoryManager
  }

  def runTask(context: TaskContext): T

  def preferredLocations: Seq[TaskLocation] = Nil

  // Map 输出跟踪器时期。将由 TaskSetManager 设置。
  var epoch: Long = -1

  // 任务上下文，在 run() 中初始化。
  @transient var context: TaskContext = _

  // 任务运行的实际线程（如果有的话）。在 run() 中初始化。
  @volatile @transient private var taskThread: Thread = _

  // 如果非空，此任务已被杀死，原因如指定。这在 kill() 被调用时上下文尚未初始化的情况下使用。
  @volatile @transient private var _reasonIfKilled: String = null

  protected var _executorDeserializeTimeNs: Long = 0
  protected var _executorDeserializeCpuTime: Long = 0

  /**
   * 如果已定义，此任务已被杀死，此选项包含原因。
   */
  def reasonIfKilled: Option[String] = Option(_reasonIfKilled)

  /**
   * 返回反序列化 RDD 和要运行的函数所花费的时间量。
   */
  def executorDeserializeTimeNs: Long = _executorDeserializeTimeNs
  def executorDeserializeCpuTime: Long = _executorDeserializeCpuTime

  /**
   * 收集此任务中使用的累加器的最新值。如果任务失败，过滤掉那些值不应包含在失败中的累加器。
   */
  def collectAccumulatorUpdates(taskFailed: Boolean = false): Seq[AccumulatorV2[_, _]] = {
    if (context != null) {
      // 注意：表示任务指标的内部累加器总是计算失败值
      context.taskMetrics.nonZeroInternalAccums() ++
        context.taskMetrics.externalAccums.filter(a => !taskFailed || a.countFailedValues)
      // 零值外部累加器可能仍然有用 ,例如 SQLMetrics，我们不应该过滤掉它们。
    } else {
      Seq.empty
    }
  }


  /**
   * 通过将中断标志设置为 true 来杀死任务。这依赖于上层 Spark 代码和用户代码
   * 正确处理该标志。此函数应该是幂等的，因此可以多次调用。
   * 如果 interruptThread 为 true，我们还将在任务的执行器线程上调用 Thread.interrupt()。
   */
  def kill(interruptThread: Boolean, reason: String): Unit = {
    require(reason != null)
    _reasonIfKilled = reason
    if (context != null) {
      context.markInterrupted(reason)
    }
    if (interruptThread && taskThread != null) {
      taskThread.interrupt()
    }
  }
}
