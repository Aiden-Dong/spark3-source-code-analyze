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

import scala.collection.mutable.Map

import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.AccumulatorV2

/**
 * 低级任务调度器接口，目前仅由 [[org.apache.spark.scheduler.TaskSchedulerImpl]] 实现。
 * 此接口允许插入不同的TaskScheduler。每个 TaskScheduler 为单个 SparkContext 调度任务。
 * 这些调度器从 DAGScheduler 接收每个阶段提交的一组任务，
 * 并负责将任务发送到集群、运行任务、在失败时重试以及缓解慢节点。
 * 它们将事件返回给 DAGScheduler。
 */
private[spark] trait TaskScheduler {

  private val appId = "spark-application-" + System.currentTimeMillis

  def rootPool: Pool

  def schedulingMode: SchedulingMode

  def start(): Unit

  // 在系统成功初始化后调用（通常在 Spark 上下文中）。
  // Yarn 使用此方法根据首选位置启动资源分配，等待执行器注册等。
  def postStartHook(): Unit = { }

  // 与集群断开连接。
  def stop(): Unit

  // 提交一个TaskSet 去调度执行
  // Submit a sequence of tasks to run.
  def submitTasks(taskSet: TaskSet): Unit

  // 杀死stage中的所有任务，并使该stage及所有依赖该stage的作业失败。
  // 如果后端不支持杀死任务，则抛出 UnsupportedOperationException 异常。
  def cancelTasks(stageId: Int, interruptThread: Boolean): Unit

  /**
   * 杀死一个task attempt。
   * 如果后端不支持杀死任务，则抛出 UnsupportedOperationException 异常。
   *
   * @return Whether the task was successfully killed.
   */
  def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean

  // 杀死阶段中所有正在运行的任务尝试。
  // 如果后端不支持杀死任务，则抛出 UnsupportedOperationException 异常。
  def killAllTaskAttempts(stageId: Int, interruptThread: Boolean, reason: String): Unit

  // 通知阶段对应的 `TaskSetManager`，某个分区已经完成，它们可以跳过运行该分区的任务。
  def notifyPartitionCompletion(stageId: Int, partitionId: Int): Unit

  // 设置用于上调的 DAG 调度器。确保在调用 submitTasks 之前设置此调度器。
  def setDAGScheduler(dagScheduler: DAGScheduler): Unit

  // 获取集群中用于作业大小调整的默认并行度级别提示。
  def defaultParallelism(): Int

  /**
   * 更新正在进行的任务和执行器指标的指标，并告知主节点 BlockManager 仍然活着。
   * 如果驱动程序知道给定的块管理器，则返回 true。否则，返回 false，表示块管理器应重新注册。
   */
  def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
      blockManagerId: BlockManagerId,
      executorUpdates: Map[(Int, Int), ExecutorMetrics]): Boolean

  /**
   * 获取与作业关联的应用程序 ID.
   *
   * @return An application ID
   */
  def applicationId(): String = appId

  /**
   * 处理一个正在下线的执行器。
   */
  def executorDecommission(executorId: String, decommissionInfo: ExecutorDecommissionInfo): Unit

  /**
   * 如果执行器已经下线，返回其对应的下线信息。
   */
  def getExecutorDecommissionState(executorId: String): Option[ExecutorDecommissionState]

  /**
   * 处理丢失的执行器。
   */
  def executorLost(executorId: String, reason: ExecutorLossReason): Unit

  /**
   * 处理移除的工作节点。
   */
  def workerRemoved(workerId: String, host: String, message: String): Unit

  /**
   * 获取与作业关联的应用程序尝试 ID。
   *
   * @return An application's Attempt ID
   */
  def applicationAttemptId(): Option[String]

}
