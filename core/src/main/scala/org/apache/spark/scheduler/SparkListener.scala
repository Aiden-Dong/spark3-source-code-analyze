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

import java.util.Properties
import javax.annotation.Nullable

import scala.collection.Map

import com.fasterxml.jackson.annotation.JsonTypeInfo

import org.apache.spark.TaskEndReason
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.storage.{BlockManagerId, BlockUpdatedInfo}

@DeveloperApi
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "Event")
trait SparkListenerEvent {
  /* Whether output this event to the event log */
  protected[spark] def logEvent: Boolean = true
}

@DeveloperApi
case class SparkListenerStageSubmitted(stageInfo: StageInfo, properties: Properties = null)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerStageCompleted(stageInfo: StageInfo) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerTaskStart(stageId: Int, stageAttemptId: Int, taskInfo: TaskInfo)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerTaskGettingResult(taskInfo: TaskInfo) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerSpeculativeTaskSubmitted(
    stageId: Int,
    stageAttemptId: Int = 0)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerTaskEnd(
    stageId: Int,
    stageAttemptId: Int,
    taskType: String,
    reason: TaskEndReason,
    taskInfo: TaskInfo,
    taskExecutorMetrics: ExecutorMetrics,
    // may be null if the task has failed
    @Nullable taskMetrics: TaskMetrics)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerJobStart(
    jobId: Int,
    time: Long,
    stageInfos: Seq[StageInfo],
    properties: Properties = null)
  extends SparkListenerEvent {
  // Note: this is here for backwards-compatibility with older versions of this event which
  // only stored stageIds and not StageInfos:
  val stageIds: Seq[Int] = stageInfos.map(_.stageId)
}

@DeveloperApi
case class SparkListenerJobEnd(
    jobId: Int,
    time: Long,
    jobResult: JobResult)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerEnvironmentUpdate(environmentDetails: Map[String, Seq[(String, String)]])
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerBlockManagerAdded(
    time: Long,
    blockManagerId: BlockManagerId,
    maxMem: Long,
    maxOnHeapMem: Option[Long] = None,
    maxOffHeapMem: Option[Long] = None) extends SparkListenerEvent {
}

@DeveloperApi
case class SparkListenerBlockManagerRemoved(time: Long, blockManagerId: BlockManagerId)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerUnpersistRDD(rddId: Int) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerExecutorAdded(time: Long, executorId: String, executorInfo: ExecutorInfo)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerExecutorRemoved(time: Long, executorId: String, reason: String)
  extends SparkListenerEvent

@DeveloperApi
@deprecated("use SparkListenerExecutorExcluded instead", "3.1.0")
case class SparkListenerExecutorBlacklisted(
    time: Long,
    executorId: String,
    taskFailures: Int)
  extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerExecutorExcluded(
    time: Long,
    executorId: String,
    taskFailures: Int)
  extends SparkListenerEvent

@deprecated("use SparkListenerExecutorExcludedForStage instead", "3.1.0")
@DeveloperApi
case class SparkListenerExecutorBlacklistedForStage(
    time: Long,
    executorId: String,
    taskFailures: Int,
    stageId: Int,
    stageAttemptId: Int)
  extends SparkListenerEvent


@DeveloperApi
@Since("3.1.0")
case class SparkListenerExecutorExcludedForStage(
    time: Long,
    executorId: String,
    taskFailures: Int,
    stageId: Int,
    stageAttemptId: Int)
  extends SparkListenerEvent

@deprecated("use SparkListenerNodeExcludedForStage instead", "3.1.0")
@DeveloperApi
case class SparkListenerNodeBlacklistedForStage(
    time: Long,
    hostId: String,
    executorFailures: Int,
    stageId: Int,
    stageAttemptId: Int)
  extends SparkListenerEvent


@DeveloperApi
@Since("3.1.0")
case class SparkListenerNodeExcludedForStage(
    time: Long,
    hostId: String,
    executorFailures: Int,
    stageId: Int,
    stageAttemptId: Int)
  extends SparkListenerEvent

@deprecated("use SparkListenerExecutorUnexcluded instead", "3.1.0")
@DeveloperApi
case class SparkListenerExecutorUnblacklisted(time: Long, executorId: String)
  extends SparkListenerEvent


@DeveloperApi
case class SparkListenerExecutorUnexcluded(time: Long, executorId: String)
  extends SparkListenerEvent

@deprecated("use SparkListenerNodeExcluded instead", "3.1.0")
@DeveloperApi
case class SparkListenerNodeBlacklisted(
    time: Long,
    hostId: String,
    executorFailures: Int)
  extends SparkListenerEvent


@DeveloperApi
@Since("3.1.0")
case class SparkListenerNodeExcluded(
    time: Long,
    hostId: String,
    executorFailures: Int)
  extends SparkListenerEvent

@deprecated("use SparkListenerNodeUnexcluded instead", "3.1.0")
@DeveloperApi
case class SparkListenerNodeUnblacklisted(time: Long, hostId: String)
  extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerNodeUnexcluded(time: Long, hostId: String)
  extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerUnschedulableTaskSetAdded(
  stageId: Int,
  stageAttemptId: Int) extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerUnschedulableTaskSetRemoved(
  stageId: Int,
  stageAttemptId: Int) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerBlockUpdated(blockUpdatedInfo: BlockUpdatedInfo) extends SparkListenerEvent

@DeveloperApi
@Since("3.2.0")
case class SparkListenerMiscellaneousProcessAdded(time: Long, processId: String,
    info: MiscellaneousProcessDetails) extends SparkListenerEvent

/**
 * Periodic updates from executors.
 * @param execId executor id
 * @param accumUpdates sequence of (taskId, stageId, stageAttemptId, accumUpdates)
 * @param executorUpdates executor level per-stage metrics updates
 *
 * @since 3.1.0
 */
@DeveloperApi
case class SparkListenerExecutorMetricsUpdate(
    execId: String,
    accumUpdates: Seq[(Long, Int, Int, Seq[AccumulableInfo])],
    executorUpdates: Map[(Int, Int), ExecutorMetrics] = Map.empty)
  extends SparkListenerEvent

/**
 * Peak metric values for the executor for the stage, written to the history log at stage
 * completion.
 * @param execId executor id
 * @param stageId stage id
 * @param stageAttemptId stage attempt
 * @param executorMetrics executor level metrics peak values
 */
@DeveloperApi
case class SparkListenerStageExecutorMetrics(
    execId: String,
    stageId: Int,
    stageAttemptId: Int,
    executorMetrics: ExecutorMetrics)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerApplicationStart(
    appName: String,
    appId: Option[String],
    time: Long,
    sparkUser: String,
    appAttemptId: Option[String],
    driverLogs: Option[Map[String, String]] = None,
    driverAttributes: Option[Map[String, String]] = None) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerApplicationEnd(time: Long) extends SparkListenerEvent

/**
 * An internal class that describes the metadata of an event log.
 */
@DeveloperApi
case class SparkListenerLogStart(sparkVersion: String) extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerResourceProfileAdded(resourceProfile: ResourceProfile)
  extends SparkListenerEvent

/**
 * 用于监听来自 Spark 调度器事件的接口。
 * 大多数应用程序应该直接扩展 SparkListener 或 SparkFirehoseListener，而不是实现这个类。
 * 请注意，这是一个内部接口，可能会在不同的 Spark 版本中发生变化。
 */
private[spark] trait SparkListenerInterface {

  // 当stage提交时被调用
  def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit

  // 在一个stage成功完成或失败时调用，并提供关于已完成stage的信息。.
  def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit

  // 当task开始时被调用
  def onTaskStart(taskStart: SparkListenerTaskStart): Unit

  // 在task开始远程获取其结果时调用（对于不需要远程获取结果的任务，不会调用此方法）。
  def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit

  // 当task结束时被调用
  def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit

  // 当job开始时被调用
  def onJobStart(jobStart: SparkListenerJobStart): Unit

  // 当job结束时被调用
  def onJobEnd(jobEnd: SparkListenerJobEnd): Unit

  // 当环境变量更新时被调用
  def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit

  // 当加入新的块时被调用
  def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit

  // 当移除块时被调用
  def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit

  // 当driver接收到块更新信息时调用。
  def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit

  // 当应用程序手动取消持久化一个 RDD 时调用。
  def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit

  // 当应用程序开始时被调用
  def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit

  // 当应用程序结束时被调用
  def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit

  // 当Driver在心跳中从Executor接收到任务指标时调用。
  def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit

  /**
   * 在给定的（执行器，阶段）组合的峰值内存指标时调用。
   * 请注意，这仅在从Event日志读取时（如在History Server中）存在，并且从不在实时应用程序中调用。
   */
  def onStageExecutorMetrics(executorMetrics: SparkListenerStageExecutorMetrics): Unit

  // 新的Executor注册到Driver时被调用
  def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit

  // Executor 退出时被调用
  def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit

  // 当driver将某个executor排除在 Spark 应用程序之外时调用。.
  @deprecated("use onExecutorExcluded instead", "3.1.0")
  def onExecutorBlacklisted(executorBlacklisted: SparkListenerExecutorBlacklisted): Unit

  // 在驱动程序将某个执行器排除在 Spark 应用程序之外时调用。.
  def onExecutorExcluded(executorExcluded: SparkListenerExecutorExcluded): Unit

  //在driver将某个executor排除在某个stage之外时调用。.
  @deprecated("use onExecutorExcludedForStage instead", "3.1.0")
  def onExecutorBlacklistedForStage(
      executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage): Unit

  // 在driver将某个executor排除在某个stage之外时调用。.
  def onExecutorExcludedForStage(
      executorExcludedForStage: SparkListenerExecutorExcludedForStage): Unit

  // 在driver将某个node排除在某个stage之外时调用。
  @deprecated("use onNodeExcludedForStage instead", "3.1.0")
  def onNodeBlacklistedForStage(nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage): Unit

  // 在driver将某个node排除在某个stage之外时调用。
  def onNodeExcludedForStage(nodeExcludedForStage: SparkListenerNodeExcludedForStage): Unit

  // 当驱动程序重新启用之前被排除的executor时调用。
  @deprecated("use onExecutorUnexcluded instead", "3.1.0")
  def onExecutorUnblacklisted(executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit

  // 当驱动程序重新启用之前被排除的executor时调用。
  def onExecutorUnexcluded(executorUnexcluded: SparkListenerExecutorUnexcluded): Unit

  // 当driver将某个node排除在 Spark 应用程序之外时调用。
  @deprecated("use onNodeExcluded instead", "3.1.0")
  def onNodeBlacklisted(nodeBlacklisted: SparkListenerNodeBlacklisted): Unit

  //  当driver将某个node排除在 Spark 应用程序之外时调用。
  def onNodeExcluded(nodeExcluded: SparkListenerNodeExcluded): Unit

  // 当驱动程序重新启用之前被排除的node时调用。
  @deprecated("use onNodeUnexcluded instead", "3.1.0")
  def onNodeUnblacklisted(nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit

  // 当驱动程序重新启用之前被排除的node时调用。
  def onNodeUnexcluded(nodeUnexcluded: SparkListenerNodeUnexcluded): Unit

  // 当由于 excludeOnFailure 和启用动态分配导致任务集无法调度时调用。
  def onUnschedulableTaskSetAdded(
      unschedulableTaskSetAdded: SparkListenerUnschedulableTaskSetAdded): Unit

  // 当一个无法调度的任务集变得可调度且动态分配已启用时调用。
  def onUnschedulableTaskSetRemoved(
      unschedulableTaskSetRemoved: SparkListenerUnschedulableTaskSetRemoved): Unit

  // 在提交推测任务时调用。
  def onSpeculativeTaskSubmitted(speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit

  // 在发布其他事件，如特定于 SQL 的事件时调用。
  def onOtherEvent(event: SparkListenerEvent): Unit

  // 在资源配置文件被添加到管理器时调用。
  def onResourceProfileAdded(event: SparkListenerResourceProfileAdded): Unit
}


/**
 * :: DeveloperApi ::
 * A default implementation for `SparkListenerInterface` that has no-op implementations for
 * all callbacks.
 *
 * Note that this is an internal interface which might change in different Spark releases.
 */
@DeveloperApi
abstract class SparkListener extends SparkListenerInterface {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = { }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = { }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = { }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = { }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = { }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = { }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = { }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = { }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = { }

  override def onBlockManagerRemoved(
      blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = { }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = { }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = { }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = { }

  override def onExecutorMetricsUpdate(
      executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = { }

  override def onStageExecutorMetrics(
      executorMetrics: SparkListenerStageExecutorMetrics): Unit = { }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = { }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = { }

  override def onExecutorBlacklisted(
      executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = { }
  override def onExecutorExcluded(
      executorExcluded: SparkListenerExecutorExcluded): Unit = { }

  override def onExecutorBlacklistedForStage(
      executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage): Unit = { }
  override def onExecutorExcludedForStage(
      executorExcludedForStage: SparkListenerExecutorExcludedForStage): Unit = { }

  override def onNodeBlacklistedForStage(
      nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage): Unit = { }
  override def onNodeExcludedForStage(
      nodeExcludedForStage: SparkListenerNodeExcludedForStage): Unit = { }

  override def onExecutorUnblacklisted(
      executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = { }
  override def onExecutorUnexcluded(
      executorUnexcluded: SparkListenerExecutorUnexcluded): Unit = { }

  override def onNodeBlacklisted(
      nodeBlacklisted: SparkListenerNodeBlacklisted): Unit = { }
  override def onNodeExcluded(
      nodeExcluded: SparkListenerNodeExcluded): Unit = { }

  override def onNodeUnblacklisted(
      nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit = { }
  override def onNodeUnexcluded(
      nodeUnexcluded: SparkListenerNodeUnexcluded): Unit = { }

  override def onUnschedulableTaskSetAdded(
      unschedulableTaskSetAdded: SparkListenerUnschedulableTaskSetAdded): Unit = { }

  override def onUnschedulableTaskSetRemoved(
      unschedulableTaskSetRemoved: SparkListenerUnschedulableTaskSetRemoved): Unit = { }

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = { }

  override def onSpeculativeTaskSubmitted(
      speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit = { }

  override def onOtherEvent(event: SparkListenerEvent): Unit = { }

  override def onResourceProfileAdded(event: SparkListenerResourceProfileAdded): Unit = { }
}
