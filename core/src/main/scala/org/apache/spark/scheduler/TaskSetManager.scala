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

import java.io.NotSerializableException
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}

import scala.collection.immutable.Map
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.math.max
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.scheduler.SchedulingMode._
import org.apache.spark.util.{AccumulatorV2, Clock, LongAccumulator, SystemClock, Utils}
import org.apache.spark.util.collection.MedianHeap

/**
 * 在 TaskSchedulerImpl中安排单个TaskSet中的任务。该类跟踪每个任务，如果它们失败(最多限制次数)，则重试任务，
 * 并通过延迟调度处理此TaskSet的位置感知调度。
 *
 * 它的主要接口是resourceOffer，它询问TaskSet是否想要在一个节点上运行一个任务，以及handleSuccessfulTask/handleFailedTask，
 * 它告诉它其中一个任务的状态已更改(例如已完成/失败)。
 *
 * 线程安全性：此类设计为仅从持有TaskScheduler锁的代码中调用(例如其事件处理程序)。不应该从其他线程调用它。
 *
 * @param sched           与TaskSetManager相关联的TaskSchedulerImpl。
 * @param taskSet         管理调度的TaskSet。
 * @param maxTaskFailures 如果任何特定任务失败次数达到这个数字，整个任务集将被中止。
 */
private[spark] class TaskSetManager(
    sched: TaskSchedulerImpl,
    val taskSet: TaskSet,        // 当前处于调度Stage的Task集合
    val maxTaskFailures: Int,    // Task 的最大失败次数
    healthTracker: Option[HealthTracker] = None,
    clock: Clock = new SystemClock()) extends Schedulable with Logging {

  private val conf = sched.sc.conf

  // SPARK-21563 复制jar文件，使它们在整个TaskSet中保持一致。
  private val addedJars = HashMap[String, Long](sched.sc.addedJars.toSeq: _*)
  private val addedFiles = HashMap[String, Long](sched.sc.addedFiles.toSeq: _*)
  private val addedArchives = HashMap[String, Long](sched.sc.addedArchives.toSeq: _*)

  val maxResultSize = conf.get(config.MAX_RESULT_SIZE)

  // 用于闭包和任务的序列化器。
  val env = SparkEnv.get
  val ser = env.closureSerializer.newInstance()

  // 当前Stage的task集合
  val tasks = taskSet.tasks

  // 判断当前Stage是否是 ShuffledMapStage
  private val isShuffleMapTasks = tasks(0).isInstanceOf[ShuffleMapTask]

  // task 分区号跟taskId绑定
  private[scheduler] val partitionToIndex = tasks.zipWithIndex
    .map { case (t, idx) => t.partitionId -> idx }.toMap

  val numTasks = tasks.length
  val copiesRunning = new Array[Int](numTasks)

  // 是否启用推测执行
  val speculationEnabled = conf.get(SPECULATION_ENABLED)
  // 开始猜测的任务的分位数
  val speculationQuantile = conf.get(SPECULATION_QUANTILE)

  // 推测执行的任务触发阈值
  val speculationMultiplier = conf.get(SPECULATION_MULTIPLIER)

  // 最少开始启用推测执行的任务数量
  val minFinishedForSpeculation = math.max((speculationQuantile * numTasks).floor.toInt, 1)

  // 用户提供的启动猜测的时间阈值阈值，无论是否达到了分位数。
  val speculationTaskDurationThresOpt = conf.get(SPECULATION_TASK_DURATION_THRESHOLD)
  // SPARK-29976：仅当阶段中的任务总数小于或等于单个执行器上的插槽数时，任务管理器才会在任务的持续时间超过给定阈值时运行这些任务的猜测。
  //              通过这种方式，我们不会过于激进地进行猜测，但仍然处理基本情况。
  // SPARK-30417：对于独立模式，可能未在Spark配置中设置每个执行器的核心数，则该配置的值默认为1。
  //               但是，执行器将使用工作节点上的所有核心。
  //               因此，CPUS_PER_TASK 可以在没有设置核心数的情况下大于1。
  //               为了处理这种情况，在我们不知道执行器核心数时，我们将插槽数设置为1。
  // TODO：对于独立模式，使用实际插槽数。
  val speculationTasksLessEqToSlots = {
    val rpId = taskSet.resourceProfileId
    val resourceProfile = sched.sc.resourceProfileManager.resourceProfileFromId(rpId)
    val slots = if (!resourceProfile.isCoresLimitKnown) {
      1
    } else {
      resourceProfile.maxTasksPerExecutor(conf)
    }
    numTasks <= slots
  }

  private val executorDecommissionKillInterval = conf.get(EXECUTOR_DECOMMISSION_KILL_INTERVAL)
    .map(TimeUnit.SECONDS.toMillis)

  // 对于每个任务，跟踪任务的一个副本是否成功。
  // 如果任务由于获取失败而失败，任务也将被标记为“成功”，
  // 在这种情况下，它不应重新运行，因为缺少的映射数据首先需要重新生成。
  val successful = new Array[Boolean](numTasks)
  // 失败次数记录
  private val numFailures = new Array[Int](numTasks)


  // 当任务被其他尝试任务杀死时，将任务的tid添加到此HashSet中。
  // 当我们将spark.speculation设置为true时会发生这种情况。
  // 被其他任务杀死的任务在execotr loss时不应该重新提交。
  private val killedByOtherAttempt = new HashSet[Long]


  // 所有Task的信息
  val taskAttempts = Array.fill[List[TaskInfo]](numTasks)(Nil)
  private[scheduler] var tasksSuccessful = 0

  val weight = 1
  val minShare = 0
  var priority = taskSet.priority
  val stageId = taskSet.stageId
  val name = "TaskSet_" + taskSet.id
  var parent: Pool = null
  private var totalResultSize = 0L
  private var calculatedTasks = 0

  private[scheduler] val taskSetExcludelistHelperOpt: Option[TaskSetExcludelist] = {
    healthTracker.map { _ =>
      new TaskSetExcludelist(sched.sc.listenerBus, conf, stageId, taskSet.stageAttemptId, clock)
    }
  }

  private[scheduler] val runningTasksSet = new HashSet[Long]

  override def runningTasks: Int = runningTasksSet.size

  def someAttemptSucceeded(tid: Long): Boolean = {
    successful(taskInfos(tid).index)
  }

  // 一旦不应再为此任务集管理器启动更多任务时，该值为 true。
  // TaskSetManagers 一旦每个任务的至少一次尝试成功完成，或任务集被中止（例如被终止），便进入 zombie 状态。
  // TaskSetManagers 将保持在 zombie 状态，直到所有任务结束运行；
  // 我们保留处于 zombie 状态的 TaskSetManagers，以便继续跟踪和计算正在运行的任务。
  // TODO: 当任务集管理器进入 zombie 状态时，我们应该终止所有正在运行的任务尝试。
  private[scheduler] var isZombie = false

  // Whether the taskSet run tasks from a barrier stage. Spark must launch all the tasks at the
  // same time for a barrier stage.
  private[scheduler] def isBarrier = taskSet.tasks.nonEmpty && taskSet.tasks(0).isBarrier

  // Barrier tasks that are pending to launch in a single resourceOffers round. Tasks will only get
  // launched when all tasks are added to this pending list in a single round. Otherwise, we'll
  // revert everything we did during task scheduling.
  private[scheduler] val barrierPendingLaunchTasks = new HashMap[Int, BarrierPendingLaunchTask]()

  // Record the last log time of the barrier TaskSetManager that failed to get all tasks launched.
  private[scheduler] var lastResourceOfferFailLogTime = clock.getTimeMillis()

  // Store tasks waiting to be scheduled by locality preferences
  private[scheduler] val pendingTasks = new PendingTasksByLocality()


  // 推测执行任务集合
  private[scheduler] val speculatableTasks = new HashSet[Int]

  // 等待推测执行任务
  private[scheduler] val pendingSpeculatableTasks = new PendingTasksByLocality()

  // Task index, start and finish time for each task attempt (indexed by task ID)
  private[scheduler] val taskInfos = new HashMap[Long, TaskInfo]

  // 使用 MedianHeap 来记录成功任务的持续时间，这样我们就知道何时启动推测性任务。
  // 只有在启用推测时才使用此功能，以避免在堆不会被使用的情况下插入元素所带来的开销。
  val successfulTaskDurations = new MedianHeap()

  // How frequently to reprint duplicate exceptions in full, in milliseconds
  val EXCEPTION_PRINT_INTERVAL = conf.getLong("spark.logging.exceptionPrintInterval", 10000)

  // Map of recent exceptions (identified by string representation and top stack frame) to
  // duplicate count (how many times the same exception has appeared) and time the full exception
  // was printed. This should ideally be an LRU map that can drop old exceptions automatically.
  private val recentExceptions = HashMap[String, (Int, Long)]()

  // 确定当前的映射输出跟踪器的 epoch，并将其设置到所有任务上。
  val epoch = sched.mapOutputTracker.getEpoch
  logDebug("Epoch for " + taskSet + ": " + epoch)
  for (t <- tasks) {
    t.epoch = epoch
  }

  // Add all our tasks to the pending lists. We do this in reverse order
  // of task index so that tasks with low indices get launched first.
  addPendingTasks()

  private def addPendingTasks(): Unit = {
    val (_, duration) = Utils.timeTakenMs {
      for (i <- (0 until numTasks).reverse) {
        addPendingTask(i, resolveRacks = false)
      }
      // Resolve the rack for each host. This can be slow, so de-dupe the list of hosts,
      // and assign the rack to all relevant task indices.
      val (hosts, indicesForHosts) = pendingTasks.forHost.toSeq.unzip
      val racks = sched.getRacksForHosts(hosts)
      racks.zip(indicesForHosts).foreach {
        case (Some(rack), indices) =>
          pendingTasks.forRack.getOrElseUpdate(rack, new ArrayBuffer) ++= indices
        case (None, _) => // no rack, nothing to do
      }
    }
    logDebug(s"Adding pending tasks took $duration ms")
  }

  /**
   * Track the set of locality levels which are valid given the tasks locality preferences and
   * the set of currently available executors.  This is updated as executors are added and removed.
   * This allows a performance optimization, of skipping levels that aren't relevant (e.g., skip
   * PROCESS_LOCAL if no tasks could be run PROCESS_LOCAL for the current set of executors).
   */
  private[scheduler] var myLocalityLevels = computeValidLocalityLevels()

  // Delay scheduling variables: we keep track of our current locality level and the time we
  // last reset the locality wait timer, and move up a level when localityWaits[curLevel] expires.
  // We then move down if we manage to launch a "more local" task when resetting the timer
  private val legacyLocalityWaitReset = conf.get(LEGACY_LOCALITY_WAIT_RESET)
  private var currentLocalityIndex = 0  // 我们当前本地性级别在 validLocalityLevels 中的索引。
  private var lastLocalityWaitResetTime = clock.getTimeMillis()  // Time we last reset locality wait

  // Time to wait at each level
  private[scheduler] var localityWaits = myLocalityLevels.map(getLocalityWait)

  override def schedulableQueue: ConcurrentLinkedQueue[Schedulable] = null

  override def schedulingMode: SchedulingMode = SchedulingMode.NONE

  private[scheduler] var emittedTaskSizeWarning = false

  /** 利用数据本地性计算所在节点所在位置 */
  private[spark] def addPendingTask(
      index: Int,
      resolveRacks: Boolean = true,
      speculatable: Boolean = false): Unit = {
    // A zombie TaskSetManager may reach here while handling failed task.
    if (isZombie) return
    val pendingTaskSetToAddTo = if (speculatable) pendingSpeculatableTasks else pendingTasks
    // 获取数据本地性
    for (loc <- tasks(index).preferredLocations) {
      // 进程本地性计算
      loc match {
        case e: ExecutorCacheTaskLocation =>
          pendingTaskSetToAddTo.forExecutor.getOrElseUpdate(e.executorId, new ArrayBuffer) += index
        case e: HDFSCacheTaskLocation =>
          val exe = sched.getExecutorsAliveOnHost(loc.host)
          exe match {
            case Some(set) =>
              for (e <- set) {
                pendingTaskSetToAddTo.forExecutor.getOrElseUpdate(e, new ArrayBuffer) += index
              }
              logInfo(s"Pending task $index has a cached location at ${e.host} " +
                ", where there are executors " + set.mkString(","))
            case None => logDebug(s"Pending task $index has a cached location at ${e.host} " +
              ", but there are no executors alive there.")
          }
        case _ =>
      }

      // 节点本地性计算
      pendingTaskSetToAddTo.forHost.getOrElseUpdate(loc.host, new ArrayBuffer) += index

      // 机架本地性计算
      if (resolveRacks) {
        sched.getRackForHost(loc.host).foreach { rack =>
          pendingTaskSetToAddTo.forRack.getOrElseUpdate(rack, new ArrayBuffer) += index
        }
      }
    }

    if (tasks(index).preferredLocations == Nil) {
      pendingTaskSetToAddTo.noPrefs += index
    }

    pendingTaskSetToAddTo.all += index
  }

  /**
   * 从给定列表中出队一个待处理任务，并返回其索引。
   * 如果列表为空，则返回 None。
   * 该方法还会清理已经启动的列表中的任何任务，因为我们希望这种清理是懒惰进行的。
   */
  private def dequeueTaskFromList(
      execId: String,
      host: String,
      list: ArrayBuffer[Int],
      speculative: Boolean = false): Option[Int] = {
    var indexOffset = list.size
    while (indexOffset > 0) {
      indexOffset -= 1
      val index = list(indexOffset)
      if (!isTaskExcludededOnExecOrNode(index, execId, host) &&
          !(speculative && hasAttemptOnHost(index, host))) {
        // This should almost always be list.trimEnd(1) to remove tail
        list.remove(indexOffset)
        // Speculatable task should only be launched when at most one copy of the
        // original task is running
        if (!successful(index)) {
          if (copiesRunning(index) == 0 && !barrierPendingLaunchTasks.contains(index)) {
            return Some(index)
          } else if (speculative && copiesRunning(index) == 1) {
            return Some(index)
          }
        }
      }
    }
    None
  }

  /** Check whether a task once ran an attempt on a given host */
  private def hasAttemptOnHost(taskIndex: Int, host: String): Boolean = {
    taskAttempts(taskIndex).exists(_.host == host)
  }

  private def isTaskExcludededOnExecOrNode(index: Int, execId: String, host: String): Boolean = {
    taskSetExcludelistHelperOpt.exists { excludeList =>
      excludeList.isNodeExcludedForTask(host, index) ||
        excludeList.isExecutorExcludedForTask(execId, index)
    }
  }

  /**
   * 为给定节点出队一个待处理任务，并返回其索引和本地性级别。
   * 只搜索符合给定本地性约束的任务。
   *
   * @return 一个选项，包含（任务在任务集中的索引，本地性级别，是否为推测性任务？）
   */
  private def dequeueTask(
      execId: String,
      host: String,
      maxLocality: TaskLocality.Value): Option[(Int, TaskLocality.Value, Boolean)] = {
    // 首先尝试调度常规任务；如果返回 None，则调度一个推测性任务。
    dequeueTaskHelper(execId, host, maxLocality, false).orElse(
      dequeueTaskHelper(execId, host, maxLocality, true))
  }

  protected def dequeueTaskHelper(
      execId: String,
      host: String,
      maxLocality: TaskLocality.Value,
      speculative: Boolean): Option[(Int, TaskLocality.Value, Boolean)] = {
    if (speculative && speculatableTasks.isEmpty) {
      return None
    }

    // 获取当前等待调度的tasks
    val pendingTaskSetToUse = if (speculative) pendingSpeculatableTasks else pendingTasks

    def dequeue(list: ArrayBuffer[Int]): Option[Int] = {
      val task = dequeueTaskFromList(execId, host, list, speculative)
      if (speculative && task.isDefined) {
        speculatableTasks -= task.get
      }
      task
    }

    dequeue(pendingTaskSetToUse.forExecutor.getOrElse(execId, ArrayBuffer())).foreach { index =>
      return Some((index, TaskLocality.PROCESS_LOCAL, speculative))
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NODE_LOCAL)) {
      dequeue(pendingTaskSetToUse.forHost.getOrElse(host, ArrayBuffer())).foreach { index =>
        return Some((index, TaskLocality.NODE_LOCAL, speculative))
      }
    }

    // Look for noPref tasks after NODE_LOCAL for minimize cross-rack traffic
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NO_PREF)) {
      dequeue(pendingTaskSetToUse.noPrefs).foreach { index =>
        return Some((index, TaskLocality.PROCESS_LOCAL, speculative))
      }
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.RACK_LOCAL)) {
      for {
        rack <- sched.getRackForHost(host)
        index <- dequeue(pendingTaskSetToUse.forRack.getOrElse(rack, ArrayBuffer()))
      } {
        return Some((index, TaskLocality.RACK_LOCAL, speculative))
      }
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.ANY)) {
      dequeue(pendingTaskSetToUse.all).foreach { index =>
        return Some((index, TaskLocality.ANY, speculative))
      }
    }
    None
  }

  private[scheduler] def resetDelayScheduleTimer(
      minLocality: Option[TaskLocality.TaskLocality]): Unit = {
    lastLocalityWaitResetTime = clock.getTimeMillis()
    for (locality <- minLocality) {
      currentLocalityIndex = getLocalityIndex(locality)
    }
  }

  /**
   * 通过找到一个任务来响应调度器提供的单个执行器资源
   *
   * 注意：此函数要么被调用时带有 maxLocality，此值会被延迟调度算法调整，
   * 要么带有特殊的 NO_PREF 本地性，此值不会被修改
   *
   * @param execId 提供的资源的执行器 ID
   * @param host 提供的资源的主机 ID
   * @param maxLocality 我们希望调度任务的最大本地性
   * @param taskCpus 任务所需的 CPU 数量
   * @param taskResourceAssignments 任务的资源分配
   *
   * @return 包含以下内容的 Triple：
   *         (如果有已启动的任务，返回 TaskDescription，
   *         因延迟调度而被拒绝的资源？，
   *         被出队的任务索引)
   */
  @throws[TaskNotSerializableException]
  def resourceOffer(
      execId: String,
      host: String,
      maxLocality: TaskLocality.TaskLocality,
      taskCpus: Int = sched.CPUS_PER_TASK,
      taskResourceAssignments: Map[String, ResourceInformation] = Map.empty)
    : (Option[TaskDescription], Boolean, Int) = {

    val offerExcluded = taskSetExcludelistHelperOpt.exists { excludeList =>
      excludeList.isNodeExcludedForTaskSet(host) ||
        excludeList.isExecutorExcludedForTaskSet(execId)
    }

    if (!isZombie && !offerExcluded) {
      val curTime = clock.getTimeMillis()

      var allowedLocality = maxLocality

      if (maxLocality != TaskLocality.NO_PREF) {
        allowedLocality = getAllowedLocalityLevel(curTime)
        if (allowedLocality > maxLocality) {
          // We're not allowed to search for farther-away tasks
          allowedLocality = maxLocality
        }
      }

      var dequeuedTaskIndex: Option[Int] = None

      // 从队列中取出一个任务来
      // 注意可能会获取不到
      val taskDescription = dequeueTask(execId, host, allowedLocality)
          .map {
            case (index, taskLocality, speculative) =>
              dequeuedTaskIndex = Some(index)
              if (legacyLocalityWaitReset && maxLocality != TaskLocality.NO_PREF) {
                resetDelayScheduleTimer(Some(taskLocality))
              }
              if (isBarrier) {
                barrierPendingLaunchTasks(index) =
                  BarrierPendingLaunchTask(
                    execId,
                    host,
                    index,
                    taskLocality,
                    taskResourceAssignments)
                // return null since the TaskDescription for the barrier task is not ready yet
                null
              } else {
                // 如果获取成功。则当前任务做运行前准备
                prepareLaunchingTask(
                  execId,
                  host,
                  index,
                  taskLocality,
                  speculative,
                  taskCpus,
                  taskResourceAssignments,
                  curTime)
              }
          }
      val hasPendingTasks = pendingTasks.all.nonEmpty || pendingSpeculatableTasks.all.nonEmpty
      val hasScheduleDelayReject =
        taskDescription.isEmpty &&
          maxLocality == TaskLocality.ANY &&
          hasPendingTasks
      (taskDescription, hasScheduleDelayReject, dequeuedTaskIndex.getOrElse(-1))
    } else {
      (None, false, -1)
    }
  }

  def prepareLaunchingTask(
      execId: String,
      host: String,
      index: Int,
      taskLocality: TaskLocality.Value,
      speculative: Boolean,
      taskCpus: Int,
      taskResourceAssignments: Map[String, ResourceInformation],
      launchTime: Long): TaskDescription = {
    // Found a task; do some bookkeeping and return a task description
    val task = tasks(index)    // 获取到调度的 Task
    val taskId = sched.newTaskId()
    // Do various bookkeeping
    copiesRunning(index) += 1
    val attemptNum = taskAttempts(index).size
    val info = new TaskInfo(
      taskId, index, attemptNum, task.partitionId, launchTime,
      execId, host, taskLocality, speculative)

    taskInfos(taskId) = info
    taskAttempts(index) = info :: taskAttempts(index)
    // Serialize and return the task
    val serializedTask: ByteBuffer = try {
      ser.serialize(task)
    } catch {
      // If the task cannot be serialized, then there's no point to re-attempt the task,
      // as it will always fail. So just abort the whole task-set.
      case NonFatal(e) =>
        val msg = s"Failed to serialize task $taskId, not attempting to retry it."
        logError(msg, e)
        abort(s"$msg Exception during serialization: $e")
        throw SparkCoreErrors.failToSerializeTaskError(e)
    }
    if (serializedTask.limit() > TaskSetManager.TASK_SIZE_TO_WARN_KIB * 1024 &&
      !emittedTaskSizeWarning) {
      emittedTaskSizeWarning = true
      logWarning(s"Stage ${task.stageId} contains a task of very large size " +
        s"(${serializedTask.limit() / 1024} KiB). The maximum recommended task size is " +
        s"${TaskSetManager.TASK_SIZE_TO_WARN_KIB} KiB.")
    }
    addRunningTask(taskId)

    // We used to log the time it takes to serialize the task, but task size is already
    // a good proxy to task serialization time.
    // val timeTaken = clock.getTime() - startTime
    val tName = taskName(taskId)
    logInfo(s"Starting $tName ($host, executor ${info.executorId}, " +
      s"partition ${task.partitionId}, $taskLocality, ${serializedTask.limit()} bytes) " +
      s"taskResourceAssignments ${taskResourceAssignments}")

    // 调度作业运行
    sched.dagScheduler.taskStarted(task, info)

    new TaskDescription(
      taskId,
      attemptNum,
      execId,
      tName,
      index,
      task.partitionId,
      addedFiles,
      addedJars,
      addedArchives,
      task.localProperties,
      taskCpus,
      taskResourceAssignments,
      serializedTask)
  }

  def taskName(tid: Long): String = {
    val info = taskInfos.get(tid)
    assert(info.isDefined, s"Can not find TaskInfo for task (TID $tid)")
    s"task ${info.get.id} in stage ${taskSet.id} (TID $tid)"
  }

  private def maybeFinishTaskSet(): Unit = {
    if (isZombie && runningTasks == 0) {
      sched.taskSetFinished(this)
      if (tasksSuccessful == numTasks) {
        healthTracker.foreach(_.updateExcludedForSuccessfulTaskSet(
          taskSet.stageId,
          taskSet.stageAttemptId,
          taskSetExcludelistHelperOpt.get.execToFailures))
      }
    }
  }

  /**
   * 根据当前等待时间，通过延迟调度获取我们可以启动任务的级别。
   * 是根据当前的资源和调度策略，确定任务调度时允许的本地性级别。
   * 它帮助 Spark 调度器在不同的本地性级别之间进行选择，以便在性能和资源利用之间取得平衡。
   */
  private def getAllowedLocalityLevel(curTime: Long): TaskLocality.TaskLocality = {
    // Remove the scheduled or finished tasks lazily
    def tasksNeedToBeScheduledFrom(pendingTaskIds: ArrayBuffer[Int]): Boolean = {
      var indexOffset = pendingTaskIds.size
      while (indexOffset > 0) {
        indexOffset -= 1
        val index = pendingTaskIds(indexOffset)
        if (copiesRunning(index) == 0 && !successful(index)) {
          return true
        } else {
          pendingTaskIds.remove(indexOffset)
        }
      }
      false
    }
    // Walk through the list of tasks that can be scheduled at each location and returns true
    // if there are any tasks that still need to be scheduled. Lazily cleans up tasks that have
    // already been scheduled.
    def moreTasksToRunIn(pendingTasks: HashMap[String, ArrayBuffer[Int]]): Boolean = {
      val emptyKeys = new ArrayBuffer[String]
      val hasTasks = pendingTasks.exists {
        case (id: String, tasks: ArrayBuffer[Int]) =>
          if (tasksNeedToBeScheduledFrom(tasks)) {
            true
          } else {
            emptyKeys += id
            false
          }
      }
      // The key could be executorId, host or rackId
      emptyKeys.foreach(id => pendingTasks.remove(id))
      hasTasks
    }

    while (currentLocalityIndex < myLocalityLevels.length - 1) {
      val moreTasks = myLocalityLevels(currentLocalityIndex) match {
        case TaskLocality.PROCESS_LOCAL => moreTasksToRunIn(pendingTasks.forExecutor)
        case TaskLocality.NODE_LOCAL => moreTasksToRunIn(pendingTasks.forHost)
        case TaskLocality.NO_PREF => pendingTasks.noPrefs.nonEmpty
        case TaskLocality.RACK_LOCAL => moreTasksToRunIn(pendingTasks.forRack)
      }
      if (!moreTasks) {
        // This is a performance optimization: if there are no more tasks that can
        // be scheduled at a particular locality level, there is no point in waiting
        // for the locality wait timeout (SPARK-4939).
        lastLocalityWaitResetTime = curTime
        logDebug(s"No tasks for locality level ${myLocalityLevels(currentLocalityIndex)}, " +
          s"so moving to locality level ${myLocalityLevels(currentLocalityIndex + 1)}")
        currentLocalityIndex += 1
      } else if (curTime - lastLocalityWaitResetTime >= localityWaits(currentLocalityIndex)) {
        // Jump to the next locality level, and reset lastLocalityWaitResetTime so that the next
        // locality wait timer doesn't immediately expire
        lastLocalityWaitResetTime += localityWaits(currentLocalityIndex)
        logDebug(s"Moving to ${myLocalityLevels(currentLocalityIndex + 1)} after waiting for " +
          s"${localityWaits(currentLocalityIndex)}ms")
        currentLocalityIndex += 1
      } else {
        return myLocalityLevels(currentLocalityIndex)
      }
    }
    myLocalityLevels(currentLocalityIndex)
  }

  /**
   * Find the index in myLocalityLevels for a given locality. This is also designed to work with
   * localities that are not in myLocalityLevels (in case we somehow get those) by returning the
   * next-biggest level we have. Uses the fact that the last value in myLocalityLevels is ANY.
   */
  def getLocalityIndex(locality: TaskLocality.TaskLocality): Int = {
    var index = 0
    while (locality > myLocalityLevels(index)) {
      index += 1
    }
    index
  }

  /**
   * Check whether the given task set has been excluded to the point that it can't run anywhere.
   *
   * It is possible that this taskset has become impossible to schedule *anywhere* due to the
   * failures that lead executors being excluded from the ones we can run on. The most common
   * scenario would be if there are fewer executors than spark.task.maxFailures.
   * We need to detect this so we can avoid the job from being hung. We try to acquire new
   * executor/s by killing an existing idle excluded executor.
   *
   * There's a tradeoff here: we could make sure all tasks in the task set are schedulable, but that
   * would add extra time to each iteration of the scheduling loop. Here, we take the approach of
   * making sure at least one of the unscheduled tasks is schedulable. This means we may not detect
   * the hang as quickly as we could have, but we'll always detect the hang eventually, and the
   * method is faster in the typical case. In the worst case, this method can take
   * O(maxTaskFailures + numTasks) time, but it will be faster when there haven't been any task
   * failures (this is because the method picks one unscheduled task, and then iterates through each
   * executor until it finds one that the task isn't excluded on).
   */
  private[scheduler] def getCompletelyExcludedTaskIfAny(
      hostToExecutors: HashMap[String, HashSet[String]]): Option[Int] = {
    taskSetExcludelistHelperOpt.flatMap { taskSetExcludelist =>
      val appHealthTracker = healthTracker.get
      // Only look for unschedulable tasks when at least one executor has registered. Otherwise,
      // task sets will be (unnecessarily) aborted in cases when no executors have registered yet.
      if (hostToExecutors.nonEmpty) {
        // find any task that needs to be scheduled
        val pendingTask: Option[Int] = {
          // usually this will just take the last pending task, but because of the lazy removal
          // from each list, we may need to go deeper in the list.  We poll from the end because
          // failed tasks are put back at the end of allPendingTasks, so we're more likely to find
          // an unschedulable task this way.
          val indexOffset = pendingTasks.all.lastIndexWhere { indexInTaskSet =>
            copiesRunning(indexInTaskSet) == 0 && !successful(indexInTaskSet)
          }
          if (indexOffset == -1) {
            None
          } else {
            Some(pendingTasks.all(indexOffset))
          }
        }

        pendingTask.find { indexInTaskSet =>
          // try to find some executor this task can run on.  Its possible that some *other*
          // task isn't schedulable anywhere, but we will discover that in some later call,
          // when that unschedulable task is the last task remaining.
          hostToExecutors.forall { case (host, execsOnHost) =>
            // Check if the task can run on the node
            val nodeExcluded =
              appHealthTracker.isNodeExcluded(host) ||
                taskSetExcludelist.isNodeExcludedForTaskSet(host) ||
                taskSetExcludelist.isNodeExcludedForTask(host, indexInTaskSet)
            if (nodeExcluded) {
              true
            } else {
              // Check if the task can run on any of the executors
              execsOnHost.forall { exec =>
                appHealthTracker.isExecutorExcluded(exec) ||
                  taskSetExcludelist.isExecutorExcludedForTaskSet(exec) ||
                  taskSetExcludelist.isExecutorExcludedForTask(exec, indexInTaskSet)
              }
            }
          }
        }
      } else {
        None
      }
    }
  }

  private[scheduler] def abortSinceCompletelyExcludedOnFailure(indexInTaskSet: Int): Unit = {
    taskSetExcludelistHelperOpt.foreach { taskSetExcludelist =>
      val partition = tasks(indexInTaskSet).partitionId
      abort(s"""
         |Aborting $taskSet because task $indexInTaskSet (partition $partition)
         |cannot run anywhere due to node and executor excludeOnFailure.
         |Most recent failure:
         |${taskSetExcludelist.getLatestFailureReason}
         |
         |ExcludeOnFailure behavior can be configured via spark.excludeOnFailure.*.
         |""".stripMargin)
    }
  }

  /**
   * Marks the task as getting result and notifies the DAG Scheduler
   */
  def handleTaskGettingResult(tid: Long): Unit = {
    val info = taskInfos(tid)
    info.markGettingResult(clock.getTimeMillis())
    sched.dagScheduler.taskGettingResult(info)
  }

  /**
   * Check whether has enough quota to fetch the result with `size` bytes.
   * This check does not apply to shuffle map tasks as they return map status and metrics updates,
   * which will be discarded by the driver after being processed.
   */
  def canFetchMoreResults(size: Long): Boolean = sched.synchronized {
    totalResultSize += size
    calculatedTasks += 1
    if (!isShuffleMapTasks && maxResultSize > 0 && totalResultSize > maxResultSize) {
      val msg = s"Total size of serialized results of ${calculatedTasks} tasks " +
        s"(${Utils.bytesToString(totalResultSize)}) is bigger than ${config.MAX_RESULT_SIZE.key} " +
        s"(${Utils.bytesToString(maxResultSize)})"
      logError(msg)
      abort(msg)
      false
    } else {
      true
    }
  }

  /**
   * Marks a task as successful and notifies the DAGScheduler that the task has ended.
   */
  def handleSuccessfulTask(tid: Long, result: DirectTaskResult[_]): Unit = {
    val info = taskInfos(tid)
    // SPARK-37300: when the task was already finished state, just ignore it,
    // so that there won't cause successful and tasksSuccessful wrong result.
    if(info.finished) {
      return
    }
    val index = info.index
    // Check if any other attempt succeeded before this and this attempt has not been handled
    if (successful(index) && killedByOtherAttempt.contains(tid)) {
      // Undo the effect on calculatedTasks and totalResultSize made earlier when
      // checking if can fetch more results
      calculatedTasks -= 1
      val resultSizeAcc = result.accumUpdates.find(a =>
        a.name == Some(InternalAccumulator.RESULT_SIZE))
      if (resultSizeAcc.isDefined) {
        totalResultSize -= resultSizeAcc.get.asInstanceOf[LongAccumulator].value
      }

      // Handle this task as a killed task
      handleFailedTask(tid, TaskState.KILLED,
        TaskKilled("Finish but did not commit due to another attempt succeeded"))
      return
    }

    info.markFinished(TaskState.FINISHED, clock.getTimeMillis())
    if (speculationEnabled) {
      successfulTaskDurations.insert(info.duration)
    }
    removeRunningTask(tid)

    // Kill any other attempts for the same task (since those are unnecessary now that one
    // attempt completed successfully).
    for (attemptInfo <- taskAttempts(index) if attemptInfo.running) {
      logInfo(s"Killing attempt ${attemptInfo.attemptNumber} for ${taskName(attemptInfo.taskId)}" +
        s" on ${attemptInfo.host} as the attempt ${info.attemptNumber} succeeded on ${info.host}")
      killedByOtherAttempt += attemptInfo.taskId
      sched.backend.killTask(
        attemptInfo.taskId,
        attemptInfo.executorId,
        interruptThread = true,
        reason = "another attempt succeeded")
    }
    if (!successful(index)) {
      tasksSuccessful += 1
      logInfo(s"Finished ${taskName(info.taskId)} in ${info.duration} ms " +
        s"on ${info.host} (executor ${info.executorId}) ($tasksSuccessful/$numTasks)")
      // Mark successful and stop if all the tasks have succeeded.
      successful(index) = true
      numFailures(index) = 0
      if (tasksSuccessful == numTasks) {
        isZombie = true
      }
    } else {
      logInfo(s"Ignoring task-finished event for ${taskName(info.taskId)} " +
        s"because it has already completed successfully")
    }
    // This method is called by "TaskSchedulerImpl.handleSuccessfulTask" which holds the
    // "TaskSchedulerImpl" lock until exiting. To avoid the SPARK-7655 issue, we should not
    // "deserialize" the value when holding a lock to avoid blocking other threads. So we call
    // "result.value()" in "TaskResultGetter.enqueueSuccessfulTask" before reaching here.
    // Note: "result.value()" only deserializes the value when it's called at the first time, so
    // here "result.value()" just returns the value and won't block other threads.
    sched.dagScheduler.taskEnded(tasks(index), Success, result.value(), result.accumUpdates,
      result.metricPeaks, info)
    maybeFinishTaskSet()
  }

  private[scheduler] def markPartitionCompleted(partitionId: Int): Unit = {
    partitionToIndex.get(partitionId).foreach { index =>
      if (!successful(index)) {
        tasksSuccessful += 1
        successful(index) = true
        numFailures(index) = 0
        if (tasksSuccessful == numTasks) {
          isZombie = true
        }
        maybeFinishTaskSet()
      }
    }
  }

  /**
   * Marks the task as failed, re-adds it to the list of pending tasks, and notifies the
   * DAG Scheduler.
   */
  def handleFailedTask(tid: Long, state: TaskState, reason: TaskFailedReason): Unit = {
    val info = taskInfos(tid)
    // SPARK-37300: when the task was already finished state, just ignore it,
    // so that there won't cause copiesRunning wrong result.
    if (info.finished) {
      return
    }
    removeRunningTask(tid)
    info.markFinished(state, clock.getTimeMillis())
    val index = info.index
    copiesRunning(index) -= 1
    var accumUpdates: Seq[AccumulatorV2[_, _]] = Seq.empty
    var metricPeaks: Array[Long] = Array.empty
    val failureReason = s"Lost ${taskName(tid)} (${info.host} " +
      s"executor ${info.executorId}): ${reason.toErrorString}"
    val failureException: Option[Throwable] = reason match {
      case fetchFailed: FetchFailed =>
        logWarning(failureReason)
        if (!successful(index)) {
          successful(index) = true
          tasksSuccessful += 1
        }
        isZombie = true

        if (fetchFailed.bmAddress != null) {
          healthTracker.foreach(_.updateExcludedForFetchFailure(
            fetchFailed.bmAddress.host, fetchFailed.bmAddress.executorId))
        }

        None

      case ef: ExceptionFailure =>
        // ExceptionFailure's might have accumulator updates
        accumUpdates = ef.accums
        metricPeaks = ef.metricPeaks.toArray
        val task = taskName(tid)
        if (ef.className == classOf[NotSerializableException].getName) {
          // If the task result wasn't serializable, there's no point in trying to re-execute it.
          logError(s"$task had a not serializable result: ${ef.description}; not retrying")
          abort(s"$task had a not serializable result: ${ef.description}")
          return
        }
        if (ef.className == classOf[TaskOutputFileAlreadyExistException].getName) {
          // If we can not write to output file in the task, there's no point in trying to
          // re-execute it.
          logError("Task %s in stage %s (TID %d) can not write to output file: %s; not retrying"
            .format(info.id, taskSet.id, tid, ef.description))
          abort("Task %s in stage %s (TID %d) can not write to output file: %s".format(
            info.id, taskSet.id, tid, ef.description))
          return
        }
        val key = ef.description
        val now = clock.getTimeMillis()
        val (printFull, dupCount) = {
          if (recentExceptions.contains(key)) {
            val (dupCount, printTime) = recentExceptions(key)
            if (now - printTime > EXCEPTION_PRINT_INTERVAL) {
              recentExceptions(key) = (0, now)
              (true, 0)
            } else {
              recentExceptions(key) = (dupCount + 1, printTime)
              (false, dupCount + 1)
            }
          } else {
            recentExceptions(key) = (0, now)
            (true, 0)
          }
        }
        if (printFull) {
          logWarning(failureReason)
        } else {
          logInfo(
            s"Lost $task on ${info.host}, executor ${info.executorId}: " +
              s"${ef.className} (${ef.description}) [duplicate $dupCount]")
        }
        ef.exception

      case tk: TaskKilled =>
        // TaskKilled might have accumulator updates
        accumUpdates = tk.accums
        metricPeaks = tk.metricPeaks.toArray
        logWarning(failureReason)
        None

      case e: ExecutorLostFailure if !e.exitCausedByApp =>
        logInfo(s"${taskName(tid)} failed because while it was being computed, its executor " +
          "exited for a reason unrelated to the task. Not counting this failure towards the " +
          "maximum number of failures for the task.")
        None

      case e: TaskFailedReason =>  // TaskResultLost and others
        logWarning(failureReason)
        None
    }

    if (tasks(index).isBarrier) {
      isZombie = true
    }

    sched.dagScheduler.taskEnded(tasks(index), reason, null, accumUpdates, metricPeaks, info)

    if (!isZombie && reason.countTowardsTaskFailures) {
      assert (null != failureReason)
      taskSetExcludelistHelperOpt.foreach(_.updateExcludedForFailedTask(
        info.host, info.executorId, index, failureReason))
      numFailures(index) += 1
      if (numFailures(index) >= maxTaskFailures) {
        logError("Task %d in stage %s failed %d times; aborting job".format(
          index, taskSet.id, maxTaskFailures))
        abort("Task %d in stage %s failed %d times, most recent failure: %s\nDriver stacktrace:"
          .format(index, taskSet.id, maxTaskFailures, failureReason), failureException)
        return
      }
    }

    if (successful(index)) {
      logInfo(s"${taskName(info.taskId)} failed, but the task will not" +
        " be re-executed (either because the task failed with a shuffle data fetch failure," +
        " so the previous stage needs to be re-run, or because a different copy of the task" +
        " has already succeeded).")
    } else {
      addPendingTask(index)
    }

    maybeFinishTaskSet()
  }

  def abort(message: String, exception: Option[Throwable] = None): Unit = sched.synchronized {
    // TODO: Kill running tasks if we were not terminated due to a Mesos error
    sched.dagScheduler.taskSetFailed(taskSet, message, exception)
    isZombie = true
    maybeFinishTaskSet()
  }

  /** If the given task ID is not in the set of running tasks, adds it.
   *
   * Used to keep track of the number of running tasks, for enforcing scheduling policies.
   */
  def addRunningTask(tid: Long): Unit = {
    if (runningTasksSet.add(tid) && parent != null) {
      parent.increaseRunningTasks(1)
    }
  }

  /** If the given task ID is in the set of running tasks, removes it. */
  def removeRunningTask(tid: Long): Unit = {
    if (runningTasksSet.remove(tid) && parent != null) {
      parent.decreaseRunningTasks(1)
    }
  }

  override def getSchedulableByName(name: String): Schedulable = {
    null
  }

  override def isSchedulable: Boolean = !isZombie &&
    (pendingTasks.all.nonEmpty || pendingSpeculatableTasks.all.nonEmpty)

  override def addSchedulable(schedulable: Schedulable): Unit = {}

  override def removeSchedulable(schedulable: Schedulable): Unit = {}

  override def getSortedTaskSetQueue(): ArrayBuffer[TaskSetManager] = {
    val sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]()
    sortedTaskSetQueue += this
    sortedTaskSetQueue
  }

  /** Called by TaskScheduler when an executor is lost so we can re-enqueue our tasks */
  override def executorLost(execId: String, host: String, reason: ExecutorLossReason): Unit = {
    // Re-enqueue any tasks that ran on the failed executor if this is a shuffle map stage,
    // and we are not using an external shuffle server which could serve the shuffle outputs.
    // The reason is the next stage wouldn't be able to fetch the data from this dead executor
    // so we would need to rerun these tasks on other executors.
    if (isShuffleMapTasks && !env.blockManager.externalShuffleServiceEnabled && !isZombie) {
      for ((tid, info) <- taskInfos if info.executorId == execId) {
        val index = info.index
        // We may have a running task whose partition has been marked as successful,
        // this partition has another task completed in another stage attempt.
        // We treat it as a running task and will call handleFailedTask later.
        if (successful(index) && !info.running && !killedByOtherAttempt.contains(tid)) {
          successful(index) = false
          copiesRunning(index) -= 1
          tasksSuccessful -= 1
          addPendingTask(index)
          // Tell the DAGScheduler that this task was resubmitted so that it doesn't think our
          // stage finishes when a total of tasks.size tasks finish.
          sched.dagScheduler.taskEnded(
            tasks(index), Resubmitted, null, Seq.empty, Array.empty, info)
        }
      }
    }
    for ((tid, info) <- taskInfos if info.running && info.executorId == execId) {
      val exitCausedByApp: Boolean = reason match {
        case exited: ExecutorExited => exited.exitCausedByApp
        case ExecutorKilled | ExecutorDecommission(_) => false
        case ExecutorProcessLost(_, _, false) => false
        case _ => true
      }
      handleFailedTask(tid, TaskState.FAILED, ExecutorLostFailure(info.executorId, exitCausedByApp, Some(reason.toString)))
    }
    // recalculate valid locality levels and waits when executor is lost
    recomputeLocality()
  }

  /**
   * 检查与给定 tid 关联的任务是否已超过时间阈值，并且是否应该进行推测性运行。
   */
  private def checkAndSubmitSpeculatableTask(
      tid: Long,
      currentTimeMillis: Long,
      threshold: Double): Boolean = {
    val info = taskInfos(tid)
    val index = info.index

    // 当前任务正在运行
    // 当前任务的运行时间超过了已完成任务平均时间的 threshold 倍
    if (!successful(index) && copiesRunning(index) == 1 &&
        info.timeRunning(currentTimeMillis) > threshold && !speculatableTasks.contains(index)) {
      addPendingTask(index, speculatable = true)
      logInfo(
        ("Marking task %d in stage %s (on %s) as speculatable because it ran more" +
          " than %.0f ms(%d speculatable tasks in this taskset now)")
          .format(index, taskSet.id, info.host, threshold, speculatableTasks.size + 1))
      speculatableTasks += index
      sched.dagScheduler.speculativeTaskSubmitted(tasks(index))
      true
    } else {
      false
    }
  }

  /**
   * Check for tasks to be speculated and return true if there are any. This is called periodically
   * by the TaskScheduler.
   *
   */
  override def checkSpeculatableTasks(minTimeToSpeculation: Long): Boolean = {
    // No need to speculate if the task set is zombie or is from a barrier stage. If there is only
    // one task we don't speculate since we don't have metrics to decide whether it's taking too
    // long or not, unless a task duration threshold is explicitly provided.
    if (isZombie || isBarrier || (numTasks == 1 && !speculationTaskDurationThresOpt.isDefined)) {
      return false
    }
    var foundTasks = false
    logDebug("Checking for speculative tasks: minFinished = " + minFinishedForSpeculation)

    // It's possible that a task is marked as completed by the scheduler, then the size of
    // `successfulTaskDurations` may not equal to `tasksSuccessful`. Here we should only count the
    // tasks that are submitted by this `TaskSetManager` and are completed successfully.
    val numSuccessfulTasks = successfulTaskDurations.size()
    if (numSuccessfulTasks >= minFinishedForSpeculation) {
      val time = clock.getTimeMillis()
      val medianDuration = successfulTaskDurations.median
      val threshold = max(speculationMultiplier * medianDuration, minTimeToSpeculation)
      // TODO: Threshold should also look at standard deviation of task durations and have a lower
      // bound based on that.
      logDebug("Task length threshold for speculation: " + threshold)
      for (tid <- runningTasksSet) {
        var speculated = checkAndSubmitSpeculatableTask(tid, time, threshold)
        if (!speculated && executorDecommissionKillInterval.isDefined) {
          val taskInfo = taskInfos(tid)
          val decomState = sched.getExecutorDecommissionState(taskInfo.executorId)
          if (decomState.isDefined) {
            // Check if this task might finish after this executor is decommissioned.
            // We estimate the task's finish time by using the median task duration.
            // Whereas the time when the executor might be decommissioned is estimated using the
            // config executorDecommissionKillInterval. If the task is going to finish after
            // decommissioning, then we will eagerly speculate the task.
            val taskEndTimeBasedOnMedianDuration = taskInfos(tid).launchTime + medianDuration
            val executorDecomTime = decomState.get.startTime + executorDecommissionKillInterval.get
            val canExceedDeadline = executorDecomTime < taskEndTimeBasedOnMedianDuration
            if (canExceedDeadline) {
              speculated = checkAndSubmitSpeculatableTask(tid, time, 0)
            }
          }
        }
        foundTasks |= speculated
      }
    } else if (speculationTaskDurationThresOpt.isDefined && speculationTasksLessEqToSlots) {
      val time = clock.getTimeMillis()
      val threshold = speculationTaskDurationThresOpt.get
      logDebug(s"Tasks taking longer time than provided speculation threshold: $threshold")
      for (tid <- runningTasksSet) {
        foundTasks |= checkAndSubmitSpeculatableTask(tid, time, threshold)
      }
    }
    foundTasks
  }

  private def getLocalityWait(level: TaskLocality.TaskLocality): Long = {
    if (legacyLocalityWaitReset && isBarrier) return 0

    val localityWait = level match {
      case TaskLocality.PROCESS_LOCAL => config.LOCALITY_WAIT_PROCESS
      case TaskLocality.NODE_LOCAL => config.LOCALITY_WAIT_NODE
      case TaskLocality.RACK_LOCAL => config.LOCALITY_WAIT_RACK
      case _ => null
    }

    if (localityWait != null) {
      conf.get(localityWait)
    } else {
      0L
    }
  }

  /**
   * 计算在此 TaskSet 中使用的本地性级别。假设所有任务已经使用 addPendingTask 添加到队列中。
   */
  private def computeValidLocalityLevels(): Array[TaskLocality.TaskLocality] = {
    import TaskLocality.{PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY}
    val levels = new ArrayBuffer[TaskLocality.TaskLocality]

    // 如果当前任务的数据倾向container正好有可用资源
    if (!pendingTasks.forExecutor.isEmpty &&
        pendingTasks.forExecutor.keySet.exists(sched.isExecutorAlive(_))) {
      levels += PROCESS_LOCAL
    }

    // 如果当前任务的数据倾向节点正好有可用资源
    if (!pendingTasks.forHost.isEmpty &&
        pendingTasks.forHost.keySet.exists(sched.hasExecutorsAliveOnHost(_))) {
      levels += NODE_LOCAL
    }
    if (!pendingTasks.noPrefs.isEmpty) {
      levels += NO_PREF
    }
    if (!pendingTasks.forRack.isEmpty &&
        pendingTasks.forRack.keySet.exists(sched.hasHostAliveOnRack(_))) {
      levels += RACK_LOCAL
    }
    levels += ANY
    logDebug("Valid locality levels for " + taskSet + ": " + levels.mkString(", "))
    levels.toArray
  }

  def executorDecommission(execId: String): Unit = {
    recomputeLocality()
  }

  // 重新计算数据本地性
  def recomputeLocality(): Unit = {
    // 当 executor 丢失时，一个僵尸 TaskSetManager 可能会到达这里。
    if (isZombie) return

    val previousLocalityIndex = currentLocalityIndex
    val previousLocalityLevel = myLocalityLevels(currentLocalityIndex)
    val previousMyLocalityLevels = myLocalityLevels
    myLocalityLevels = computeValidLocalityLevels()
    localityWaits = myLocalityLevels.map(getLocalityWait)
    currentLocalityIndex = getLocalityIndex(previousLocalityLevel)

    if (currentLocalityIndex > previousLocalityIndex) {
      // SPARK-31837: If the new level is more local, shift to the new most local locality
      // level in terms of better data locality. For example, say the previous locality
      // levels are [PROCESS, NODE, ANY] and current level is ANY. After recompute, the
      // locality levels are [PROCESS, NODE, RACK, ANY]. Then, we'll shift to RACK level.
      currentLocalityIndex = getLocalityIndex(myLocalityLevels.diff(previousMyLocalityLevels).head)
    }
  }

  def executorAdded(): Unit = {
    recomputeLocality()
  }
}

private[spark] object TaskSetManager {
  // The user will be warned if any stages contain a task that has a serialized size greater than
  // this.
  val TASK_SIZE_TO_WARN_KIB = 1000

  // 1 minute
  val BARRIER_LOGGING_INTERVAL = 60000
}

/**
 * 不同本地性级别（executor、host、rack、noPrefs 和 all）的待处理任务集合。
 * 这些集合实际上被当作栈来处理，新任务被添加到 ArrayBuffer 的末尾，并从末尾移除。
 * 这样可以更快地检测到反复失败的任务，因为每当任务失败时，它会被放回栈顶。
 * 这些集合可能包含重复项，原因有两个：
 * (1) 任务只被懒惰地移除；当一个任务被启动时，它仍然保留在除启动任务的那个队列之外的所有待处理列表中。
 * (2) 由于失败，任务可能多次被重新添加到这些列表中。
 * 在 dequeueTaskFromList 中处理重复项，该方法确保在启动任务之前任务尚未开始运行。
 */
private[scheduler] class PendingTasksByLocality {

  // 每个executor的待处理任务集合。
  val forExecutor = new HashMap[String, ArrayBuffer[Int]]
  // 每个host的待处理任务集合。类似于 pendingTasksForExecutor，但在主机级别。
  val forHost = new HashMap[String, ArrayBuffer[Int]]
  // 包含无本地性偏好待处理任务的集合。
  val noPrefs = new ArrayBuffer[Int]
  // 每个机架的待处理任务集合 —— 类似于上面的集合。
  val forRack = new HashMap[String, ArrayBuffer[Int]]
  // 包含所有待处理任务的集合（也用作栈，如上所述）。
  val all = new ArrayBuffer[Int]
}

private[scheduler] case class BarrierPendingLaunchTask(
    execId: String,
    host: String,
    index: Int,
    taskLocality: TaskLocality.TaskLocality,
    assignedResources: Map[String, ResourceInformation]) {
  // Stored the corresponding index of the WorkerOffer which is responsible to launch the task.
  // Used to revert the assigned resources (e.g., cores, custome resources) when the barrier
  // task set doesn't launch successfully in a single resourceOffers round.
  var assignedOfferIndex: Int = _
  var assignedCores: Int = 0
}
