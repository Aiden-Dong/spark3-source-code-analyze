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
import java.util.{Timer, TimerTask}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Buffer, HashMap, HashSet}
import scala.util.Random

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config._
import org.apache.spark.resource.{ResourceInformation, ResourceProfile}
import org.apache.spark.rpc.RpcEndpoint
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler.TaskLocality.TaskLocality
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{AccumulatorV2, Clock, SystemClock, ThreadUtils, Utils}

/**
 * 通过 SchedulerBackend 为多种类型的集群调度任务。
 * 它还可以通过使用 `LocalSchedulerBackend` 并将 isLocal 设置为 true 来与本地设置一起工作。
 * 它处理常见逻辑，如确定作业之间的调度顺序、唤醒以启动推测任务等。
 *
 * 客户端应首先调用 initialize() 和 start()，然后通过 submitTasks 方法提交任务集。
 *
 * 线程处理：[[SchedulerBackend]] 和提交任务的客户端可以从多个线程调用此类，
 * 因此它需要在公共 API 方法中加锁以维护其状态。
 * 此外，一些 [[SchedulerBackend]] 在发送事件到此类时会对自身进行同步，然后获取我们的锁，
 * 因此我们需要确保在持有自身锁时不尝试锁定后端。
 * 此类由许多线程调用，特别是：
 *   * DAGScheduler 事件循环
 *   * RPCHandler 线程，响应来自执行器的状态更新
 *   * CoarseGrainedSchedulerBackend 的所有报价的周期性恢复，以适应延迟调度
 *   * task-result-getter 线程
 *
 * 警告：Spark RPC 框架中的任何非致命异常都可能被吞掉。
 * 因此，在类似 resourceOffers、statusUpdate 之类的方法中抛出异常不会使应用程序失败，但可能导致未定义的行为。
 * 相反，我们应该使用类似 TaskSetManager.abort() 的方法来终止一个阶段，然后使应用程序失败 (SPARK-31485)。
 *
 * 延迟调度：
 * 延迟调度是一种优化，它牺牲了作业公平性以换取数据本地性，从而提高集群和工作负载的吞吐量。
 * “延迟”的一个有用定义是自 TaskSet 使用其公平份额的资源以来经过的时间。
 * 由于在没有完整模拟的情况下计算此延迟是不切实际的，因此使用的启发式方法是
 * 自 TaskSetManager 上次启动任务以来经过的时间，并且自上次提供其“公平份额”以来没有因延迟调度而拒绝任何资源。
 * 当 [[resourceOffers]] 的参数 “isAllFreeResources” 设置为 true 时，这是一个“公平份额”提议。
 * “延迟调度拒绝”是指尽管有待处理任务但资源未被利用（在 [[TaskSetManager]] 内实现）。
 * 传统的启发式方法只测量自 [[TaskSetManager]] 上次启动任务以来的时间，
 * 可以通过将 spark.locality.wait.legacyResetOnTaskLaunch 设置为 true 来重新启用。
 */
private[spark] class TaskSchedulerImpl(
    val sc: SparkContext,
    val maxTaskFailures: Int,
    isLocal: Boolean = false,
    clock: Clock = new SystemClock)
  extends TaskScheduler with Logging {

  import TaskSchedulerImpl._

  def this(sc: SparkContext) = {
    this(sc, sc.conf.get(config.TASK_MAX_FAILURES))
  }

  // 延迟初始化 healthTrackerOpt 以避免获得空的 ExecutorAllocationClient，
  // 因为 ExecutorAllocationClient 在 TaskSchedulerImpl 之后创建。
  private[scheduler] lazy val healthTrackerOpt = maybeCreateHealthTracker(sc)

  val conf = sc.conf

  // 检查推测任务的频率。 (spark.speculation.interval)
  val SPECULATION_INTERVAL_MS = conf.get(SPECULATION_INTERVAL)

  // 只有在原始任务副本运行时间至少达到此时间后，才会启动任务的重复副本。
  // 这是为了避免启动非常短的任务的推测副本所带来的开销。 (spark.speculation.minTaskRuntime)
  val MIN_TIME_TO_SPECULATION = conf.get(SPECULATION_MIN_THRESHOLD)

  // 推测执行调度线程
  private val speculationScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("task-scheduler-speculation")

  // 超过此阈值时，我们会警告用户初始 TaskSet 可能会被饿死。
  val STARVATION_TIMEOUT_MS = conf.getTimeAsMs("spark.starvation.timeout", "15s")

  // 每个任务请求的 CPU 数量。 (spark.task.cpus)
  val CPUS_PER_TASK = conf.get(config.CPUS_PER_TASK)

  // TaskSetManagers 不是线程安全的，
  // 因此对它的任何访问都应在此类上同步。由 `this` 保护。
  // (stageId -> (attempId -> task_set_manager))
  private val taskSetsByStageIdAndAttempt = new HashMap[Int, HashMap[Int, TaskSetManager]]

  // keyed by taskset
  // 如果 taskset 自上次重置计时器以来没有因本地性而拒绝任何资源，则值为 true
  private val noRejectsSinceLastReset = new mutable.HashMap[TaskSet, Boolean]()
  private val legacyLocalityWaitReset = conf.get(LEGACY_LOCALITY_WAIT_RESET)

  // taskId -> tasksetmanager
  private[scheduler] val taskIdToTaskSetManager = new ConcurrentHashMap[Long, TaskSetManager]
  // Protected by `this`
  val taskIdToExecutorId = new HashMap[Long, String]

  @volatile private var hasReceivedTask = false
  @volatile private var hasLaunchedTask = false
  private val starvationTimer = new Timer("task-starvation-timer", true)

  // Incrementing task IDs
  val nextTaskId = new AtomicLong(0)

  // 每个 executor 上运行的 task id 集合
  private val executorIdToRunningTaskIds = new HashMap[String, HashSet[Long]]

  // 当我们第一次收到executor的 decommission 通知时，我们将它们添加到这里。
  // 即使在要求它们停用decommission后，executor 仍然可以继续运行，但最终它们会退出。
  val executorsPendingDecommission = new HashMap[String, ExecutorDecommissionState]

  def runningTasksByExecutors: Map[String, Int] = synchronized {
    executorIdToRunningTaskIds.toMap.mapValues(_.size).toMap
  }

  // host -> executorIds 集合， 这用于计算 hostsAlive，
  // 进而用于决定何时可以在给定主机上实现数据本地性。
  protected val hostToExecutors = new HashMap[String, HashSet[String]]

  // rack -> hosts 集合
  protected val hostsByRack = new HashMap[String, HashSet[String]]

  protected val executorIdToHost = new HashMap[String, String]

  private val abortTimer = new Timer("task-abort-timer", true)
  // Exposed for testing
  val unschedulableTaskSetToExpiryTime = new HashMap[TaskSetManager, Long]

  // Listener object to pass upcalls into
  var dagScheduler: DAGScheduler = null

  // 调度后端
  var backend: SchedulerBackend = null

  val mapOutputTracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]

  private var schedulableBuilder: SchedulableBuilder = null

  // 调度模式 : FIFO, FAIR
  private val schedulingModeConf = conf.get(SCHEDULER_MODE)
  val schedulingMode: SchedulingMode =
    try {
      SchedulingMode.withName(schedulingModeConf)
    } catch {
      case e: java.util.NoSuchElementException =>
        throw SparkCoreErrors.unrecognizedSchedulerModePropertyError(SCHEDULER_MODE_PROPERTY,
          schedulingModeConf)
    }

  // 调度资源池
  val rootPool: Pool = new Pool("", schedulingMode, 0, 0)

  // 获取远程执行结果.
  private[spark] var taskResultGetter = new TaskResultGetter(sc.env, this)

  // spark.barrier.sync.timeout
  private lazy val barrierSyncTimeout = conf.get(config.BARRIER_SYNC_TIMEOUT)

  private[scheduler] var barrierCoordinator: RpcEndpoint = null

  protected val defaultRackValue: Option[String] = None

  private def maybeInitBarrierCoordinator(): Unit = {
    if (barrierCoordinator == null) {
      barrierCoordinator = new BarrierCoordinator(barrierSyncTimeout, sc.listenerBus,
        sc.env.rpcEnv)
      sc.env.rpcEnv.setupEndpoint("barrierSync", barrierCoordinator)
      logInfo("Registered BarrierCoordinator endpoint")
    }
  }

  override def setDAGScheduler(dagScheduler: DAGScheduler): Unit = {
    this.dagScheduler = dagScheduler
  }

  // 初始化调度后端跟作业调度策略
  def initialize(backend: SchedulerBackend): Unit = {
    this.backend = backend
    schedulableBuilder = {
      schedulingMode match {
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          new FairSchedulableBuilder(rootPool, sc)
        case _ =>
          throw new IllegalArgumentException(s"Unsupported $SCHEDULER_MODE_PROPERTY: " +
          s"$schedulingMode")
      }
    }
    schedulableBuilder.buildPools()
  }

  def newTaskId(): Long = nextTaskId.getAndIncrement()

  override def start(): Unit = {
    backend.start()  // 开启调度资源你后端

    if (!isLocal && conf.get(SPECULATION_ENABLED)) {
      logInfo("Starting speculative execution thread")
      speculationScheduler.scheduleWithFixedDelay(
        () => Utils.tryOrStopSparkContext(sc) { checkSpeculatableTasks() },
        SPECULATION_INTERVAL_MS, SPECULATION_INTERVAL_MS, TimeUnit.MILLISECONDS)
    }
  }

  override def postStartHook(): Unit = {
    waitBackendReady()
  }

  override def submitTasks(taskSet: TaskSet): Unit = {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks "
      + "resource profile " + taskSet.resourceProfileId)
    this.synchronized {
      // 对每一个 taskset 创建一个 TaskSetManager
      val manager = createTaskSetManager(taskSet, maxTaskFailures)
      val stage = taskSet.stageId

      val stageTaskSets = taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])

      // 将此 stage 的所有现有 TaskSetManagers 标记为僵尸，因为我们正在添加一个新的。
      // 这是处理一个边缘情况所必需的。假设一个 stage 有 10 个分区，并且有 2 个 TaskSetManagers：
      //   - TSM1（僵尸）和 TSM2（活跃）。
      //   - TSM1 正在运行分区 10 的task并完成。
      //   - TSM2 完成了分区 1-9 的任务，并认为自己仍然活跃，因为分区 10 尚未完成。
      // 然而，DAGScheduler 收到了所有 10 个分区的任务完成事件，并认为stage已经完成。
      // 如果这是一个 shuffle stage，并且由于某种原因缺少 map 输出，DAGScheduler 将重新提交它并为其创建一个 TSM3。
      // 由于一个 stage 不能有多个活跃的 TaskSetManager，我们必须将 TSM2 标记为僵尸（它实际上是）。
      stageTaskSets.foreach { case (_, ts) =>
        ts.isZombie = true   // 僵尸标识
      }

      stageTaskSets(taskSet.stageAttemptId) = manager
      // 将 tasksetManager 加入调度构建器
      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

      if (!isLocal && !hasReceivedTask) {
        starvationTimer.scheduleAtFixedRate(new TimerTask() {
          override def run(): Unit = {
            if (!hasLaunchedTask) {
              logWarning("Initial job has not accepted any resources; " +
                "check your cluster UI to ensure that workers are registered " +
                "and have sufficient resources")
            } else {
              this.cancel()
            }
          }
        }, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS)
      }
      hasReceivedTask = true
    }
    backend.reviveOffers()
  }

  // 标记为 private[scheduler]，以便在必要时允许测试更换不同的任务集管理器。
  private[scheduler] def createTaskSetManager(
      taskSet: TaskSet,
      maxTaskFailures: Int): TaskSetManager = {
    new TaskSetManager(this, taskSet, maxTaskFailures, healthTrackerOpt, clock)
  }

  // 取消任务执行
  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = synchronized {
    logInfo("Cancelling stage " + stageId)
    // Kill all running tasks for the stage.
    killAllTaskAttempts(stageId, interruptThread, reason = "Stage cancelled")
    // Cancel all attempts for the stage.
    taskSetsByStageIdAndAttempt.get(stageId).foreach { attempts =>
      attempts.foreach { case (_, tsm) =>
        tsm.abort("Stage %s cancelled".format(stageId))
        logInfo("Stage %d was cancelled".format(stageId))
      }
    }
  }

  override def killTaskAttempt(
      taskId: Long,
      interruptThread: Boolean,
      reason: String): Boolean = synchronized {
    logInfo(s"Killing task $taskId: $reason")
    val execId = taskIdToExecutorId.get(taskId)
    if (execId.isDefined) {
      backend.killTask(taskId, execId.get, interruptThread, reason)
      true
    } else {
      logWarning(s"Could not kill task $taskId because no task with that ID was found.")
      false
    }
  }

  // 杀死当前任务的运行
  override def killAllTaskAttempts(
      stageId: Int,
      interruptThread: Boolean,
      reason: String): Unit = synchronized {
    logInfo(s"Killing all running tasks in stage $stageId: $reason")

    taskSetsByStageIdAndAttempt.get(stageId).foreach { attempts =>
      attempts.foreach { case (_, tsm) =>
        // 这里有两种可能的情况：
        // 1. 任务集管理器已创建并且一些任务已被调度。 在这种情况下，向执行器发送终止信号以终止任务。
        // 2. 任务集管理器已创建但尚未调度任何任务。在这种情况下，继续执行。
        tsm.runningTasksSet.foreach { tid =>
          taskIdToExecutorId.get(tid).foreach { execId =>
            backend.killTask(tid, execId, interruptThread, reason)
          }
        }
      }
    }
  }

  override def notifyPartitionCompletion(stageId: Int, partitionId: Int): Unit = {
    taskResultGetter.enqueuePartitionCompletionNotification(stageId, partitionId)
  }

  /**
   * 调用此方法以指示与给定 TaskSetManager 关联的所有task attempt（包括推测任务）均已完成，
   * 因此应清理与 TaskSetManager 相关的状态。
   */
  def taskSetFinished(manager: TaskSetManager): Unit = synchronized {
    taskSetsByStageIdAndAttempt.get(manager.taskSet.stageId).foreach { taskSetsForStage =>
      taskSetsForStage -= manager.taskSet.stageAttemptId
      if (taskSetsForStage.isEmpty) {
        taskSetsByStageIdAndAttempt -= manager.taskSet.stageId
      }
    }
    noRejectsSinceLastReset -= manager.taskSet
    manager.parent.removeSchedulable(manager)
    logInfo(s"Removed TaskSet ${manager.taskSet.id}, whose tasks have all completed, from pool" +
      s" ${manager.parent.name}")
  }

  /**
   * 在给定的最大允许 [[TaskLocality]] 上向单个 [[TaskSetManager]] 提供资源。
   *
   * @param taskSet 要向其提供资源的任务集管理器
   * @param maxLocality 调度时允许的最大本地性
   * @param shuffledOffers 用于调度的混排资源提供，随着任务调度，下面字段跟踪剩余资源
   * @param availableCpus 每个提供的剩余 CPU 数量， 索引 'i' 处的值对应于 shuffledOffers[i]
   * @param availableResources 每个提供的剩余资源，  索引 'i' 处的值对应于 shuffledOffers[i]
   * @param tasks 每个提供调度的任务，索引 'i' 处的值对应于 shuffledOffers[i]
   * @return tuple (没有延迟调度拒绝?, 启动任务的最小本地性的选项)
   */
  private def resourceOfferSingleTaskSet(
      taskSet: TaskSetManager,
      maxLocality: TaskLocality,
      shuffledOffers: Seq[WorkerOffer],
      availableCpus: Array[Int],
      availableResources: Array[Map[String, Buffer[String]]],
      tasks: IndexedSeq[ArrayBuffer[TaskDescription]])
    : (Boolean, Option[TaskLocality]) = {

    var noDelayScheduleRejects = true
    var minLaunchedLocality: Option[TaskLocality] = None

    // 遍历每个主机节点
    for (i <- 0 until shuffledOffers.size) {

      val execId = shuffledOffers(i).executorId  // 资源单元
      val host = shuffledOffers(i).host

      val taskSetRpID = taskSet.taskSet.resourceProfileId

      // 目前将资源配置文件 ID 作为硬性要求 —— 即仅将任务集放在资源配置文件完全匹配的执行器上。
      if (taskSetRpID == shuffledOffers(i).resourceProfileId) {

        // STEP-1 : 校验改节点是否满足改 taskset 上的task 资源需求
        // 主要分为 单一task上的 cpu资源需求，其他资源需求
        val taskResAssignmentsOpt = resourcesMeetTaskRequirements(taskSet, availableCpus(i), availableResources(i))


        taskResAssignmentsOpt.foreach { taskResAssignments =>

          try {
            val prof = sc.resourceProfileManager.resourceProfileFromId(taskSetRpID)
            val taskCpus = ResourceProfile.getTaskCpusOrDefaultForProfile(prof, conf)

            // STEP-2 : 如果该资源满足以后，基于本地性，挑选一个任务在该节点上运行
            val (taskDescOption, didReject, index) = taskSet.resourceOffer(execId, host, maxLocality, taskCpus, taskResAssignments)

            noDelayScheduleRejects &= !didReject

            for (task <- taskDescOption) {
              val (locality, resources) = if (task != null) {
                // 表示已经成功的在改资源节点上调起一个资源
                tasks(i) += task

                addRunningTask(task.taskId, execId, taskSet)
                (taskSet.taskInfos(task.taskId).taskLocality, task.resources)
              } else {
                assert(taskSet.isBarrier, "TaskDescription can only be null for barrier task")
                val barrierTask = taskSet.barrierPendingLaunchTasks(index)
                barrierTask.assignedOfferIndex = i
                barrierTask.assignedCores = taskCpus
                (barrierTask.taskLocality, barrierTask.assignedResources)
              }

              minLaunchedLocality = minTaskLocality(minLaunchedLocality, Some(locality))
              availableCpus(i) -= taskCpus     // 占有cpu资源
              assert(availableCpus(i) >= 0)
              // 移除资源
              resources.foreach { case (rName, rInfo) =>
                // 从 availableResources 地址中移除前 n 个元素，这些被移除的地址与我们在 taskResourceAssignments 中分配的地址是相同的，因为它是同步的。
                // 我们不移除确切分配的地址，因为当前的方法可以在相同的时间复杂度下产生相同的结果。
                availableResources(i)(rName).remove(0, rInfo.addresses.size)
              }
            }
          } catch {
            case e: TaskNotSerializableException =>
              logError(s"Resource offer failed, task set ${taskSet.name} was not serializable")
              // Do not offer resources for this task, but don't throw an error to allow other
              // task sets to be submitted.
              return (noDelayScheduleRejects, minLaunchedLocality)
          }
        }
      }
    }
    (noDelayScheduleRejects, minLaunchedLocality)
  }

  /**
   * 将正在运行的任务添加到 TaskScheduler 相关结构中
   */
  private def addRunningTask(tid: Long, execId: String, taskSet: TaskSetManager): Unit = {
    taskIdToTaskSetManager.put(tid, taskSet)
    taskIdToExecutorId(tid) = execId
    executorIdToRunningTaskIds(execId).add(tid)
  }

  /**
   * 检查 WorkerOffer 的资源是否足以运行至少一个任务。
   * 如果资源不满足任务要求，则返回 None，否则返回分配给下一个任务的任务资源分配。
   * 请注意，如果不使用自定义资源，则分配可能为空。
   */
  private def resourcesMeetTaskRequirements(
      taskSet: TaskSetManager,
      availCpus: Int,
      availWorkerResources: Map[String, Buffer[String]]
    ): Option[Map[String, ResourceInformation]] = {

    // 每个taskset 挂一个 resourceProfileId -- 绑定的stageId, 默认0
    // 获取当前 stage 的资源需求
    val rpId = taskSet.taskSet.resourceProfileId
    val taskSetProf = sc.resourceProfileManager.resourceProfileFromId(rpId)

    // 每个task的cpu需求数
    val taskCpus = ResourceProfile.getTaskCpusOrDefaultForProfile(taskSetProf, conf)

    // cpu 不满足，则标识没有合适的计算资源
    if (availCpus < taskCpus) return None

    // 仅查看 CPU 以外的资源。
    val tsResources = ResourceProfile.getCustomTaskResources(taskSetProf)

    // 如果没有合适的资源项，则返回空
    if (tsResources.isEmpty) return Some(Map.empty)

    val localTaskReqAssign = HashMap[String, ResourceInformation]()

    // 我们在这里检查所有资源，以确保它们匹配，并获取下一个任务的资源分配。
    for ((rName, taskReqs) <- tsResources) {
      val taskAmount = taskSetProf.getSchedulerTaskResourceAmount(rName)  // 获取当前task 的资源需求
      availWorkerResources.get(rName) match {                             // 可用资源大小
        case Some(workerRes) =>
          if (workerRes.size >= taskAmount) {    // 判断当前资源是否
            localTaskReqAssign.put(rName, new ResourceInformation(rName, workerRes.take(taskAmount).toArray))
          } else {
            return None
          }
        case None => return None
      }
    }

    Some(localTaskReqAssign.toMap)
  }

  private def minTaskLocality(
      l1: Option[TaskLocality],
      l2: Option[TaskLocality]) : Option[TaskLocality] = {
    if (l1.isEmpty) {
      l2
    } else if (l2.isEmpty) {
      l1
    } else if (l1.get < l2.get) {
      l1
    } else {
      l2
    }
  }

  /**
   * 由 ClusterManager 调用，以在 Worker 节点上提供资源。我们通过按照优先级顺序向我们的活动taskset 请求任务来响应。
   * 我们以轮询的方式填充每个 node，以便任务在集群中均衡分配。
   */
  def resourceOffers(
      offers: IndexedSeq[WorkerOffer],
      isAllFreeResources: Boolean = true): Seq[Seq[TaskDescription]] = synchronized {

    // 将每个工作节点标记为活跃并记住其主机名, 同时跟踪是否添加了新的执行器
    var newExecAvail = false

    // 维护资源信息
    // (host -> (executor1,executor2, ...))
    // (executorId -> host)
    // (executorId -> (runningTasks...))
    for (o <- offers) {
      if (!hostToExecutors.contains(o.host)) {
        hostToExecutors(o.host) = new HashSet[String]()
      }

      if (!executorIdToRunningTaskIds.contains(o.executorId)) {
        hostToExecutors(o.host) += o.executorId
        executorAdded(o.executorId, o.host)
        executorIdToHost(o.executorId) = o.host
        executorIdToRunningTaskIds(o.executorId) = HashSet[Long]()
        newExecAvail = true
      }
    }

    // 更新机架信息
    //  (rack -> (hosts...))
    val hosts = offers.map(_.host).distinct
    for ((host, Some(rack)) <- hosts.zip(getRacksForHosts(hosts))) {
      hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += host
    }

    // 在提出任何任务之前，包含所有超时失效的节点。
    // 在这里进行此操作以避免单独的线程和额外的同步开销，
    // 同时因为在任务提出时更新被排除的执行器和节点才有意义。
    healthTrackerOpt.foreach(_.applyExcludeOnFailureTimeout())

    // 过滤掉被提出的 Executor 或者是 Node
    val filteredOffers = healthTrackerOpt.map { healthTracker =>
      offers.filter { offer =>
        !healthTracker.isNodeExcluded(offer.host) &&
          !healthTracker.isExecutorExcluded(offer.executorId)
      }
    }.getOrElse(offers)

    // 散列当前的work节点
    val shuffledOffers = shuffleOffers(filteredOffers)

    // 任务分配数组，用于存放待运行任务的插槽
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores / CPUS_PER_TASK))  // [Task-ArrayBuffer-1, Task-ArrayBuffer-2, ...]
    // 可用的cpu集合
    val availableCpus = shuffledOffers.map(o => o.cores).toArray           // [CoreNum-1, CoreNum-2, ...]

    val availableResources = shuffledOffers.map(_.resources).toArray
    val resourceProfileIds = shuffledOffers.map(o => o.resourceProfileId).toArray

    // 获取基于优先级排队的 tasksetManager 队列
    val sortedTaskSets = rootPool.getSortedTaskSetQueue

    for (taskSet <- sortedTaskSets) {
      logDebug("parentName: %s, name: %s, runningTasks: %s".format(taskSet.parent.name, taskSet.name, taskSet.runningTasks))
      if (newExecAvail) {
        // 重新计算任务本地性
        taskSet.executorAdded()
      }
    }

    // 按照调度顺序取出每个 TaskSet，然后按照本地性级别从低到高的顺序将其分配给每个节点，
    // 以便每个节点都有机会启动本地任务。
    // 注意：preferredLocality 的顺序是：PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY。
    for (taskSet <- sortedTaskSets) {
      // 如果使用障碍调度，我们只需要计算可用槽位，否则值为 -1。
      val numBarrierSlotsAvailable = if (taskSet.isBarrier) {
        val rpId = taskSet.taskSet.resourceProfileId
        val availableResourcesAmount = availableResources.map { resourceMap =>
          // available addresses already takes into account if there are fractional
          // task resource requests
          resourceMap.map { case (name, addresses) => (name, addresses.length) }
        }
        calculateAvailableSlots(this, conf, rpId, resourceProfileIds, availableCpus,
          availableResourcesAmount)
      } else {
        -1
      }
      // Skip the barrier taskSet if the available slots are less than the number of pending tasks.
      if (taskSet.isBarrier && numBarrierSlotsAvailable < taskSet.numTasks) {
        // Skip the launch process.
        // TODO SPARK-24819 If the job requires more slots than available (both busy and free
        // slots), fail the job on submit.
        logInfo(s"Skip current round of resource offers for barrier stage ${taskSet.stageId} " +
          s"because the barrier taskSet requires ${taskSet.numTasks} slots, while the total " +
          s"number of available slots is $numBarrierSlotsAvailable.")
      } else {

        var launchedAnyTask = false
        var noDelaySchedulingRejects = true
        var globalMinLocality: Option[TaskLocality] = None

        // 迭代当前任务的数据本地性选择
        for (currentMaxLocality <- taskSet.myLocalityLevels) {
          var launchedTaskAtCurrentMaxLocality = false
          do {
            // 在指定的 taskset 使用 shuffledOffers 调度 task 任务
            // 每次 resourceOfferSingleTaskSet 对每个 offers 最多只调度一个 task,
            // 这是为了让作业均匀分布在所有的 work 上
            val (noDelayScheduleReject, minLocality) = resourceOfferSingleTaskSet(
              taskSet,
              currentMaxLocality,
              shuffledOffers,
              availableCpus,
              availableResources,
              tasks)

            launchedTaskAtCurrentMaxLocality = minLocality.isDefined // 表示可能会调用多次
            launchedAnyTask |= launchedTaskAtCurrentMaxLocality
            noDelaySchedulingRejects &= noDelayScheduleReject
            globalMinLocality = minTaskLocality(globalMinLocality, minLocality)
          } while (launchedTaskAtCurrentMaxLocality)
        }

        if (!legacyLocalityWaitReset) {
          if (noDelaySchedulingRejects) {
            if (launchedAnyTask &&
              (isAllFreeResources || noRejectsSinceLastReset.getOrElse(taskSet.taskSet, true))) {
              taskSet.resetDelayScheduleTimer(globalMinLocality)
              noRejectsSinceLastReset.update(taskSet.taskSet, true)
            }
          } else {
            noRejectsSinceLastReset.update(taskSet.taskSet, false)
          }
        }

        if (!launchedAnyTask) {
          taskSet.getCompletelyExcludedTaskIfAny(hostToExecutors).foreach { taskIndex =>
              // If the taskSet is unschedulable we try to find an existing idle excluded
              // executor and kill the idle executor and kick off an abortTimer which if it doesn't
              // schedule a task within the timeout will abort the taskSet if we were unable to
              // schedule any task from the taskSet.
              // Note 1: We keep track of schedulability on a per taskSet basis rather than on a per
              // task basis.
              // Note 2: The taskSet can still be aborted when there are more than one idle
              // excluded executors and dynamic allocation is on. This can happen when a killed
              // idle executor isn't replaced in time by ExecutorAllocationManager as it relies on
              // pending tasks and doesn't kill executors on idle timeouts, resulting in the abort
              // timer to expire and abort the taskSet.
              //
              // If there are no idle executors and dynamic allocation is enabled, then we would
              // notify ExecutorAllocationManager to allocate more executors to schedule the
              // unschedulable tasks else we will abort immediately.
              executorIdToRunningTaskIds.find(x => !isExecutorBusy(x._1)) match {
                case Some ((executorId, _)) =>
                  if (!unschedulableTaskSetToExpiryTime.contains(taskSet)) {
                    healthTrackerOpt.foreach(blt => blt.killExcludedIdleExecutor(executorId))
                    updateUnschedulableTaskSetTimeoutAndStartAbortTimer(taskSet, taskIndex)
                  }
                case None =>
                  //  Notify ExecutorAllocationManager about the unschedulable task set,
                  // in order to provision more executors to make them schedulable
                  if (Utils.isDynamicAllocationEnabled(conf)) {
                    if (!unschedulableTaskSetToExpiryTime.contains(taskSet)) {
                      logInfo("Notifying ExecutorAllocationManager to allocate more executors to" +
                        " schedule the unschedulable task before aborting" +
                        " stage ${taskSet.stageId}.")
                      dagScheduler.unschedulableTaskSetAdded(taskSet.taskSet.stageId,
                        taskSet.taskSet.stageAttemptId)
                      updateUnschedulableTaskSetTimeoutAndStartAbortTimer(taskSet, taskIndex)
                    }
                  } else {
                    // Abort Immediately
                    logInfo("Cannot schedule any task because all executors excluded from " +
                      "failures. No idle executors can be found to kill. Aborting stage " +
                      s"${taskSet.stageId}.")
                    taskSet.abortSinceCompletelyExcludedOnFailure(taskIndex)
                  }
              }
          }
        } else {
          // We want to defer killing any taskSets as long as we have a non excluded executor
          // which can be used to schedule a task from any active taskSets. This ensures that the
          // job can make progress.
          // Note: It is theoretically possible that a taskSet never gets scheduled on a
          // non-excluded executor and the abort timer doesn't kick in because of a constant
          // submission of new TaskSets. See the PR for more details.
          if (unschedulableTaskSetToExpiryTime.nonEmpty) {
            logInfo("Clearing the expiry times for all unschedulable taskSets as a task was " +
              "recently scheduled.")
            // Notify ExecutorAllocationManager as well as other subscribers that a task now
            // recently becomes schedulable
            dagScheduler.unschedulableTaskSetRemoved(taskSet.taskSet.stageId, taskSet.taskSet.stageAttemptId)
            unschedulableTaskSetToExpiryTime.clear()
          }
        }

        if (launchedAnyTask && taskSet.isBarrier) {
          val barrierPendingLaunchTasks = taskSet.barrierPendingLaunchTasks.values.toArray
          // Check whether the barrier tasks are partially launched.
          if (barrierPendingLaunchTasks.length != taskSet.numTasks) {
            if (legacyLocalityWaitReset) {
              // Legacy delay scheduling always reset the timer when there's a task that is able
              // to be scheduled. Thus, whenever there's a timer reset could happen during a single
              // round resourceOffer, tasks that don't get or have the preferred locations would
              // always reject the offered resources. As a result, the barrier taskset can't get
              // launched. And if we retry the resourceOffer, we'd go through the same path again
              // and get into the endless loop in the end.
              val errorMsg = s"Fail resource offers for barrier stage ${taskSet.stageId} " +
                s"because only ${barrierPendingLaunchTasks.length} out of a total number " +
                s"of ${taskSet.numTasks} tasks got resource offers. We highly recommend " +
                "you to use the non-legacy delay scheduling by setting " +
                s"${LEGACY_LOCALITY_WAIT_RESET.key} to false to get rid of this error."
              logWarning(errorMsg)
              taskSet.abort(errorMsg)
              throw SparkCoreErrors.sparkError(errorMsg)
            } else {
              val curTime = clock.getTimeMillis()
              if (curTime - taskSet.lastResourceOfferFailLogTime >
                TaskSetManager.BARRIER_LOGGING_INTERVAL) {
                logInfo("Releasing the assigned resource offers since only partial tasks can " +
                  "be launched. Waiting for later round resource offers.")
                taskSet.lastResourceOfferFailLogTime = curTime
              }
              barrierPendingLaunchTasks.foreach { task =>
                // revert all assigned resources
                availableCpus(task.assignedOfferIndex) += task.assignedCores
                task.assignedResources.foreach { case (rName, rInfo) =>
                  availableResources(task.assignedOfferIndex)(rName).appendAll(rInfo.addresses)
                }
                // re-add the task to the schedule pending list
                taskSet.addPendingTask(task.index)
              }
            }
          } else {
            // All tasks are able to launch in this barrier task set. Let's do
            // some preparation work before launching them.
            val launchTime = clock.getTimeMillis()
            val addressesWithDescs = barrierPendingLaunchTasks.map { task =>
              val taskDesc = taskSet.prepareLaunchingTask(
                task.execId,
                task.host,
                task.index,
                task.taskLocality,
                false,
                task.assignedCores,
                task.assignedResources,
                launchTime)
              addRunningTask(taskDesc.taskId, taskDesc.executorId, taskSet)
              tasks(task.assignedOfferIndex) += taskDesc
              shuffledOffers(task.assignedOfferIndex).address.get -> taskDesc
            }

            // materialize the barrier coordinator.
            maybeInitBarrierCoordinator()

            // Update the taskInfos into all the barrier task properties.
            val addressesStr = addressesWithDescs
              // Addresses ordered by partitionId
              .sortBy(_._2.partitionId)
              .map(_._1)
              .mkString(",")
            addressesWithDescs.foreach(_._2.properties.setProperty("addresses", addressesStr))

            logInfo(s"Successfully scheduled all the ${addressesWithDescs.size} tasks for " +
              s"barrier stage ${taskSet.stageId}.")
          }
          taskSet.barrierPendingLaunchTasks.clear()
        }
      }
    }

    // TODO SPARK-24823 Cancel a job that contains barrier stage(s) if the barrier tasks don't get
    // launched within a configured time.
    if (tasks.nonEmpty) {
      hasLaunchedTask = true
    }
    return tasks.map(_.toSeq)
  }

  private def updateUnschedulableTaskSetTimeoutAndStartAbortTimer(
      taskSet: TaskSetManager,
      taskIndex: Int): Unit = {
    val timeout = conf.get(config.UNSCHEDULABLE_TASKSET_TIMEOUT) * 1000
    unschedulableTaskSetToExpiryTime(taskSet) = clock.getTimeMillis() + timeout
    logInfo(s"Waiting for $timeout ms for completely " +
      s"excluded task to be schedulable again before aborting stage ${taskSet.stageId}.")
    abortTimer.schedule(
      createUnschedulableTaskSetAbortTimer(taskSet, taskIndex), timeout)
  }

  private def createUnschedulableTaskSetAbortTimer(
      taskSet: TaskSetManager,
      taskIndex: Int): TimerTask = {
    new TimerTask() {
      override def run(): Unit = TaskSchedulerImpl.this.synchronized {
        if (unschedulableTaskSetToExpiryTime.contains(taskSet) &&
            unschedulableTaskSetToExpiryTime(taskSet) <= clock.getTimeMillis()) {
          logInfo("Cannot schedule any task because all executors excluded due to failures. " +
            s"Wait time for scheduling expired. Aborting stage ${taskSet.stageId}.")
          taskSet.abortSinceCompletelyExcludedOnFailure(taskIndex)
        } else {
          this.cancel()
        }
      }
    }
  }

  /**
   * Shuffle offers around to avoid always placing tasks on the same workers.  Exposed to allow
   * overriding in tests, so it can be deterministic.
   */
  protected def shuffleOffers(offers: IndexedSeq[WorkerOffer]): IndexedSeq[WorkerOffer] = {
    Random.shuffle(offers)
  }

  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer): Unit = {
    var failedExecutor: Option[String] = None
    var reason: Option[ExecutorLossReason] = None
    synchronized {
      try {
        Option(taskIdToTaskSetManager.get(tid)) match {
          case Some(taskSet) =>
            if (state == TaskState.LOST) {
              // TaskState.LOST is only used by the deprecated Mesos fine-grained scheduling mode,
              // where each executor corresponds to a single task, so mark the executor as failed.
              val execId = taskIdToExecutorId.getOrElse(tid, {
                val errorMsg =
                  "taskIdToTaskSetManager.contains(tid) <=> taskIdToExecutorId.contains(tid)"
                taskSet.abort(errorMsg)
                throw new SparkException(
                  "taskIdToTaskSetManager.contains(tid) <=> taskIdToExecutorId.contains(tid)")
              })
              if (executorIdToRunningTaskIds.contains(execId)) {
                reason = Some(
                  ExecutorProcessLost(
                    s"Task $tid was lost, so marking the executor as lost as well."))
                removeExecutor(execId, reason.get)
                failedExecutor = Some(execId)
              }
            }
            if (TaskState.isFinished(state)) {
              cleanupTaskState(tid)
              taskSet.removeRunningTask(tid)
              if (state == TaskState.FINISHED) {
                taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
              } else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
                taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
              }
            }
          case None =>
            logError(
              ("Ignoring update with state %s for TID %s because its task set is gone (this is " +
                "likely the result of receiving duplicate task finished status updates) or its " +
                "executor has been marked as failed.")
                .format(state, tid))
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // Update the DAGScheduler without holding a lock on this, since that can deadlock
    if (failedExecutor.isDefined) {
      assert(reason.isDefined)
      dagScheduler.executorLost(failedExecutor.get, reason.get)
      backend.reviveOffers()
    }
  }

  /**
   * Update metrics for in-progress tasks and executor metrics, and let the master know that the
   * BlockManager is still alive. Return true if the driver knows about the given block manager.
   * Otherwise, return false, indicating that the block manager should re-register.
   */
  override def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
      blockManagerId: BlockManagerId,
      executorUpdates: mutable.Map[(Int, Int), ExecutorMetrics]): Boolean = {
    // (taskId, stageId, stageAttemptId, accumUpdates)
    val accumUpdatesWithTaskIds: Array[(Long, Int, Int, Seq[AccumulableInfo])] = {
      accumUpdates.flatMap { case (id, updates) =>
        val accInfos = updates.map(acc => acc.toInfo(Some(acc.value), None))
        Option(taskIdToTaskSetManager.get(id)).map { taskSetMgr =>
          (id, taskSetMgr.stageId, taskSetMgr.taskSet.stageAttemptId, accInfos)
        }
      }
    }
    dagScheduler.executorHeartbeatReceived(execId, accumUpdatesWithTaskIds, blockManagerId,
      executorUpdates)
  }

  def handleTaskGettingResult(taskSetManager: TaskSetManager, tid: Long): Unit = synchronized {
    taskSetManager.handleTaskGettingResult(tid)
  }

  def handleSuccessfulTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskResult: DirectTaskResult[_]): Unit = synchronized {
    taskSetManager.handleSuccessfulTask(tid, taskResult)
  }

  def handleFailedTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskState: TaskState,
      reason: TaskFailedReason): Unit = synchronized {
    taskSetManager.handleFailedTask(tid, taskState, reason)
    if (!taskSetManager.isZombie && !taskSetManager.someAttemptSucceeded(tid)) {
      // Need to revive offers again now that the task set manager state has been updated to
      // reflect failed tasks that need to be re-run.
      backend.reviveOffers()
    }
  }

  /**
   * Marks the task has completed in the active TaskSetManager for the given stage.
   *
   * After stage failure and retry, there may be multiple TaskSetManagers for the stage.
   * If an earlier zombie attempt of a stage completes a task, we can ask the later active attempt
   * to skip submitting and running the task for the same partition, to save resource. That also
   * means that a task completion from an earlier zombie attempt can lead to the entire stage
   * getting marked as successful.
   */
  private[scheduler] def handlePartitionCompleted(stageId: Int, partitionId: Int) = synchronized {
    taskSetsByStageIdAndAttempt.get(stageId).foreach(_.values.filter(!_.isZombie).foreach { tsm =>
      tsm.markPartitionCompleted(partitionId)
    })
  }

  def error(message: String): Unit = {
    synchronized {
      if (taskSetsByStageIdAndAttempt.nonEmpty) {
        // Have each task set throw a SparkException with the error
        for {
          attempts <- taskSetsByStageIdAndAttempt.values
          manager <- attempts.values
        } {
          try {
            manager.abort(message)
          } catch {
            case e: Exception => logError("Exception in error callback", e)
          }
        }
      } else {
        // No task sets are active but we still got an error. Just exit since this
        // must mean the error is during registration.
        // It might be good to do something smarter here in the future.
        throw SparkCoreErrors.clusterSchedulerError(message)
      }
    }
  }

  override def stop(): Unit = {
    speculationScheduler.shutdown()
    if (backend != null) {
      backend.stop()
    }
    if (taskResultGetter != null) {
      taskResultGetter.stop()
    }
    if (barrierCoordinator != null) {
      barrierCoordinator.stop()
    }
    starvationTimer.cancel()
    abortTimer.cancel()
  }

  override def defaultParallelism(): Int = backend.defaultParallelism()

  // Check for speculatable tasks in all our active jobs.
  def checkSpeculatableTasks(): Unit = {
    var shouldRevive = false
    synchronized {
      shouldRevive = rootPool.checkSpeculatableTasks(MIN_TIME_TO_SPECULATION)
    }
    if (shouldRevive) {
      backend.reviveOffers()
    }
  }

  override def executorDecommission(
      executorId: String, decommissionInfo: ExecutorDecommissionInfo): Unit = {
    synchronized {
      // Don't bother noting decommissioning for executors that we don't know about
      if (executorIdToHost.contains(executorId)) {
        executorsPendingDecommission(executorId) =
          ExecutorDecommissionState(clock.getTimeMillis(), decommissionInfo.workerHost)
      }
    }
    rootPool.executorDecommission(executorId)
    backend.reviveOffers()
  }

  override def getExecutorDecommissionState(executorId: String)
    : Option[ExecutorDecommissionState] = synchronized {
    executorsPendingDecommission.get(executorId)
  }

  override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {
    var failedExecutor: Option[String] = None

    synchronized {
      if (executorIdToRunningTaskIds.contains(executorId)) {
        val hostPort = executorIdToHost(executorId)
        logExecutorLoss(executorId, hostPort, reason)
        removeExecutor(executorId, reason)
        failedExecutor = Some(executorId)
      } else {
        executorIdToHost.get(executorId) match {
          case Some(hostPort) =>
            // If the host mapping still exists, it means we don't know the loss reason for the
            // executor. So call removeExecutor() to update tasks running on that executor when
            // the real loss reason is finally known.
            logExecutorLoss(executorId, hostPort, reason)
            removeExecutor(executorId, reason)

          case None =>
            // We may get multiple executorLost() calls with different loss reasons. For example,
            // one may be triggered by a dropped connection from the worker while another may be a
            // report of executor termination from Mesos. We produce log messages for both so we
            // eventually report the termination reason.
            logError(s"Lost an executor $executorId (already removed): $reason")
        }
      }
    }
    // Call dagScheduler.executorLost without holding the lock on this to prevent deadlock
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get, reason)
      backend.reviveOffers()
    }
  }

  override def workerRemoved(workerId: String, host: String, message: String): Unit = {
    logInfo(s"Handle removed worker $workerId: $message")
    dagScheduler.workerRemoved(workerId, host, message)
  }

  private def logExecutorLoss(
      executorId: String,
      hostPort: String,
      reason: ExecutorLossReason): Unit = reason match {
    case LossReasonPending =>
      logDebug(s"Executor $executorId on $hostPort lost, but reason not yet known.")
    case ExecutorKilled =>
      logInfo(s"Executor $executorId on $hostPort killed by driver.")
    case _ =>
      logError(s"Lost executor $executorId on $hostPort: $reason")
  }

  /**
   * Cleans up the TaskScheduler's state for tracking the given task.
   */
  private def cleanupTaskState(tid: Long): Unit = {
    taskIdToTaskSetManager.remove(tid)
    taskIdToExecutorId.remove(tid).foreach { executorId =>
      executorIdToRunningTaskIds.get(executorId).foreach { _.remove(tid) }
    }
  }

  /**
   * Remove an executor from all our data structures and mark it as lost. If the executor's loss
   * reason is not yet known, do not yet remove its association with its host nor update the status
   * of any running tasks, since the loss reason defines whether we'll fail those tasks.
   */
  private def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
    // The tasks on the lost executor may not send any more status updates (because the executor
    // has been lost), so they should be cleaned up here.
    executorIdToRunningTaskIds.remove(executorId).foreach { taskIds =>
      logDebug("Cleaning up TaskScheduler state for tasks " +
        s"${taskIds.mkString("[", ",", "]")} on failed executor $executorId")
      // We do not notify the TaskSetManager of the task failures because that will
      // happen below in the rootPool.executorLost() call.
      taskIds.foreach(cleanupTaskState)
    }

    val host = executorIdToHost(executorId)
    val execs = hostToExecutors.getOrElse(host, new HashSet)
    execs -= executorId
    if (execs.isEmpty) {
      hostToExecutors -= host
      for (rack <- getRackForHost(host); hosts <- hostsByRack.get(rack)) {
        hosts -= host
        if (hosts.isEmpty) {
          hostsByRack -= rack
        }
      }
    }

    executorsPendingDecommission.remove(executorId)

    if (reason != LossReasonPending) {
      executorIdToHost -= executorId
      rootPool.executorLost(executorId, host, reason)
    }
    healthTrackerOpt.foreach(_.handleRemovedExecutor(executorId))
  }

  // 向DAGScheduler 告知有新的工作节点加入
  def executorAdded(execId: String, host: String): Unit = {
    dagScheduler.executorAdded(execId, host)
  }

  def getExecutorsAliveOnHost(host: String): Option[Set[String]] = synchronized {
    hostToExecutors.get(host).map(_.filterNot(isExecutorDecommissioned)).map(_.toSet)
  }

  def hasExecutorsAliveOnHost(host: String): Boolean = synchronized {
    hostToExecutors.get(host)
      .exists(executors => executors.exists(e => !isExecutorDecommissioned(e)))
  }

  def hasHostAliveOnRack(rack: String): Boolean = synchronized {
    hostsByRack.get(rack)
      .exists(hosts => hosts.exists(h => !isHostDecommissioned(h)))
  }

  def isExecutorAlive(execId: String): Boolean = synchronized {
    executorIdToRunningTaskIds.contains(execId) && !isExecutorDecommissioned(execId)
  }

  def isExecutorBusy(execId: String): Boolean = synchronized {
    executorIdToRunningTaskIds.get(execId).exists(_.nonEmpty)
  }

  // exposed for test
  protected final def isExecutorDecommissioned(execId: String): Boolean =
    getExecutorDecommissionState(execId).isDefined

  // exposed for test
  protected final def isHostDecommissioned(host: String): Boolean = {
    hostToExecutors.get(host).exists { executors =>
      executors.exists(e => getExecutorDecommissionState(e).exists(_.workerHost.isDefined))
    }
  }

  /**
   * Get a snapshot of the currently excluded nodes for the entire application. This is
   * thread-safe -- it can be called without a lock on the TaskScheduler.
   */
  def excludedNodes(): Set[String] = {
    healthTrackerOpt.map(_.excludedNodeList()).getOrElse(Set.empty)
  }

  /**
   * Get the rack for one host.
   *
   * Note that [[getRacksForHosts]] should be preferred when possible as that can be much
   * more efficient.
   */
  def getRackForHost(host: String): Option[String] = {
    getRacksForHosts(Seq(host)).head
  }

  /**
   * Get racks for multiple hosts.
   *
   * The returned Sequence will be the same length as the hosts argument and can be zipped
   * together with the hosts argument.
   */
  def getRacksForHosts(hosts: Seq[String]): Seq[Option[String]] = {
    hosts.map(_ => defaultRackValue)
  }

  private def waitBackendReady(): Unit = {
    if (backend.isReady) {
      return
    }
    while (!backend.isReady) {
      // Might take a while for backend to be ready if it is waiting on resources.
      if (sc.stopped.get) {
        // For example: the master removes the application for some reason
        throw new IllegalStateException("Spark context stopped while waiting for backend")
      }
      synchronized {
        this.wait(100)
      }
    }
  }

  override def applicationId(): String = backend.applicationId()

  override def applicationAttemptId(): Option[String] = backend.applicationAttemptId()

  // exposed for testing
  private[scheduler] def taskSetManagerForAttempt(
      stageId: Int,
      stageAttemptId: Int): Option[TaskSetManager] = synchronized {
    for {
      attempts <- taskSetsByStageIdAndAttempt.get(stageId)
      manager <- attempts.get(stageAttemptId)
    } yield {
      manager
    }
  }
}


private[spark] object TaskSchedulerImpl {

  val SCHEDULER_MODE_PROPERTY = SCHEDULER_MODE.key

  /**
   * Calculate the max available task slots given the `availableCpus` and `availableResources`
   * from a collection of ResourceProfiles. And only those ResourceProfiles who has the
   * same id with the `rpId` can be used to calculate the task slots.
   *
   * @param scheduler the TaskSchedulerImpl instance
   * @param conf SparkConf used to calculate the limiting resource and get the cpu amount per task
   * @param rpId the target ResourceProfile id. Only those ResourceProfiles who has the same id
   *             with it can be used to calculate the task slots.
   * @param availableRPIds an Array of ids of the available ResourceProfiles from the executors.
   * @param availableCpus an Array of the amount of available cpus from the executors.
   * @param availableResources an Array of the resources map from the executors. In the resource
   *                           map, it maps from the resource name to its amount.
   * @return the number of max task slots
   */
  def calculateAvailableSlots(
      scheduler: TaskSchedulerImpl,
      conf: SparkConf,
      rpId: Int,
      availableRPIds: Array[Int],
      availableCpus: Array[Int],
      availableResources: Array[Map[String, Int]]): Int = {
    val resourceProfile = scheduler.sc.resourceProfileManager.resourceProfileFromId(rpId)
    val coresKnown = resourceProfile.isCoresLimitKnown
    val (limitingResource, limitedByCpu) = {
      val limiting = resourceProfile.limitingResource(conf)
      // if limiting resource is empty then we have no other resources, so it has to be CPU
      if (limiting == ResourceProfile.CPUS || limiting.isEmpty) {
        (ResourceProfile.CPUS, true)
      } else {
        (limiting, false)
      }
    }
    val cpusPerTask = ResourceProfile.getTaskCpusOrDefaultForProfile(resourceProfile, conf)
    val taskLimit = resourceProfile.taskResources.get(limitingResource).map(_.amount).get

    availableCpus.zip(availableResources).zip(availableRPIds)
      .filter { case (_, id) => id == rpId }
      .map { case ((cpu, resources), _) =>
        val numTasksPerExecCores = cpu / cpusPerTask
        if (limitedByCpu) {
          numTasksPerExecCores
        } else {
          val availAddrs = resources.getOrElse(limitingResource, 0)
          val resourceLimit = (availAddrs / taskLimit).toInt
          // when executor cores config isn't set, we can't calculate the real limiting resource
          // and number of tasks per executor ahead of time, so calculate it now based on what
          // is available.
          if (!coresKnown && numTasksPerExecCores <= resourceLimit) {
            numTasksPerExecCores
          } else {
            resourceLimit
          }
        }
      }.sum
  }

  /**
   * Used to balance containers across hosts.
   *
   * Accepts a map of hosts to resource offers for that host, and returns a prioritized list of
   * resource offers representing the order in which the offers should be used. The resource
   * offers are ordered such that we'll allocate one container on each host before allocating a
   * second container on any host, and so on, in order to reduce the damage if a host fails.
   *
   * For example, given {@literal <h1, [o1, o2, o3]>}, {@literal <h2, [o4]>} and
   * {@literal <h3, [o5, o6]>}, returns {@literal [o1, o5, o4, o2, o6, o3]}.
   */
  def prioritizeContainers[K, T] (map: HashMap[K, ArrayBuffer[T]]): List[T] = {
    val _keyList = new ArrayBuffer[K](map.size)
    _keyList ++= map.keys

    // order keyList based on population of value in map
    val keyList = _keyList.sortWith(
      (left, right) => map(left).size > map(right).size
    )

    val retval = new ArrayBuffer[T](keyList.size * 2)
    var index = 0
    var found = true

    while (found) {
      found = false
      for (key <- keyList) {
        val containerList: ArrayBuffer[T] = map.getOrElse(key, null)
        assert(containerList != null)
        // Get the index'th entry for this host - if present
        if (index < containerList.size) {
          retval += containerList.apply(index)
          found = true
        }
      }
      index += 1
    }

    retval.toList
  }

  private def maybeCreateHealthTracker(sc: SparkContext): Option[HealthTracker] = {
    if (HealthTracker.isExcludeOnFailureEnabled(sc.conf)) {
      val executorAllocClient: Option[ExecutorAllocationClient] = sc.schedulerBackend match {
        case b: ExecutorAllocationClient => Some(b)
        case _ => None
      }
      Some(new HealthTracker(sc, executorAllocClient))
    } else {
      None
    }
  }

}
