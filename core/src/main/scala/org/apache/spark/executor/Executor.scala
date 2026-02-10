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

package org.apache.spark.executor

import java.io.{File, NotSerializableException}
import java.lang.Thread.UncaughtExceptionHandler
import java.lang.management.ManagementFactory
import java.net.{URI, URL}
import java.nio.ByteBuffer
import java.util.{Locale, Properties}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import javax.annotation.concurrent.GuardedBy
import javax.ws.rs.core.UriBuilder

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.collection.mutable.{ArrayBuffer, HashMap, Map, WrappedArray}
import scala.concurrent.duration._
import scala.util.control.NonFatal

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.slf4j.MDC

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.internal.plugin.PluginContainer
import org.apache.spark.memory.{SparkOutOfMemoryError, TaskMemoryManager}
import org.apache.spark.metrics.source.JVMCPUSource
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.scheduler._
import org.apache.spark.shuffle.{FetchFailedException, ShuffleBlockPusher}
import org.apache.spark.storage.{StorageLevel, TaskResultBlockId}
import org.apache.spark.util._
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Spark 执行器，由线程池支持来运行任务。
 *
 * 可与 Mesos、YARN、Kubernetes 和独立调度器一起使用。
 * 使用内部 RPC 接口与驱动程序通信，Mesos 细粒度模式除外。
 */
private[spark] class Executor(
    executorId: String,
    executorHostname: String,
    env: SparkEnv,
    userClassPath: Seq[URL] = Nil,
    isLocal: Boolean = false,
    uncaughtExceptionHandler: UncaughtExceptionHandler = new SparkUncaughtExceptionHandler,
    resources: immutable.Map[String, ResourceInformation])
  extends Logging {

  logInfo(s"Starting executor ID $executorId on host $executorHostname")

  private val executorShutdown = new AtomicBoolean(false)
  val stopHookReference = ShutdownHookManager.addShutdownHook(
    () => stop()
  )

  // 应用依赖（通过 SparkContext 添加），我们在此节点上已获取的。
  // 每个映射保存我们获取的文件或 JAR 版本的主节点时间戳。
  private val currentFiles: HashMap[String, Long] = new HashMap[String, Long]()
  private val currentJars: HashMap[String, Long] = new HashMap[String, Long]()
  private val currentArchives: HashMap[String, Long] = new HashMap[String, Long]()

  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  private[executor] val conf = env.conf

  // 没有 IP 或 host:port - 只有主机名
  Utils.checkHost(executorHostname)

  assert (0 == Utils.parseHostPort(executorHostname)._2) // 不能指定端口。

  // 确保我们报告的本地主机名与集群调度器对此主机的名称匹配
  Utils.setCustomHostname(executorHostname)

  if (!isLocal) {
    // 为非本地模式设置未捕获异常处理器。
    // 使由于未捕获异常导致的任何线程终止都会终止整个执行器进程，以避免令人惊讶的停顿。
    Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler)
  }

  // 启动工作线程池- 用来执行 Task
  private[executor] val threadPool = {
    val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("Executor task launch worker-%d")
      .setThreadFactory((r: Runnable) => new UninterruptibleThread(r, "unused"))
      .build()
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }


  private val schemes = conf.get(EXECUTOR_METRICS_FILESYSTEM_SCHEMES)
    .toLowerCase(Locale.ROOT).split(",").map(_.trim).filter(_.nonEmpty)

  private val executorSource = new ExecutorSource(threadPool, executorId, schemes)
  // 用于监督任务终止/取消的线程池
  private val taskReaperPool = ThreadUtils.newDaemonCachedThreadPool("Task reaper")
  // 对于正在被终止的任务，此映射保存最近创建的TaskReaper。
  // 对此映射的所有访问都应在映射本身上同步（这不是 ConcurrentHashMap，因为我们使用同步的目的不仅仅是保护映射内部状态的完整性）。
  // 此映射的目的是防止为给定任务的每次 killTask() 创建单独的 TaskReaper。
  // 相反，此映射允许我们跟踪现有 TaskReaper 是否满足我们原本会创建的 TaskReaper 的角色。映射键是task ID。
  private val taskReaperForTask: HashMap[Long, TaskReaper] = HashMap[Long, TaskReaper]()

  val executorMetricsSource =
    if (conf.get(METRICS_EXECUTORMETRICS_SOURCE_ENABLED)) {
      Some(new ExecutorMetricsSource)
    } else {
      None
    }

  if (!isLocal) {
    env.blockManager.initialize(conf.getAppId)
    env.metricsSystem.registerSource(executorSource)
    env.metricsSystem.registerSource(new JVMCPUSource())
    executorMetricsSource.foreach(_.register(env.metricsSystem))
    env.metricsSystem.registerSource(env.blockManager.shuffleMetricsSource)
  } else {
    // 这使得在本地模式下可以注册执行器源。
    // 实际注册发生在 SparkContext 中，
    // 不能在这里完成，因为 appId 尚不可用
    Executor.executorSourceLocalModeOnly = executorSource
  }

  // 是否在 Spark JAR 之前加载用户 JAR 中的类
  private val userClassPathFirst = conf.get(EXECUTOR_USER_CLASS_PATH_FIRST)

  // 是否监控已终止/中断的任务
  private val taskReaperEnabled = conf.get(TASK_REAPER_ENABLED)

  private val killOnFatalErrorDepth = conf.get(EXECUTOR_KILL_ON_FATAL_ERROR_DEPTH)

  // 创建我们的 ClassLoader
  // 在 SparkEnv 创建后执行此操作，以便可以访问 SecurityManager
  private val urlClassLoader = createClassLoader()
  private val replClassLoader = addReplClassLoaderIfNeeded(urlClassLoader)

  // 为序列化器设置类加载器
  env.serializer.setDefaultClassLoader(replClassLoader)
  // SPARK-21928。
  // SerializerManager 的内部 Kryo 实例可能会在 netty 线程中使用用于获取远程缓存的 RDD 块，因此需要确保它也使用正确的类加载器。
  env.serializerManager.setDefaultClassLoader(replClassLoader)

  // 直接结果的最大大小。如果任务结果大于此值，我们使用块管理器将结果发送回去。
  private val maxDirectResultSize = Math.min(
    conf.get(TASK_MAX_DIRECT_RESULT_SIZE),
    RpcUtils.maxMessageSizeBytes(conf))

  private val maxResultSize = conf.get(MAX_RESULT_SIZE)

  // 维护正在运行的任务列表。
  private[executor] val runningTasks = new ConcurrentHashMap[Long, TaskRunner]

  // 终止标记 TTL（毫秒）- 10 秒。
  private val KILL_MARK_TTL_MS = 10000L

  // 带有 interruptThread 标志、终止原因和时间戳的终止标记。
  // 这是为了避免在 launchTask() 之前调用 killTask() 时丢失终止事件。
  private[executor] val killMarks = new ConcurrentHashMap[Long, (Boolean, String, Long)]

  private val killMarkCleanupTask = new Runnable {
    override def run(): Unit = {
      val oldest = System.currentTimeMillis() - KILL_MARK_TTL_MS
      val iter = killMarks.entrySet().iterator()
      while (iter.hasNext) {
        if (iter.next().getValue._3 < oldest) {
          iter.remove()
        }
      }
    }
  }

  // 终止标记清理线程执行器。
  private val killMarkCleanupService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("executor-kill-mark-cleanup")

  killMarkCleanupService.scheduleAtFixedRate(
    killMarkCleanupTask, KILL_MARK_TTL_MS, KILL_MARK_TTL_MS, TimeUnit.MILLISECONDS)

  /**
   * 当执行器无法向驱动程序发送心跳超过 `HEARTBEAT_MAX_FAILURES`次时，它应该终止自己。
   * 默认值为 60。例如，如果最大失败次数为 60 且心跳间隔为 10 秒，则它将尝试发送心跳最多 600 秒（10 分钟）。
   */
  private val HEARTBEAT_MAX_FAILURES = conf.get(EXECUTOR_HEARTBEAT_MAX_FAILURES)

  /**
   * 是否从发送到驱动程序的心跳中删除空累加器。包含空累加器（满足 isZero）可能会使心跳消息的大小非常大。
   */
  private val HEARTBEAT_DROP_ZEROES = conf.get(EXECUTOR_HEARTBEAT_DROP_ZERO_ACCUMULATOR_UPDATES)

  /**
   * 发送心跳的间隔，以毫秒为单位
   */
  private val HEARTBEAT_INTERVAL_MS = conf.get(EXECUTOR_HEARTBEAT_INTERVAL)

  /**
   * 轮询执行器指标的间隔，以毫秒为单位
   */
  private val METRICS_POLLING_INTERVAL_MS = conf.get(EXECUTOR_METRICS_POLLING_INTERVAL)

  private val pollOnHeartbeat = if (METRICS_POLLING_INTERVAL_MS > 0) false else true

  // 内存指标的轮询器。对测试可见。
  private[executor] val metricsPoller = new ExecutorMetricsPoller(
    env.memoryManager,
    METRICS_POLLING_INTERVAL_MS,
    executorMetricsSource)

  // 心跳任务的执行器。
  private val heartbeater = new Heartbeater(
    () => Executor.this.reportHeartBeat(),
    "executor-heartbeater",
    HEARTBEAT_INTERVAL_MS)

  // 必须在运行 startDriverHeartbeat() 之前初始化
  private val heartbeatReceiverRef = RpcUtils.makeDriverRef(HeartbeatReceiver.ENDPOINT_NAME, conf, env.rpcEnv)

  /**
   * 计算心跳失败次数。它应该只在心跳线程中访问。每次
   * 成功的心跳将其重置为 0。
   */
  private var heartbeatFailures = 0

  /**
   * 防止在退役时启动新任务的标志。访问此标志可能存在竞争条件，
   * 但退役只是为了帮助，而不是硬停止。
   */
  private var decommissioned = false

  heartbeater.start()

  private val appStartTime = conf.getLong("spark.app.startTime", 0)

  // 为了允许用户分发插件及其所需的文件
  // 通过应用程序提交时的 --jars、--files 和 --archives 指定，这些
  // jars/files/archives 应该被下载并通过
  // updateDependencies 添加到类加载器。这应该在下面的插件初始化之前完成
  // 因为执行器从类加载器搜索插件并初始化它们。
  private val Seq(initialUserJars, initialUserFiles, initialUserArchives) =
    Seq("jar", "file", "archive").map { key =>
      conf.getOption(s"spark.app.initial.$key.urls").map { urls =>
        Map(urls.split(",").map(url => (url, appStartTime)): _*)
      }.getOrElse(Map.empty)
    }
  updateDependencies(initialUserFiles, initialUserJars, initialUserArchives)

  // 插件需要使用包含执行器用户类路径的类加载器加载。
  // 插件还需要在心跳启动后初始化
  // 以避免阻塞发送心跳（参见 SPARK-32175）。
  private val plugins: Option[PluginContainer] = Utils.withContextClassLoader(replClassLoader) {
    PluginContainer(env, resources.asJava)
  }

  metricsPoller.start()

  private[executor] def numRunningTasks: Int = runningTasks.size()

  /**
   * 标记执行器为退役并避免启动新任务。
   */
  private[spark] def decommission(): Unit = {
    decommissioned = true
  }

  private[executor] def createTaskRunner(context: ExecutorBackend,
    taskDescription: TaskDescription) = new TaskRunner(context, taskDescription, plugins)

  // 运行任务 - 创建一个 TaskRunner, 提交给线程池来处理
  def launchTask(context: ExecutorBackend, taskDescription: TaskDescription): Unit = {
    val taskId = taskDescription.taskId
    val tr = createTaskRunner(context, taskDescription)
    runningTasks.put(taskId, tr)
    val killMark = killMarks.get(taskId)
    if (killMark != null) {
      tr.kill(killMark._1, killMark._2)
      killMarks.remove(taskId)
    }
    threadPool.execute(tr)
    if (decommissioned) {
      log.error(s"Launching a task while in decommissioned state.")
    }
  }

  def killTask(taskId: Long, interruptThread: Boolean, reason: String): Unit = {
    killMarks.put(taskId, (interruptThread, reason, System.currentTimeMillis()))
    val taskRunner = runningTasks.get(taskId)
    if (taskRunner != null) {
      if (taskReaperEnabled) {
        val maybeNewTaskReaper: Option[TaskReaper] = taskReaperForTask.synchronized {
          val shouldCreateReaper = taskReaperForTask.get(taskId) match {
            case None => true
            case Some(existingReaper) => interruptThread && !existingReaper.interruptThread
          }
          if (shouldCreateReaper) {
            val taskReaper = new TaskReaper(
              taskRunner, interruptThread = interruptThread, reason = reason)
            taskReaperForTask(taskId) = taskReaper
            Some(taskReaper)
          } else {
            None
          }
        }
        // 从同步块外部执行 TaskReaper。
        maybeNewTaskReaper.foreach(taskReaperPool.execute)
      } else {
        taskRunner.kill(interruptThread = interruptThread, reason = reason)
      }
      // 安全地删除终止标记，因为我们有机会使用 TaskRunner。
      killMarks.remove(taskId)
    }
  }

  /**
   * 终止执行器中正在运行的任务的函数。
   * 这可以由执行器后端调用以终止
   * 任务，而不是关闭 JVM。
   * @param interruptThread 是否中断任务线程
   */
  def killAllTasks(interruptThread: Boolean, reason: String) : Unit = {
    runningTasks.keys().asScala.foreach(t =>
      killTask(t, interruptThread = interruptThread, reason = reason))
  }

  def stop(): Unit = {
    if (!executorShutdown.getAndSet(true)) {
      ShutdownHookManager.removeShutdownHook(stopHookReference)
      env.metricsSystem.report()
      try {
        if (metricsPoller != null) {
          metricsPoller.stop()
        }
      } catch {
        case NonFatal(e) =>
          logWarning("Unable to stop executor metrics poller", e)
      }
      try {
        if (heartbeater != null) {
          heartbeater.stop()
        }
      } catch {
        case NonFatal(e) =>
          logWarning("Unable to stop heartbeater", e)
      }
      ShuffleBlockPusher.stop()
      if (threadPool != null) {
        threadPool.shutdown()
      }
      if (killMarkCleanupService != null) {
        killMarkCleanupService.shutdown()
      }
      if (replClassLoader != null && plugins != null) {
        // 通知插件执行器正在关闭，以便它们可以干净地终止
        Utils.withContextClassLoader(replClassLoader) {
          plugins.foreach(_.shutdown())
        }
      }
      if (!isLocal) {
        env.stop()
      }
    }
  }

  /** 返回此 JVM 进程在垃圾回收中花费的总时间。 */
  private def computeTotalGcTime(): Long = {
    ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionTime).sum
  }

  class TaskRunner(
      execBackend: ExecutorBackend,                    // 任务运行后端(yarn/k8s/...)
      private val taskDescription: TaskDescription,    // 任务描述信息，包含其序列化信息
      private val plugins: Option[PluginContainer])
    extends Runnable {

    val taskId = taskDescription.taskId
    val taskName = taskDescription.name
    val threadName = s"Executor task launch worker for $taskName"
    val mdcProperties = taskDescription.properties.asScala
      .filter(_._1.startsWith("mdc.")).toSeq

    /** 如果指定，此任务已被终止，此选项包含原因。 */
    @volatile private var reasonIfKilled: Option[String] = None

    @volatile private var threadId: Long = -1

    def getThreadId: Long = threadId

    /** 此任务是否已完成。 */
    @GuardedBy("TaskRunner.this")
    private var finished = false
    def isFinished: Boolean = synchronized { finished }

    /** 任务开始运行时 JVM 进程在 GC 中花费的时间。 */
    @volatile var startGCTime: Long = _

    /**
     * 要运行的任务。这将在 run() 中通过反序列化来自驱动程序的任务二进制文件来设置。一旦设置，它将永远不会改变。
     */
    @volatile var task: Task[Any] = _

    def kill(interruptThread: Boolean, reason: String): Unit = {
      logInfo(s"Executor is trying to kill $taskName, reason: $reason")
      reasonIfKilled = Some(reason)
      if (task != null) {
        synchronized {
          if (!finished) {
            task.kill(interruptThread, reason)
          }
        }
      }
    }

    /**
     * 将 finished 标志设置为 true 并清除当前线程的中断状态
     */
    private def setTaskFinishedAndClearInterruptStatus(): Unit = synchronized {
      this.finished = true
      // SPARK-14234 - 重置线程的中断状态以避免
      // execBackend.statusUpdate 期间的 ClosedByInterruptException，这会导致
      // 执行器崩溃
      Thread.interrupted()
      // 通知任何等待的 TaskReapers。通常每个任务只有一个 reaper，但有
      // 一个罕见的边缘情况，即一个任务可以有两个 reapers，以防 cancel(interrupt=False)
      // 后跟 cancel(interrupt=True)。因此我们使用 notifyAll() 以避免丢失唤醒：
      notifyAll()
    }

    /**
     * 实用函数：
     *   1. 如果可能，报告执行器运行时间和 JVM gc 时间
     *   2. 收集累加器更新
     *   3. 将 finished 标志设置为 true 并清除当前线程的中断状态
     */
    private def collectAccumulatorsAndResetStatusOnFailure(taskStartTimeNs: Long) = {
      // 报告执行器运行时间和 JVM gc 时间
      Option(task).foreach(t => {
        t.metrics.setExecutorRunTime(TimeUnit.NANOSECONDS.toMillis(
          // SPARK-32898：当 taskStartTimeNs 仍具有初始值（=0）时，任务可能被终止。
          // 在这种情况下，executorRunTime 应被视为 0。
          if (taskStartTimeNs > 0) System.nanoTime() - taskStartTimeNs else 0))
        t.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
      })

      // 收集最新的累加器值以报告回驱动程序
      val accums: Seq[AccumulatorV2[_, _]] =
        Option(task).map(_.collectAccumulatorUpdates(taskFailed = true)).getOrElse(Seq.empty)
      val accUpdates = accums.map(acc => acc.toInfo(Some(acc.value), None))

      setTaskFinishedAndClearInterruptStatus()
      (accums, accUpdates)
    }

    override def run(): Unit = {
      setMDCForTask(taskName, mdcProperties)
      // 线程处理
      threadId = Thread.currentThread.getId
      Thread.currentThread.setName(threadName)  // Executor task launch worker for $taskName

      val threadMXBean = ManagementFactory.getThreadMXBean
      val taskMemoryManager = new TaskMemoryManager(env.memoryManager, taskId)   // 获取任务执行内存管理
      val deserializeStartTimeNs = System.nanoTime()

      val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
        threadMXBean.getCurrentThreadCpuTime
      } else 0L

      Thread.currentThread.setContextClassLoader(replClassLoader)
      val ser = env.closureSerializer.newInstance()
      logInfo(s"Running $taskName")

      // 更新任务执行状态为Running
      execBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
      var taskStartTimeNs: Long = 0
      var taskStartCpu: Long = 0
      startGCTime = computeTotalGcTime()
      var taskStarted: Boolean = false

      try {
        Executor.taskDeserializationProps.set(taskDescription.properties)
        updateDependencies(
          taskDescription.addedFiles, taskDescription.addedJars, taskDescription.addedArchives)

        // 反序列化得到 Task 执行实体
        task = ser.deserialize[Task[Any]](
          taskDescription.serializedTask, Thread.currentThread.getContextClassLoader)
        task.localProperties = taskDescription.properties
        task.setTaskMemoryManager(taskMemoryManager)

        // 如果此任务在我们反序列化它之前已被终止，现在退出。否则，
        // 继续执行任务。
        val killReason = reasonIfKilled
        if (killReason.isDefined) {
          // 抛出异常而不是返回，因为在 try{} 块内返回
          // 会导致抛出 NonLocalReturnControl 异常。NonLocalReturnControl
          // 异常将被 catch 块捕获，导致任务的
          // ExceptionFailure 不正确。
          throw new TaskKilledException(killReason.get)
        }

        // 在此处更新 epoch 的目的是使执行器 map 输出状态缓存失效
        // 以防发生 FetchFailures。在本地模式下，`env.mapOutputTracker` 将是
        // MapOutputTrackerMaster，其缓存失效不基于 epoch 数字，因此
        // 我们不需要在这里进行任何特殊调用。
        if (!isLocal) {
          logDebug(s"$taskName's epoch is ${task.epoch}")
          env.mapOutputTracker.asInstanceOf[MapOutputTrackerWorker].updateEpoch(task.epoch)
        }

        metricsPoller.onTaskStart(taskId, task.stageId, task.stageAttemptId)
        taskStarted = true

        // 运行实际任务并测量其运行时间。
        taskStartTimeNs = System.nanoTime()
        taskStartCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime
        } else 0L
        var threwException = true
        val value = Utils.tryWithSafeFinally {
          // 开始执行该任务
          val res = task.run(
            taskAttemptId = taskId,
            attemptNumber = taskDescription.attemptNumber,
            metricsSystem = env.metricsSystem,
            cpus = taskDescription.cpus,
            resources = taskDescription.resources,
            plugins = plugins)
          threwException = false
          res
        } {
          // 任务运行完成安全收尾工作
          val releasedLocks = env.blockManager.releaseAllLocksForTask(taskId)
          val freedMemory = taskMemoryManager.cleanUpAllAllocatedMemory()

          if (freedMemory > 0 && !threwException) {
            val errMsg = s"Managed memory leak detected; size = $freedMemory bytes, $taskName"
            if (conf.get(UNSAFE_EXCEPTION_ON_MEMORY_LEAK)) {
              throw new SparkException(errMsg)
            } else {
              logWarning(errMsg)
            }
          }

          if (releasedLocks.nonEmpty && !threwException) {
            val errMsg = s"${releasedLocks.size} block locks were not released by $taskName\n" +
                releasedLocks.mkString("[", ", ", "]")
            if (conf.get(STORAGE_EXCEPTION_PIN_LEAK)) {
              throw new SparkException(errMsg)
            } else {
              logInfo(errMsg)
            }
          }
        }
        task.context.fetchFailed.foreach { fetchFailure =>
          // 哎呀。看起来用户代码捕获了 fetch-failure 而没有抛出任何其他异常。
          // 这*可能*是用户想要做的（尽管极不可能）。所以我们将记录一个错误并继续。
          logError(s"$taskName completed successfully though internally it encountered " +
            s"unrecoverable fetch failures!  Most likely this means user code is incorrectly " +
            s"swallowing Spark's internal ${classOf[FetchFailedException]}", fetchFailure)
        }
        val taskFinishNs = System.nanoTime()
        val taskFinishCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime
        } else 0L

        // 如果任务已被终止，让我们使其失败。
        task.context.killTaskIfInterrupted()

        val resultSer = env.serializer.newInstance()
        val beforeSerializationNs = System.nanoTime()
        val valueBytes = resultSer.serialize(value)
        val afterSerializationNs = System.nanoTime()

        // 反序列化分两部分进行：首先，我们反序列化一个 Task 对象，其中包括 Partition。其次，Task.run() 反序列化要运行的 RDD 和函数。
        task.metrics.setExecutorDeserializeTime(TimeUnit.NANOSECONDS.toMillis((taskStartTimeNs - deserializeStartTimeNs) + task.executorDeserializeTimeNs))
        task.metrics.setExecutorDeserializeCpuTime((taskStartCpu - deserializeStartCpuTime) + task.executorDeserializeCpuTime)
        // 我们需要减去 Task.run() 的反序列化时间以避免重复计算
        task.metrics.setExecutorRunTime(TimeUnit.NANOSECONDS.toMillis((taskFinishNs - taskStartTimeNs) - task.executorDeserializeTimeNs))
        task.metrics.setExecutorCpuTime((taskFinishCpu - taskStartCpu) - task.executorDeserializeCpuTime)
        task.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
        task.metrics.setResultSerializationTime(TimeUnit.NANOSECONDS.toMillis(afterSerializationNs - beforeSerializationNs))

        // 使用 Dropwizard 指标系统公开任务指标。 更新任务指标计数器
        executorSource.METRIC_CPU_TIME.inc(task.metrics.executorCpuTime)
        executorSource.METRIC_RUN_TIME.inc(task.metrics.executorRunTime)
        executorSource.METRIC_JVM_GC_TIME.inc(task.metrics.jvmGCTime)
        executorSource.METRIC_DESERIALIZE_TIME.inc(task.metrics.executorDeserializeTime)
        executorSource.METRIC_DESERIALIZE_CPU_TIME.inc(task.metrics.executorDeserializeCpuTime)
        executorSource.METRIC_RESULT_SERIALIZE_TIME.inc(task.metrics.resultSerializationTime)
        executorSource.METRIC_SHUFFLE_FETCH_WAIT_TIME.inc(task.metrics.shuffleReadMetrics.fetchWaitTime)
        executorSource.METRIC_SHUFFLE_WRITE_TIME.inc(task.metrics.shuffleWriteMetrics.writeTime)
        executorSource.METRIC_SHUFFLE_TOTAL_BYTES_READ.inc(task.metrics.shuffleReadMetrics.totalBytesRead)
        executorSource.METRIC_SHUFFLE_REMOTE_BYTES_READ.inc(task.metrics.shuffleReadMetrics.remoteBytesRead)
        executorSource.METRIC_SHUFFLE_REMOTE_BYTES_READ_TO_DISK.inc(task.metrics.shuffleReadMetrics.remoteBytesReadToDisk)
        executorSource.METRIC_SHUFFLE_LOCAL_BYTES_READ.inc(task.metrics.shuffleReadMetrics.localBytesRead)
        executorSource.METRIC_SHUFFLE_RECORDS_READ.inc(task.metrics.shuffleReadMetrics.recordsRead)
        executorSource.METRIC_SHUFFLE_REMOTE_BLOCKS_FETCHED.inc(task.metrics.shuffleReadMetrics.remoteBlocksFetched)
        executorSource.METRIC_SHUFFLE_LOCAL_BLOCKS_FETCHED.inc(task.metrics.shuffleReadMetrics.localBlocksFetched)
        executorSource.METRIC_SHUFFLE_BYTES_WRITTEN.inc(task.metrics.shuffleWriteMetrics.bytesWritten)
        executorSource.METRIC_SHUFFLE_RECORDS_WRITTEN.inc(task.metrics.shuffleWriteMetrics.recordsWritten)
        executorSource.METRIC_INPUT_BYTES_READ.inc(task.metrics.inputMetrics.bytesRead)
        executorSource.METRIC_INPUT_RECORDS_READ.inc(task.metrics.inputMetrics.recordsRead)
        executorSource.METRIC_OUTPUT_BYTES_WRITTEN.inc(task.metrics.outputMetrics.bytesWritten)
        executorSource.METRIC_OUTPUT_RECORDS_WRITTEN.inc(task.metrics.outputMetrics.recordsWritten)
        executorSource.METRIC_RESULT_SIZE.inc(task.metrics.resultSize)
        executorSource.METRIC_DISK_BYTES_SPILLED.inc(task.metrics.diskBytesSpilled)
        executorSource.METRIC_MEMORY_BYTES_SPILLED.inc(task.metrics.memoryBytesSpilled)

        // 注意：必须在 TaskMetrics 更新后收集累加器更新
        val accumUpdates = task.collectAccumulatorUpdates()
        val metricPeaks = metricsPoller.getTaskMetricPeaks(taskId)
        // TODO: do not serialize value twice
        val directResult = new DirectTaskResult(valueBytes, accumUpdates, metricPeaks)
        val serializedDirectResult = ser.serialize(directResult)
        val resultSize = serializedDirectResult.limit()

        // TODO: 不要序列化值两次
        // 将结果序列化
        val serializedResult: ByteBuffer = {
          if (maxResultSize > 0 && resultSize > maxResultSize) {
            logWarning(s"Finished $taskName. Result is larger than maxResultSize " +
              s"(${Utils.bytesToString(resultSize)} > ${Utils.bytesToString(maxResultSize)}), " +
              s"dropping it.")
            ser.serialize(new IndirectTaskResult[Any](TaskResultBlockId(taskId), resultSize))
          } else if (resultSize > maxDirectResultSize) {   // 数据量过大，则将数据序列化到BlockManager 上
            val blockId = TaskResultBlockId(taskId)
            env.blockManager.putBytes(
              blockId,
              new ChunkedByteBuffer(serializedDirectResult.duplicate()),
              StorageLevel.MEMORY_AND_DISK_SER)
            logInfo(s"Finished $taskName. $resultSize bytes result sent via BlockManager)")
            ser.serialize(new IndirectTaskResult[Any](blockId, resultSize))
          } else {
            logInfo(s"Finished $taskName. $resultSize bytes result sent to driver")
            serializedDirectResult
          }
        }

        executorSource.SUCCEEDED_TASKS.inc(1L)
        setTaskFinishedAndClearInterruptStatus()
        plugins.foreach(_.onTaskSucceeded())
        execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult)
      } catch {
        case t: TaskKilledException =>
          logInfo(s"Executor killed $taskName, reason: ${t.reason}")

          val (accums, accUpdates) = collectAccumulatorsAndResetStatusOnFailure(taskStartTimeNs)
          // 这里和下面，将任务指标峰值放入 WrappedArray 以将它们公开为 Seq 而不需要复制。
          val metricPeaks = WrappedArray.make(metricsPoller.getTaskMetricPeaks(taskId))
          val reason = TaskKilled(t.reason, accUpdates, accums, metricPeaks.toSeq)
          plugins.foreach(_.onTaskFailed(reason))
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(reason))

        case _: InterruptedException | NonFatal(_) if
            task != null && task.reasonIfKilled.isDefined =>
          val killReason = task.reasonIfKilled.getOrElse("unknown reason")
          logInfo(s"Executor interrupted and killed $taskName, reason: $killReason")

          val (accums, accUpdates) = collectAccumulatorsAndResetStatusOnFailure(taskStartTimeNs)
          val metricPeaks = WrappedArray.make(metricsPoller.getTaskMetricPeaks(taskId))
          val reason = TaskKilled(killReason, accUpdates, accums, metricPeaks.toSeq)
          plugins.foreach(_.onTaskFailed(reason))
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(reason))

        case t: Throwable if hasFetchFailure && !Executor.isFatalError(t, killOnFatalErrorDepth) =>
          val reason = task.context.fetchFailed.get.toTaskFailedReason
          if (!t.isInstanceOf[FetchFailedException]) {
            // 任务中存在 fetch failure，但某些用户代码包装了该异常
            // 并抛出了其他内容。无论如何，我们将其视为 fetch failure。
            val fetchFailedCls = classOf[FetchFailedException].getName
            logWarning(s"$taskName encountered a ${fetchFailedCls} and " +
              s"failed, but the ${fetchFailedCls} was hidden by another " +
              s"exception.  Spark is handling this like a fetch failure and ignoring the " +
              s"other exception: $t")
          }
          setTaskFinishedAndClearInterruptStatus()
          plugins.foreach(_.onTaskFailed(reason))
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case CausedBy(cDE: CommitDeniedException) =>
          val reason = cDE.toTaskCommitDeniedReason
          setTaskFinishedAndClearInterruptStatus()
          plugins.foreach(_.onTaskFailed(reason))
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(reason))

        case t: Throwable if env.isStopped =>
          // 在 executor.stop 后记录预期的异常，不带堆栈跟踪
          // 参见：SPARK-19147
          logError(s"Exception in $taskName: ${t.getMessage}")

        case t: Throwable =>
          // 尝试通过通知驱动程序我们的失败来干净地退出。
          // 如果出现任何问题（或这是一个致命异常），我们将委托给默认的未捕获异常处理器，它将终止执行器。
          logError(s"Exception in $taskName", t)

          if (!ShutdownHookManager.inShutdown()) {
            // SPARK-20904：如果在关闭期间发生，不要向驱动程序报告失败。
            // 因为库可能会设置在关闭期间与正在运行的任务竞争的关闭钩子，可能会发生虚假失败，
            // 并可能导致驱动程序中的不当计数（例如如果关闭是由于抢占而不是应用程序问题发生的，任务失败将不会被忽略）。
            val (accums, accUpdates) = collectAccumulatorsAndResetStatusOnFailure(taskStartTimeNs)
            val metricPeaks = WrappedArray.make(metricsPoller.getTaskMetricPeaks(taskId))

            val (taskFailureReason, serializedTaskFailureReason) = {
              try {
                val ef = new ExceptionFailure(t, accUpdates).withAccums(accums)
                  .withMetricPeaks(metricPeaks.toSeq)
                (ef, ser.serialize(ef))
              } catch {
                case _: NotSerializableException =>
                  // t 不可序列化，所以只发送堆栈跟踪
                  val ef = new ExceptionFailure(t, accUpdates, false).withAccums(accums)
                    .withMetricPeaks(metricPeaks.toSeq)
                  (ef, ser.serialize(ef))
              }
            }
            setTaskFinishedAndClearInterruptStatus()
            plugins.foreach(_.onTaskFailed(taskFailureReason))
            execBackend.statusUpdate(taskId, TaskState.FAILED, serializedTaskFailureReason)
          } else {
            logInfo("Not reporting error to driver during JVM shutdown.")
          }
          // 不要强制退出，除非异常本质上是致命的，以避免不必要地停止其他任务。
          if (Executor.isFatalError(t, killOnFatalErrorDepth)) {
            uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), t)
          }
      } finally {
        runningTasks.remove(taskId)
        if (taskStarted) {
          // 这意味着任务已成功反序列化，其 stageId 和 stageAttemptId
          // 已知，并且调用了 metricsPoller.onTaskStart。
          metricsPoller.onTaskCompletion(taskId, task.stageId, task.stageAttemptId)
        }
      }
    }

    private def hasFetchFailure: Boolean = {
      task != null && task.context != null && task.context.fetchFailed.isDefined
    }
  }

  private def setMDCForTask(taskName: String, mdc: Seq[(String, String)]): Unit = {
    try {
      // 确保我们仅使用用户指定的 mdc 属性运行任务
      MDC.clear()
      mdc.foreach { case (key, value) => MDC.put(key, value) }
      // 避免用户覆盖 taskName
      MDC.put("mdc.taskName", taskName)
    } catch {
      case _: NoSuchFieldError => logInfo("MDC is not supported.")
    }
  }

  /**
   * 通过发送中断标志、可选地
   * 发送 Thread.interrupt() 并监控任务直到完成来监督任务的终止/取消。
   *
   * Spark 当前的任务取消/任务终止机制是"尽力而为"的，因为某些任务
   * 可能无法中断或可能不响应其"已终止"标志的设置。如果集群的任务槽中有相当大的
   * 一部分被标记为已终止但仍在运行的任务占用，
   * 则这可能导致新作业和任务缺乏
   * 这些僵尸任务正在使用的资源的情况。
   *
   * TaskReaper 在 SPARK-18761 中引入，作为监控和清理僵尸
   * 任务的机制。为了向后兼容/可移植性，此组件默认禁用，
   * 必须通过设置 `spark.task.reaper.enabled=true` 显式启用。
   *
   * 当任务被终止/取消时，会为该特定任务创建 TaskReaper。通常
   * 一个任务只有一个 TaskReaper，但如果使用不同的 `interrupt` 参数值
   * 两次调用 kill，则一个任务可能有多达两个 reapers。
   *
   * 一旦创建，TaskReaper 将运行直到其监督的任务完成运行。如果
   * TaskReaper 未配置为在超时后终止 JVM（即如果
   * `spark.task.reaper.killTimeout < 0`），则这意味着如果监督的任务永远不退出，
   * TaskReaper 可能会无限期运行。
   */
  private class TaskReaper(
      taskRunner: TaskRunner,
      val interruptThread: Boolean,
      val reason: String)
    extends Runnable {

    private[this] val taskId: Long = taskRunner.taskId

    private[this] val killPollingIntervalMs: Long = conf.get(TASK_REAPER_POLLING_INTERVAL)

    private[this] val killTimeoutNs: Long = {
      TimeUnit.MILLISECONDS.toNanos(conf.get(TASK_REAPER_KILL_TIMEOUT))
    }

    private[this] val takeThreadDump: Boolean = conf.get(TASK_REAPER_THREAD_DUMP)

    override def run(): Unit = {
      setMDCForTask(taskRunner.taskName, taskRunner.mdcProperties)
      val startTimeNs = System.nanoTime()
      def elapsedTimeNs = System.nanoTime() - startTimeNs
      def timeoutExceeded(): Boolean = killTimeoutNs > 0 && elapsedTimeNs > killTimeoutNs
      try {
        // 只尝试终止任务一次。如果 interruptThread = false，则第二次终止
        // 尝试将是无操作，如果 interruptThread = true，则多次中断可能不安全或
        // 有效：
        taskRunner.kill(interruptThread = interruptThread, reason = reason)
        // 监控已终止的任务直到它退出。这里的同步逻辑很复杂
        // 因为我们不想在可能获取线程转储时在 taskRunner 上同步，
        // 但我们还需要小心避免在检查任务是否
        // 已完成和 wait() 等待它完成之间的竞争。
        var finished: Boolean = false
        while (!finished && !timeoutExceeded()) {
          taskRunner.synchronized {
            // 我们需要在 TaskRunner 上同步，同时检查任务是否
            // 已完成，以避免在我们检查之后任务被标记为已完成
            // 但在我们调用 wait() 之前的竞争。
            if (taskRunner.isFinished) {
              finished = true
            } else {
              taskRunner.wait(killPollingIntervalMs)
            }
          }
          if (taskRunner.isFinished) {
            finished = true
          } else {
            val elapsedTimeMs = TimeUnit.NANOSECONDS.toMillis(elapsedTimeNs)
            logWarning(s"Killed task $taskId is still running after $elapsedTimeMs ms")
            if (takeThreadDump) {
              try {
                Utils.getThreadDumpForThread(taskRunner.getThreadId).foreach { thread =>
                  if (thread.threadName == taskRunner.threadName) {
                    logWarning(s"Thread dump from task $taskId:\n${thread.stackTrace}")
                  }
                }
              } catch {
                case NonFatal(e) =>
                  logWarning("Exception thrown while obtaining thread dump: ", e)
              }
            }
          }
        }

        if (!taskRunner.isFinished && timeoutExceeded()) {
          val killTimeoutMs = TimeUnit.NANOSECONDS.toMillis(killTimeoutNs)
          if (isLocal) {
            logError(s"Killed task $taskId could not be stopped within $killTimeoutMs ms; " +
              "not killing JVM because we are running in local mode.")
          } else {
            // 在非本地模式下，这里抛出的异常将冒泡到未捕获异常
            // 处理器并导致执行器 JVM 退出。
            throw new SparkException(
              s"Killing executor JVM because killed task $taskId could not be stopped within " +
                s"$killTimeoutMs ms.")
          }
        }
      } finally {
        // 清理 taskReaperForTask 映射中的条目。
        taskReaperForTask.synchronized {
          taskReaperForTask.get(taskId).foreach { taskReaperInMap =>
            if (taskReaperInMap eq this) {
              taskReaperForTask.remove(taskId)
            } else {
              // 这必定是一个 interruptThread == false 的 TaskReaper，其中对同一任务的后续
              // killTask() 调用具有 interruptThread == true 并覆盖了
              // 映射条目。
            }
          }
        }
      }
    }
  }

  /**
   * 创建用于任务的 ClassLoader，添加用户指定的任何 JAR 或解释器创建的任何类到搜索路径
   */
  private def createClassLoader(): MutableURLClassLoader = {
    // 使用用户类路径引导 jar 列表。
    val now = System.currentTimeMillis()

    // 获取用户上传的JAR包
    userClassPath.foreach { url =>
      currentJars(url.getPath().split("/").last) = now
    }

    // 当前JVM 默认的类加载器
    val currentLoader = Utils.getContextOrSparkClassLoader

    val urls = userClassPath.toArray ++ currentJars.keySet.map { uri =>
      new File(uri.split("/").last).toURI.toURL
    }

    logInfo(s"Starting executor with user classpath (userClassPathFirst = $userClassPathFirst): " + urls.mkString("'", ",", "'"))

    if (userClassPathFirst) {
      new ChildFirstURLClassLoader(urls, currentLoader)
    } else {
      new MutableURLClassLoader(urls, currentLoader)
    }
  }

  /**
   * 如果正在使用 REPL，添加另一个 ClassLoader，它将读取
   * 用户输入代码时 REPL 定义的新类
   */
  private def addReplClassLoaderIfNeeded(parent: ClassLoader): ClassLoader = {
    val classUri = conf.get("spark.repl.class.uri", null)
    if (classUri != null) {
      logInfo("Using REPL class URI: " + classUri)
      try {
        val _userClassPathFirst: java.lang.Boolean = userClassPathFirst
        val klass = Utils.classForName("org.apache.spark.repl.ExecutorClassLoader")
          .asInstanceOf[Class[_ <: ClassLoader]]
        val constructor = klass.getConstructor(classOf[SparkConf], classOf[SparkEnv],
          classOf[String], classOf[ClassLoader], classOf[Boolean])
        constructor.newInstance(conf, env, classUri, parent, _userClassPathFirst)
      } catch {
        case _: ClassNotFoundException =>
          logError("Could not find org.apache.spark.repl.ExecutorClassLoader on classpath!")
          System.exit(1)
          null
      }
    } else {
      parent
    }
  }

  /**
   * 如果我们从 SparkContext 接收到新的文件和 JAR 集合，则下载任何缺失的依赖。
   * 还将我们获取的任何新 JAR 添加到类加载器。
   */
  private def updateDependencies(
      newFiles: Map[String, Long],
      newJars: Map[String, Long],
      newArchives: Map[String, Long]): Unit = {
    lazy val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    synchronized {
      // 获取缺失的依赖
      for ((name, timestamp) <- newFiles if currentFiles.getOrElse(name, -1L) < timestamp) {
        logInfo(s"Fetching $name with timestamp $timestamp")
        // 使用 useCache 模式获取文件，为本地模式关闭缓存。
        Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
          hadoopConf, timestamp, useCache = !isLocal)
        currentFiles(name) = timestamp
      }
      for ((name, timestamp) <- newArchives if currentArchives.getOrElse(name, -1L) < timestamp) {
        logInfo(s"Fetching $name with timestamp $timestamp")
        val sourceURI = new URI(name)
        val uriToDownload = UriBuilder.fromUri(sourceURI).fragment(null).build()
        val source = Utils.fetchFile(uriToDownload.toString, Utils.createTempDir(), conf,
          hadoopConf, timestamp, useCache = !isLocal, shouldUntar = false)
        val dest = new File(
          SparkFiles.getRootDirectory(),
          if (sourceURI.getFragment != null) sourceURI.getFragment else source.getName)
        logInfo(
          s"Unpacking an archive $name from ${source.getAbsolutePath} to ${dest.getAbsolutePath}")
        Utils.deleteRecursively(dest)
        Utils.unpack(source, dest)
        currentArchives(name) = timestamp
      }
      for ((name, timestamp) <- newJars) {
        val localName = new URI(name).getPath.split("/").last
        val currentTimeStamp = currentJars.get(name)
          .orElse(currentJars.get(localName))
          .getOrElse(-1L)
        if (currentTimeStamp < timestamp) {
          logInfo(s"Fetching $name with timestamp $timestamp")
          // 使用 useCache 模式获取文件，为本地模式关闭缓存。
          Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
            hadoopConf, timestamp, useCache = !isLocal)
          currentJars(name) = timestamp
          // 将其添加到我们的类加载器
          val url = new File(SparkFiles.getRootDirectory(), localName).toURI.toURL
          if (!urlClassLoader.getURLs().contains(url)) {
            logInfo(s"Adding $url to class loader")
            urlClassLoader.addURL(url)
          }
        }
      }
    }
  }

  /** 向驱动程序报告活动任务的心跳和指标。 */
  private def reportHeartBeat(): Unit = {
    // 要发送回驱动程序的 (task id, accumUpdates) 列表
    val accumUpdates = new ArrayBuffer[(Long, Seq[AccumulatorV2[_, _]])]()
    val curGCTime = computeTotalGcTime()

    if (pollOnHeartbeat) {
      metricsPoller.poll()
    }

    val executorUpdates = metricsPoller.getExecutorUpdates()

    for (taskRunner <- runningTasks.values().asScala) {
      if (taskRunner.task != null) {
        taskRunner.task.metrics.mergeShuffleReadMetrics()
        taskRunner.task.metrics.setJvmGCTime(curGCTime - taskRunner.startGCTime)
        val accumulatorsToReport =
          if (HEARTBEAT_DROP_ZEROES) {
            taskRunner.task.metrics.accumulators().filterNot(_.isZero)
          } else {
            taskRunner.task.metrics.accumulators()
          }
        accumUpdates += ((taskRunner.taskId, accumulatorsToReport))
      }
    }

    val message = Heartbeat(executorId, accumUpdates.toArray, env.blockManager.blockManagerId,
      executorUpdates)
    try {
      val response = heartbeatReceiverRef.askSync[HeartbeatResponse](
        message, new RpcTimeout(HEARTBEAT_INTERVAL_MS.millis, EXECUTOR_HEARTBEAT_INTERVAL.key))
      if (!executorShutdown.get && response.reregisterBlockManager) {
        logInfo("Told to re-register on heartbeat")
        env.blockManager.reregister()
      }
      heartbeatFailures = 0
    } catch {
      case NonFatal(e) =>
        logWarning("Issue communicating with driver in heartbeater", e)
        heartbeatFailures += 1
        if (heartbeatFailures >= HEARTBEAT_MAX_FAILURES) {
          logError(s"Exit as unable to send heartbeats to driver " +
            s"more than $HEARTBEAT_MAX_FAILURES times")
          System.exit(ExecutorExitCode.HEARTBEAT_FAILURE)
        }
    }
  }
}

private[spark] object Executor {
  // 这保留供需要在任务完全反序列化之前读取任务属性的组件内部使用。
  // 如果可能，应使用 TaskContext.getLocalProperty 调用
  // 代替。
  val taskDeserializationProps: ThreadLocal[Properties] = new ThreadLocal[Properties]

  // 用于存储 executorSource，仅用于本地模式
  var executorSourceLocalModeOnly: ExecutorSource = null

  /**
   * 从任务抛出的 `Throwable` 是否为致命错误。我们将使用此来决定是否
   * 终止执行器。
   *
   * @param depthToCheck 我们应该搜索致命错误的异常链的最大深度。0
   *                     表示不检查任何致命错误（换句话说，返回 false），1 表示
   *                     仅检查异常但不检查原因，依此类推。这是为了避免
   *                     在异常链中遇到循环时出现 `StackOverflowError`。
   */
  @scala.annotation.tailrec
  def isFatalError(t: Throwable, depthToCheck: Int): Boolean = {
    if (depthToCheck <= 0) {
      false
    } else {
      t match {
        case _: SparkOutOfMemoryError => false
        case e if Utils.isFatalError(e) => true
        case e if e.getCause != null => isFatalError(e.getCause, depthToCheck - 1)
        case _ => false
      }
    }
  }
}
