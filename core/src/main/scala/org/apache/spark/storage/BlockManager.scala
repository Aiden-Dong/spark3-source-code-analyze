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

package org.apache.spark.storage

import java.io._
import java.lang.ref.{ReferenceQueue => JReferenceQueue, WeakReference}
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.Collections
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.{Failure, Random, Success, Try}
import scala.util.control.NonFatal

import com.codahale.metrics.{MetricRegistry, MetricSet}
import com.esotericsoftware.kryo.KryoException
import com.google.common.cache.CacheBuilder
import org.apache.commons.io.IOUtils

import org.apache.spark._
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.internal.config.Network
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.metrics.source.Source
import org.apache.spark.network._
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle._
import org.apache.spark.network.shuffle.checksum.{Cause, ShuffleChecksumHelper}
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo
import org.apache.spark.network.util.TransportConf
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.serializer.{SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, MigratableResolver, ShuffleManager, ShuffleWriteMetricsReporter}
import org.apache.spark.storage.BlockManagerMessages.{DecommissionBlockManager, ReplicateBlock}
import org.apache.spark.storage.memory._
import org.apache.spark.unsafe.Platform
import org.apache.spark.util._
import org.apache.spark.util.io.ChunkedByteBuffer

/* 用于返回获取的块和相关指标的类. */
private[spark] class BlockResult(
    val data: Iterator[Any],
    val readMethod: DataReadMethod.Value,
    val bytes: Long)

/**
 * 抽象化块的存储方式，并提供不同的方式来读取底层块数据。
 * 调用者在使用完块后应该调用 [[dispose()]]。
 * [[DiskBlockData]], [[ByteBufferBlockData]], [[EncryptedBlockData]]
 */
private[spark] trait BlockData {
  def toInputStream(): InputStream

  /**
   * 返回块数据的 Netty 友好包装器。.
   * 请参阅 [[ManagedBuffer.convertToNetty()]] 了解更多详情.
   */
  def toNetty(): Object
  def toChunkedByteBuffer(allocator: Int => ByteBuffer): ChunkedByteBuffer
  def toByteBuffer(): ByteBuffer
  def size: Long
  def dispose(): Unit

}

private[spark] class ByteBufferBlockData(
    val buffer: ChunkedByteBuffer,
    val shouldDispose: Boolean) extends BlockData {

  override def toInputStream(): InputStream = buffer.toInputStream(dispose = false)

  override def toNetty(): Object = buffer.toNetty

  override def toChunkedByteBuffer(allocator: Int => ByteBuffer): ChunkedByteBuffer = {
    buffer.copy(allocator)
  }

  override def toByteBuffer(): ByteBuffer = buffer.toByteBuffer

  override def size: Long = buffer.size

  override def dispose(): Unit = {
    if (shouldDispose) {
      buffer.dispose()
    }
  }

}

private[spark] class HostLocalDirManager(
    futureExecutionContext: ExecutionContext,
    cacheSize: Int,
    blockStoreClient: BlockStoreClient) extends Logging {

  private val executorIdToLocalDirsCache =
    CacheBuilder
      .newBuilder()
      .maximumSize(cacheSize)
      .build[String, Array[String]]()

  private[spark] def getCachedHostLocalDirs: Map[String, Array[String]] =
    executorIdToLocalDirsCache.synchronized {
      executorIdToLocalDirsCache.asMap().asScala.toMap
    }

  private[spark] def getCachedHostLocalDirsFor(executorId: String): Option[Array[String]] =
    executorIdToLocalDirsCache.synchronized {
      Option(executorIdToLocalDirsCache.getIfPresent(executorId))
    }

  private[spark] def getHostLocalDirs(
      host: String,
      port: Int,
      executorIds: Array[String])(
      callback: Try[Map[String, Array[String]]] => Unit): Unit = {
    val hostLocalDirsCompletable = new CompletableFuture[java.util.Map[String, Array[String]]]
    blockStoreClient.getHostLocalDirs(
      host,
      port,
      executorIds,
      hostLocalDirsCompletable)
    hostLocalDirsCompletable.whenComplete { (hostLocalDirs, throwable) =>
      if (hostLocalDirs != null) {
        callback(Success(hostLocalDirs.asScala.toMap))
        executorIdToLocalDirsCache.synchronized {
          executorIdToLocalDirsCache.putAll(hostLocalDirs)
        }
      } else {
        callback(Failure(throwable))
      }
    }
  }
}

/**
 * * 运行在每个节点（驱动程序和执行器）上的管理器，提供在本地和远程将块存储到各种存储（内存、磁盘和堆外）
 * * 以及从中检索块的接口。
 * *
 * * 注意，必须在 BlockManager 可用之前调用 [[initialize()]]。
 */
private[spark] class BlockManager(
    val executorId: String,
    rpcEnv: RpcEnv,
    val master: BlockManagerMaster,
    val serializerManager: SerializerManager,             // 序列化器
    val conf: SparkConf,
    memoryManager: MemoryManager,
    mapOutputTracker: MapOutputTracker,
    shuffleManager: ShuffleManager,
    val blockTransferService: BlockTransferService,        // 块传输服务 - 处理节点间数据传输
    securityManager: SecurityManager,
    externalBlockStoreClient: Option[ExternalBlockStoreClient])
  extends BlockDataManager with BlockEvictionHandler with Logging {

  // 与 `conf.get(config.SHUFFLE_SERVICE_ENABLED)` 相同
  private[spark] val externalShuffleServiceEnabled: Boolean = externalBlockStoreClient.isDefined
  private val isDriver = executorId == SparkContext.DRIVER_IDENTIFIER

  // 块管理器ID - 唯一标识
  var blockManagerId: BlockManagerId = _

  ////// ======================================== 存储组件 =========================//
  private[spark] val subDirsPerLocalDir = conf.get(config.DISKSTORE_SUB_DIRECTORIES)

  //  磁盘块管理器 - 管理磁盘文件和目录
  val diskBlockManager = {
    // 只有在外部服务不为我们的 shuffle 文件提供服务时才执行清理。
    val deleteFilesOnStop = !externalShuffleServiceEnabled || isDriver
    new DiskBlockManager(conf, deleteFilesOnStop = deleteFilesOnStop, isDriver = isDriver)
  }

  // 块信息管理器 - 管理块的元数据和锁
  private[storage] val blockInfoManager = new BlockInfoManager

  //////////////////// 块实际存储的地方 ////////////////////////////////////////////////

  //  内存存储 - 管理内存中的数据块
  private[spark] val memoryStore = new MemoryStore(conf, blockInfoManager, serializerManager, memoryManager, this)
  // 磁盘存储 - 管理磁盘上的数据块
  private[spark] val diskStore = new DiskStore(conf, diskBlockManager, securityManager)

  private val remoteReadNioBufferConversion = conf.get(Network.NETWORK_REMOTE_READ_NIO_BUFFER_CONVERSION)

  private val futureExecutionContext = ExecutionContext.fromExecutorService(ThreadUtils.newDaemonCachedThreadPool("block-manager-future", 128))

  // =========================== 内存管理 ===========================
  memoryManager.setMemoryStore(memoryStore)
  private val maxOnHeapMemory = memoryManager.maxOnHeapStorageMemory
  private val maxOffHeapMemory = memoryManager.maxOffHeapStorageMemory


  ////// ======================================== 网络和通信 =========================//
  private[spark] val externalShuffleServicePort = StorageUtils.externalShuffleServicePort(conf)
  private[spark] var shuffleServerId: BlockManagerId = _
  // 块传输工具
  private[spark] val blockStoreClient = externalBlockStoreClient.getOrElse(blockTransferService)

  // Max number of failures before this block manager refreshes the block locations from the driver
  private val maxFailuresBeforeLocationRefresh = conf.get(config.BLOCK_FAILURES_BEFORE_LOCATION_REFRESH)

  private val storageEndpoint = rpcEnv.setupEndpoint(
    "BlockManagerEndpoint" + BlockManager.ID_GENERATOR.next,
    new BlockManagerStorageEndpoint(rpcEnv, this, mapOutputTracker))

  // 正在异步执行的待处理重新注册操作，如果没有待处理操作则为 null。
  // 访问应该在 asyncReregisterLock 上同步。
  private var asyncReregisterTask: Future[Unit] = null
  private val asyncReregisterLock = new Object

  // 与块复制所需的对等块管理器相关的字段
  @volatile private var cachedPeers: Seq[BlockManagerId] = _
  private val peerFetchLock = new Object
  private var lastPeerFetchTimeNs = 0L

  private var blockReplicationPolicy: BlockReplicationPolicy = _   // // 复制策略

  // 可见用于测试
  // 这是 volatile 的，因为如果定义了它，我们就不应该接受远程块。
  @volatile private[spark] var decommissioner: Option[BlockManagerDecommissioner] = None

  // 用于跟踪超过指定内存阈值的远程块的所有文件的 DownloadFileManager。
  // 文件将基于弱引用自动删除。
  // 暴露用于测试
  private[storage] val remoteBlockTempFileManager =
    new BlockManager.RemoteBlockDownloadFileManager(
      this,
      securityManager.getIOEncryptionKey())
  private val maxRemoteBlockToMem = conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM)

  var hostLocalDirManager: Option[HostLocalDirManager] = None

  @inline final private def isDecommissioning() = {
    decommissioner.isDefined
  }

  @inline final private def checkShouldStore(blockId: BlockId) = {
    // 不要拒绝广播块，因为它们可能在任务执行期间存储，
    // 不需要迁移。
    if (isDecommissioning() && !blockId.isBroadcast) {
      throw SparkCoreErrors.cannotSaveBlockOnDecommissionedExecutorError(blockId)
    }
  }

  // 这是一个惰性 val，所以即使没有用于 shuffle 的 MigratableResolver，
  // 也可以迁移 RDD。在 BlockManagerDecommissioner 和块存储中使用。
  private[storage] lazy val migratableResolver: MigratableResolver = {
    shuffleManager.shuffleBlockResolver.asInstanceOf[MigratableResolver]
  }

  override def getLocalDiskDirs: Array[String] = diskBlockManager.localDirsString

  /**
   * 通过验证 shuffle 校验和来诊断 shuffle 数据损坏的可能原因
   *
   * @param blockId 损坏的 shuffle 块的 blockId
   * @param checksumByReader 损坏块的校验和值
   * @param algorithm 计算校验和值时使用的校验和算法
   */
  override def diagnoseShuffleBlockCorruption(
      blockId: BlockId,
      checksumByReader: Long,
      algorithm: String): Cause = {
    assert(blockId.isInstanceOf[ShuffleBlockId],
      s"Corruption diagnosis only supports shuffle block yet, but got $blockId")
    val shuffleBlock = blockId.asInstanceOf[ShuffleBlockId]
    val resolver = shuffleManager.shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver]
    val checksumFile =
      resolver.getChecksumFile(shuffleBlock.shuffleId, shuffleBlock.mapId, algorithm)
    val reduceId = shuffleBlock.reduceId
    ShuffleChecksumHelper.diagnoseCorruption(
      algorithm, checksumFile, reduceId, resolver.getBlockData(shuffleBlock), checksumByReader)
  }

  /**
   * 从字节存储块的抽象，无论它们是从内存还是磁盘开始。
   *
   * @param blockSize 块的解密大小
   */
  private[spark] abstract class BlockStoreUpdater[T](
      blockSize: Long,           // Block 大小
      blockId: BlockId,          // 当前Block唯一标识
      level: StorageLevel,       // 存储级别
      classTag: ClassTag[T],     // 类对象
      tellMaster: Boolean,
      keepReadLock: Boolean) {

    /**
     * 用于存放序列化数据
     * 将块内容读入内存。如果块存储的更新基于临时文件，
     * 这可能导致将整个文件加载到 ChunkedByteBuffer 中。
     */
    protected def readToByteBuffer(): ChunkedByteBuffer

    protected def blockData(): BlockData

    // 数据存储到磁盘上
    protected def saveToDiskStore(): Unit

    private def saveDeserializedValuesToMemoryStore(inputStream: InputStream): Boolean = {
      try {
        val values = serializerManager.dataDeserializeStream(blockId, inputStream)(classTag)
        memoryStore.putIteratorAsValues(blockId, values, level.memoryMode, classTag) match {
          case Right(_) => true
          case Left(iter) =>
            // 如果将反序列化的值放入内存失败，我们将直接将字节
            // 放到磁盘上，所以我们不需要这个迭代器，可以关闭它以更早地释放资源。
            iter.close()
            false
        }
      } catch {
        case ex: KryoException if ex.getCause.isInstanceOf[IOException] =>
          // 我们需要详细的日志消息来轻松捕获环境问题。
          // 更多详情：https://issues.apache.org/jira/browse/SPARK-37710
          processKryoException(ex, blockId)
          throw ex
      } finally {
        IOUtils.closeQuietly(inputStream)
      }
    }

    private def saveSerializedValuesToMemoryStore(bytes: ChunkedByteBuffer): Boolean = {
      val memoryMode = level.memoryMode
      memoryStore.putBytes(blockId, blockSize, memoryMode, () => {
        if (memoryMode == MemoryMode.OFF_HEAP && bytes.chunks.exists(!_.isDirect)) {
          bytes.copy(Platform.allocateDirectBuffer)
        } else {
          bytes
        }
      })
    }

    /**
     * 根据给定的级别将给定的数据放入其中一个块存储中，必要时复制值。 如果块已经存在，此方法不会覆盖它。
     *
     * 如果 keepReadLock 为 true，此方法在返回时将持有读锁（即使块已经存在）。
     * 如果为 false，此方法在返回时将不持有任何锁。
     *
     * @return 如果块已经存在或放置成功则返回 true，否则返回 false。
     */
     def save(): Boolean = {
      doPut(blockId, level, classTag, tellMaster, keepReadLock) { info =>
        val startTimeNs = System.nanoTime()

        val replicationFuture = if (level.replication > 1) {
          // 由于我们存储字节，在本地存储之前启动复制;这更快，因为数据已经序列化并准备发送
          Future {
            // 这是一个阻塞操作，应该在 futureExecutionContext 中运行，
            // 它是一个缓存线程池
            replicate(blockId, blockData(), level, classTag)
          }(futureExecutionContext)
        } else {
          null
        }

        // 数据存储
        if (level.useMemory) {
          // 首先放入内存，即使它也设置了 useDisk 为 true；
          // 如果内存存储无法容纳，我们稍后会将其放到磁盘上。
          val putSucceeded = if (level.deserialized) { // java 对象
            saveDeserializedValuesToMemoryStore(blockData().toInputStream())
          } else {
            saveSerializedValuesToMemoryStore(readToByteBuffer())
          }
          if (!putSucceeded && level.useDisk) {
            logWarning(s"将块 $blockId 持久化到磁盘。")
            saveToDiskStore()
          }
        } else if (level.useDisk) {
          saveToDiskStore()
        }

        // 获取Block状态
        val putBlockStatus = getCurrentBlockStatus(blockId, info)
        val blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid
        if (blockWasSuccessfullyStored) {
          // 现在块在内存或磁盘存储中，告诉主节点。
          info.size = blockSize
          if (tellMaster && info.tellMaster) {
            reportBlockStatus(blockId, putBlockStatus)
          }
          addUpdatedBlockStatusToTaskMetrics(blockId, putBlockStatus)
        }
        logDebug(s"Put block ${blockId} locally took ${Utils.getUsedTimeNs(startTimeNs)}")
        if (level.replication > 1) {
          // Wait for asynchronous replication to finish
          try {
            ThreadUtils.awaitReady(replicationFuture, Duration.Inf)
          } catch {
            case NonFatal(t) => throw SparkCoreErrors.waitingForReplicationToFinishError(t)
          }
        }
        if (blockWasSuccessfullyStored) {
          None
        } else {
          Some(blockSize)
        }
      }.isEmpty
    }
  }


  /**
   * 用于基于已在本地临时文件中的字节存储块的辅助器。
   */
  private[spark] case class TempFileBasedBlockStoreUpdater[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[T],
      tmpFile: File,
      blockSize: Long,
      tellMaster: Boolean = true,
      keepReadLock: Boolean = false)
    extends BlockStoreUpdater[T](blockSize, blockId, level, classTag, tellMaster, keepReadLock) {

    override def readToByteBuffer(): ChunkedByteBuffer = {
      val allocator = level.memoryMode match {
        case MemoryMode.ON_HEAP => ByteBuffer.allocate _
        case MemoryMode.OFF_HEAP => Platform.allocateDirectBuffer _
      }
      blockData().toChunkedByteBuffer(allocator)
    }

    override def blockData(): BlockData = diskStore.getBytes(tmpFile, blockSize)

    override def saveToDiskStore(): Unit = diskStore.moveFileToBlock(tmpFile, blockSize, blockId)

    override def save(): Boolean = {
      val res = super.save()
      tmpFile.delete()
      res
    }

  }

  /**
   * 使用给定的 appId 初始化 BlockManager。
   * 这不在构造函数中执行， 因为在 BlockManager 实例化时可能不知道 appId（特别是对于驱动程序， 它只有在向 TaskScheduler 注册后才知道）。
   *
   * 此方法初始化 BlockTransferService 和 BlockStoreClient，向BlockManagerMaster 注册，启动 BlockManagerWorker 端点，
   * 并在配置时向本地 shuffle 服务注册。
   */
  def initialize(appId: String): Unit = {
    blockTransferService.init(this)
    externalBlockStoreClient.foreach { blockStoreClient =>
      blockStoreClient.init(appId)
    }

    blockReplicationPolicy = {
      val priorityClass = conf.get(config.STORAGE_REPLICATION_POLICY)
      val clazz = Utils.classForName(priorityClass)
      val ret = clazz.getConstructor().newInstance().asInstanceOf[BlockReplicationPolicy]
      logInfo(s"Using $priorityClass for block replication policy")
      ret
    }

    // 向本地 shuffle 服务注册执行器的配置（如果应该存在的话）。
    // 向 ESS 的注册应该在向 BlockManagerMaster 注册块管理器之前进行。
    // 在基于推送的 shuffle 中，注册的 BM 由驱动程序选择作为合并器。
    // 但是，为了让此主机上的 ESS 能够成功合并块，它需要合并目录元数据，
    // 这些元数据由本地执行器在向 ESS 注册期间提供。
    // 因此，此注册应该在 BlockManager 注册之前进行。参见 SPARK-39647。
    if (externalShuffleServiceEnabled) {
      logInfo(s"external shuffle service port = $externalShuffleServicePort")
      shuffleServerId = BlockManagerId(executorId, blockTransferService.hostName,
        externalShuffleServicePort)
      if (!isDriver) {
        registerWithExternalShuffleServer()
      }
    }

    val id = BlockManagerId(executorId, blockTransferService.hostName, blockTransferService.port, None)

    // idFromMaster 只是有额外的拓扑信息。否则，它具有与 idWithoutTopologyInfo 相同的
    // executor id/host/port，这些不应该被更改。
    val idFromMaster = master.registerBlockManager(
      id,
      diskBlockManager.localDirsString,
      maxOnHeapMemory,
      maxOffHeapMemory,
      storageEndpoint)

    blockManagerId = if (idFromMaster != null) idFromMaster else id

    if (!externalShuffleServiceEnabled) {
      shuffleServerId = blockManagerId
    }

    hostLocalDirManager = {
      if ((conf.get(config.SHUFFLE_HOST_LOCAL_DISK_READING_ENABLED) &&
          !conf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)) ||
          Utils.isPushBasedShuffleEnabled(conf, isDriver)) {
        Some(new HostLocalDirManager(
          futureExecutionContext,
          conf.get(config.STORAGE_LOCAL_DISK_BY_EXECUTORS_CACHE_SIZE),
          blockStoreClient))
      } else {
        None
      }
    }

    logInfo(s"Initialized BlockManager: $blockManagerId")
  }

  def shuffleMetricsSource: Source = {
    import BlockManager._

    if (externalShuffleServiceEnabled) {
      new ShuffleMetricsSource("ExternalShuffle", blockStoreClient.shuffleMetrics())
    } else {
      new ShuffleMetricsSource("NettyBlockTransfer", blockStoreClient.shuffleMetrics())
    }
  }

  private def registerWithExternalShuffleServer(): Unit = {
    logInfo("向本地外部 shuffle 服务注册执行器。")
    val shuffleManagerMeta =
      if (Utils.isPushBasedShuffleEnabled(conf, isDriver = isDriver, checkSerializer = false)) {
        s"${shuffleManager.getClass.getName}:" +
          s"${diskBlockManager.getMergeDirectoryAndAttemptIDJsonString()}}}"
      } else {
        shuffleManager.getClass.getName
      }
    val shuffleConfig = new ExecutorShuffleInfo(
      diskBlockManager.localDirsString,
      diskBlockManager.subDirsPerLocalDir,
      shuffleManagerMeta)

    val MAX_ATTEMPTS = conf.get(config.SHUFFLE_REGISTRATION_MAX_ATTEMPTS)
    val SLEEP_TIME_SECS = 5

    for (i <- 1 to MAX_ATTEMPTS) {
      try {
        // 同步的，如果无法连接将抛出异常。
        blockStoreClient.asInstanceOf[ExternalBlockStoreClient].registerWithShuffleServer(
          shuffleServerId.host, shuffleServerId.port, shuffleServerId.executorId, shuffleConfig)
        return
      } catch {
        case e: Exception if i < MAX_ATTEMPTS =>
          logError(s"Failed to connect to external shuffle server, will retry ${MAX_ATTEMPTS - i}"
            + s" more times after waiting $SLEEP_TIME_SECS seconds...", e)
          Thread.sleep(SLEEP_TIME_SECS * 1000L)
        case NonFatal(e) => throw SparkCoreErrors.unableToRegisterWithExternalShuffleServerError(e)
      }
    }
  }

  /**
   * 再次向 BlockManager 报告所有块。如果我们被 BlockManager 丢弃并回来，
   * 或者如果我们在执行器崩溃后能够恢复磁盘上的块，这可能是必要的。
   *
   * 如果主节点返回 false（表示存储端点需要重新注册），此函数故意静默失败。
   * 错误条件将在下一次心跳尝试或新块注册时再次检测到，
   * 然后将再次尝试重新注册所有块。
   */
  private def reportAllBlocks(): Unit = {
    logInfo(s"Reporting ${blockInfoManager.size} blocks to the master.")
    for ((blockId, info) <- blockInfoManager.entries) {
      val status = getCurrentBlockStatus(blockId, info)
      if (info.tellMaster && !tryToReportBlockStatus(blockId, status)) {
        logError(s"Failed to report $blockId to master; giving up.")
        return
      }
    }
  }

  /**
   * 重新向主节点注册并向其报告所有块。如果我们向块管理器的心跳表明
   * 我们未注册，心跳线程将调用此方法。
   *
   * 注意，此方法必须在不持有任何 BlockInfo 锁的情况下调用。
   */
  def reregister(): Unit = {
    // TODO: 我们可能需要限制重新注册的速率。
    logInfo(s"BlockManager $blockManagerId re-registering with master")
    master.registerBlockManager(blockManagerId, diskBlockManager.localDirsString, maxOnHeapMemory,
      maxOffHeapMemory, storageEndpoint)
    reportAllBlocks()
  }

  /**
   * 很快重新向主节点注册。
   */
  private def asyncReregister(): Unit = {
    asyncReregisterLock.synchronized {
      if (asyncReregisterTask == null) {
        asyncReregisterTask = Future[Unit] {
          // 这是一个阻塞操作，应该在 futureExecutionContext 中运行，
          // 它是一个缓存线程池
          reregister()
          asyncReregisterLock.synchronized {
            asyncReregisterTask = null
          }
        }(futureExecutionContext)
      }
    }
  }

  /**
   * 用于测试。等待任何待处理的异步重新注册；否则，什么都不做。
   */
  def waitForAsyncReregister(): Unit = {
    val task = asyncReregisterTask
    if (task != null) {
      try {
        ThreadUtils.awaitReady(task, Duration.Inf)
      } catch {
        case NonFatal(t) =>
          throw SparkCoreErrors.waitingForAsyncReregistrationError(t)
      }
    }
  }

  override def getHostLocalShuffleData(
      blockId: BlockId,
      dirs: Array[String]): ManagedBuffer = {
    shuffleManager.shuffleBlockResolver.getBlockData(blockId, Some(dirs))
  }

  /**
   * 获取本地块数据的接口。如果无法找到块或无法成功读取，则抛出异常。
   */
  override def getLocalBlockData(blockId: BlockId): ManagedBuffer = {
    if (blockId.isShuffle) {
      logDebug(s"Getting local shuffle block ${blockId}")
      try {
        shuffleManager.shuffleBlockResolver.getBlockData(blockId)
      } catch {
        case e: IOException =>
          if (conf.get(config.STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).isDefined) {
            FallbackStorage.read(conf, blockId)
          } else {
            throw e
          }
      }
    } else {
      getLocalBytes(blockId) match {
        case Some(blockData) =>
          new BlockManagerManagedBuffer(blockInfoManager, blockId, blockData, true)
        case None =>
          // 如果此块管理器收到对它没有的块的请求，那么
          // 主节点很可能对此块有过时的块状态。因此，我们发送
          // RPC，以便将此块标记为从此块管理器不可用。
          reportBlockStatus(blockId, BlockStatus.empty)
          throw SparkCoreErrors.blockNotFoundError(blockId)
      }
    }
  }


  override def putBlockDataAsStream(
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[_]): StreamCallbackWithID = {

    checkShouldStore(blockId)

    if (blockId.isShuffle) {
      logDebug(s"放置 shuffle 块 ${blockId}")
      try {
        return migratableResolver.putShuffleBlockAsStream(blockId, serializerManager)
      } catch {
        case e: ClassCastException =>
          throw SparkCoreErrors.unexpectedShuffleBlockWithUnsupportedResolverError(shuffleManager,
            blockId)
      }
    }
    logDebug(s"放置常规块 ${blockId}")
    // All other blocks
    val (_, tmpFile) = diskBlockManager.createTempLocalBlock()
    val channel = new CountingWritableChannel(
      Channels.newChannel(serializerManager.wrapForEncryption(new FileOutputStream(tmpFile))))
    logTrace(s"将块 $blockId 流式传输到临时文件 $tmpFile")
    new StreamCallbackWithID {

      override def getID: String = blockId.name

      override def onData(streamId: String, buf: ByteBuffer): Unit = {
        while (buf.hasRemaining) {
          channel.write(buf)
        }
      }

      override def onComplete(streamId: String): Unit = {
        logTrace(s"完成接收块 $blockId，现在放入本地 blockManager")
        // 注意这一切都发生在 netty 线程中，一旦它读取到流的末尾。
        channel.close()
        val blockSize = channel.getCount
        val blockStored = TempFileBasedBlockStoreUpdater(
          blockId, level, classTag, tmpFile, blockSize).save()
        if (!blockStored) {
          throw SparkCoreErrors.failToStoreBlockOnBlockManagerError(blockManagerId, blockId)
        }
      }

      override def onFailure(streamId: String, cause: Throwable): Unit = {
        // 框架自己处理连接，我们只需要进行本地清理
        channel.close()
        tmpFile.delete()
      }
    }
  }

  /**
   * 获取给定块 ID 的本地合并 shuffle 块数据作为多个块。
   * 合并的 shuffle 文件根据索引文件分为多个块。
   * 我们不是将整个文件作为单个块读取，而是将其拆分为较小的块，
   * 这在执行某些操作时会更节省内存。
   */
  def getLocalMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Array[String]): Seq[ManagedBuffer] = {
    shuffleManager.shuffleBlockResolver.getMergedBlockData(blockId, Some(dirs))
  }

  /**
   * 获取给定块 ID 的本地合并 shuffle 块元数据。
   */
  def getLocalMergedBlockMeta(
      blockId: ShuffleMergedBlockId,
      dirs: Array[String]): MergedBlockMeta = {
    shuffleManager.shuffleBlockResolver.getMergedBlockMeta(blockId, Some(dirs))
  }


  /**
   * 获取由给定 ID 标识的块的 BlockStatus（如果存在）。
   * 注意：这主要用于测试。
   */
  def getStatus(blockId: BlockId): Option[BlockStatus] = {
    blockInfoManager.get(blockId).map { info =>
      val memSize = if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
      val diskSize = if (diskStore.contains(blockId)) diskStore.getSize(blockId) else 0L
      BlockStatus(info.level, memSize = memSize, diskSize = diskSize)
    }
  }

  /**
   * 获取与给定过滤器匹配的现有块的 ID。注意这将
   * 查询存储在磁盘块管理器中的块（块管理器可能不知道）。
   */
  def getMatchingBlockIds(filter: BlockId => Boolean): Seq[BlockId] = {
    // 这里的 `toArray` 是必要的，以便强制列表被物化，
    // 这样我们在响应客户端请求时就不会尝试序列化惰性迭代器。
    (blockInfoManager.entries.map(_._1) ++ diskBlockManager.getAllBlocks())
      .filter(filter)
      .toArray
      .toSeq
  }

  /**
   * 告诉主节点块的当前存储状态。这将发送反映当前状态的块更新消息，
   * *而不是* 其块信息中所需的存储级别。
   * 例如，设置了 MEMORY_AND_DISK 的块可能已经掉落到只在磁盘上。
   *
   * droppedMemorySize 存在是为了说明块从内存掉落到磁盘时的情况
   * （所以它仍然有效）。这确保主节点中的更新将补偿存储端点上内存的增加。
   */
  private[spark] def reportBlockStatus(
      blockId: BlockId,    // 每个主机上的数据分片对应一个BlockId
      status: BlockStatus,
      droppedMemorySize: Long = 0L): Unit = {
    val needReregister = !tryToReportBlockStatus(blockId, status, droppedMemorySize)
    if (needReregister) {
      logInfo(s"Got told to re-register updating block $blockId")
      // 重新注册将免费报告我们的新块。
      asyncReregister()
    }
    logDebug(s"告诉主节点关于块 $blockId")
  }

  /**
   * 实际发送 UpdateBlockInfo 消息。返回主节点的响应，
   * 如果块被成功记录则为 true，如果存储端点需要重新注册则为 false。
   */
  private def tryToReportBlockStatus(
      blockId: BlockId,
      status: BlockStatus,
      droppedMemorySize: Long = 0L): Boolean = {
    val storageLevel = status.storageLevel
    val inMemSize = Math.max(status.memSize, droppedMemorySize)
    val onDiskSize = status.diskSize
    master.updateBlockInfo(blockManagerId, blockId, storageLevel, inMemSize, onDiskSize)
  }


  /**
   * 返回具有给定 ID 的块的更新存储状态。更具体地说，如果
   * 块从内存中掉落并可能添加到磁盘，返回新的存储级别
   * 以及更新的内存和磁盘大小。
   */
  private def getCurrentBlockStatus(blockId: BlockId, info: BlockInfo): BlockStatus = {
    info.synchronized {
      info.level match {
        case null =>
          BlockStatus.empty
        case level =>
          val inMem = level.useMemory && memoryStore.contains(blockId)
          val onDisk = level.useDisk && diskStore.contains(blockId)
          val deserialized = if (inMem) level.deserialized else false
          val replication = if (inMem  || onDisk) level.replication else 1
          val storageLevel = StorageLevel(
            useDisk = onDisk,
            useMemory = inMem,
            useOffHeap = level.useOffHeap,
            deserialized = deserialized,
            replication = replication)
          val memSize = if (inMem) memoryStore.getSize(blockId) else 0L
          val diskSize = if (onDisk) diskStore.getSize(blockId) else 0L
          BlockStatus(storageLevel, memSize, diskSize)
      }
    }
  }

  /**
   * 获取块数组的位置。
   */
  private def getLocationBlockIds(blockIds: Array[BlockId]): Array[Seq[BlockManagerId]] = {
    val startTimeNs = System.nanoTime()
    val locations = master.getLocations(blockIds).toArray
    logDebug(s"Got multiple block location in ${Utils.getUsedTimeNs(startTimeNs)}")
    locations
  }

  /**
   * 响应失败的本地读取运行的清理代码。
   * 必须在持有块的读锁时调用。
   */
  private def handleLocalReadFailure(blockId: BlockId): Nothing = {
    releaseLock(blockId)
    // 删除丢失的块，以便将其不可用性报告给驱动程序
    removeBlock(blockId)
    throw SparkCoreErrors.readLockedBlockNotFoundError(blockId)
  }

  private def processKryoException(ex: KryoException, blockId: BlockId): Unit = {
    var message =
      "%s. %s - blockId: %s".format(ex.getMessage, blockManagerId.toString, blockId)
    val file = diskBlockManager.getFile(blockId)
    if (file.exists()) {
      message = "%s - blockDiskPath: %s".format(message, file.getAbsolutePath)
    }
    logInfo(message)
  }

  /**
   * 从块管理器（本地或远程）获取块。
   *
   * 如果块存储在本地，这会获取块的读锁，如果块从远程块管理器获取，
   * 则不获取任何锁。一旦结果的 `data` 迭代器完全消费，读锁将自动释放。
   */
  def get[T: ClassTag](blockId: BlockId): Option[BlockResult] = {
    val local = getLocalValues(blockId)
    if (local.isDefined) {
      logInfo(s"Found block $blockId locally")
      return local
    }
    val remote = getRemoteValues[T](blockId)
    if (remote.isDefined) {
      logInfo(s"Found block $blockId remotely")
      return remote
    }
    None
  }

  //// ==================================================================================  /////
  ////                           从远程获取物理快                                            ////
  //// ==================================================================================  /////
  /**
   * 从远程块管理器获取块。
   *
   * 这不会在此 JVM 中获取此块的锁。
   */
  private[spark] def getRemoteValues[T: ClassTag](blockId: BlockId): Option[BlockResult] = {
    val ct = implicitly[ClassTag[T]]
    getRemoteBlock(blockId, (data: ManagedBuffer) => {
      val values =
        serializerManager.dataDeserializeStream(blockId, data.createInputStream())(ct)
      new BlockResult(values, DataReadMethod.Network, data.size)
    })
  }

  /**
   * 获取远程块并将其转换为提供的数据类型。
   *
   * 如果块持久化到磁盘并存储在运行在同一主机上的执行器，那么首先尝试直接使用其他执行器的本地目录访问它。
   * 如果文件成功识别，则尝试通过提供的转换函数进行转换，该函数预期打开文件。
   * 如果在此转换过程中有任何异常，则块访问回退到通过网络从远程执行器获取。
   *
   * @param blockId 标识要获取的块
   * @param bufferTransformer 此转换器预期在块由文件支持时打开文件，通过这种方式保证可以加载整个内容
   * @tparam T 结果类型
   */
  private[spark] def getRemoteBlock[T](blockId: BlockId,
                                       bufferTransformer: ManagedBuffer => T): Option[T] = {
    logDebug(s"Getting remote block $blockId")
    require(blockId != null, "BlockId is null")

    // 向 BlockManagerMaster 去获取
    val locationsAndStatusOption = master.getLocationsAndStatus(blockId, blockManagerId.host)

    if (locationsAndStatusOption.isEmpty) {
      logDebug(s"块管理器主节点不知道块 $blockId")
      None
    } else {
      val locationsAndStatus = locationsAndStatusOption.get
      val blockSize = locationsAndStatus.status.diskSize.max(locationsAndStatus.status.memSize)

      // 如果本地磁盘存在当前的数据块
      locationsAndStatus.localDirs.flatMap { localDirs =>
        val blockDataOption =
          readDiskBlockFromSameHostExecutor(blockId, localDirs, locationsAndStatus.status.diskSize)
        val res = blockDataOption.flatMap { blockData =>
          try {
            Some(bufferTransformer(blockData))
          } catch {
            case NonFatal(e) =>
              logDebug("来自同一主机执行器的块无法打开：", e)
              None
          }
        }
        logInfo(s"Read $blockId from the disk of a same host executor is " +
          (if (res.isDefined) "successful." else "failed."))
        res
      }.orElse {
        fetchRemoteManagedBuffer(blockId, blockSize, locationsAndStatus).map(bufferTransformer)
      }
    }
  }

  /**
   * 从运行在同一主机上的另一个执行器的本地目录读取块。
   */
  private[spark] def readDiskBlockFromSameHostExecutor(blockId: BlockId,
                     localDirs: Array[String], blockSize: Long): Option[ManagedBuffer] = {

    val file = new File(ExecutorDiskUtils.getFilePath(localDirs, subDirsPerLocalDir, blockId.name))
    if (file.exists()) {
      val managedBuffer = securityManager.getIOEncryptionKey() match {
        case Some(key) =>
          // 加密块不能内存映射；返回一个特殊对象，该对象进行解密
          // 并提供用于读取数据的 InputStream / FileRegion 实现。
          new EncryptedManagedBuffer(new EncryptedBlockData(file, blockSize, conf, key))

        case _ =>
          val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")
          new FileSegmentManagedBuffer(transportConf, file, 0, file.length)
      }
      Some(managedBuffer)
    } else {
      None
    }
  }

  /**
   * 从远程块管理器获取块作为 ManagedBuffer。
   */
  private def fetchRemoteManagedBuffer(
      blockId: BlockId,
      blockSize: Long,
      locationsAndStatus: BlockManagerMessages.BlockLocationsAndStatus): Option[ManagedBuffer] = {
    // 如果块大小超过阈值，我们应该将 FileManger 传递给
    // BlockTransferService，它将利用它来溢出块；如果没有，则传入的
    // null 值意味着块将持久化在内存中。
    val tempFileManager = if (blockSize > maxRemoteBlockToMem) {  // 如果数据块过大
      remoteBlockTempFileManager
    } else {
      null
    }

    var runningFailureCount = 0
    var totalFailureCount = 0
    // 对于指定的物理块， 按照同主机， 同机架等的优先级优先对主机位置进行排序
    val locations = sortLocations(locationsAndStatus.locations)
    val maxFetchFailures = locations.size
    var locationIterator = locations.iterator

    while (locationIterator.hasNext) {
      val loc = locationIterator.next()
      logDebug(s"Getting remote block $blockId from $loc")
      val data = try {
        // 同步获取远程物理块
        val buf = blockTransferService.fetchBlockSync(loc.host, loc.port, loc.executorId,
          blockId.toString, tempFileManager)

        if (blockSize > 0 && buf.size() == 0) {
          throw new IllegalStateException("Empty buffer received for non empty block " +
            s"when fetching remote block $blockId from $loc")
        }

        buf
      } catch {
        case NonFatal(e) =>
          runningFailureCount += 1
          totalFailureCount += 1

          if (totalFailureCount >= maxFetchFailures) {
            // 放弃尝试更多位置。要么我们已经尝试了所有原始位置，
            // 要么我们已经从主节点刷新了位置列表，并且在尝试
            // 刷新列表中的位置后仍然遇到失败。
            logWarning(s"Failed to fetch remote block $blockId " +
              s"from [${locations.mkString(", ")}] after $totalFailureCount fetch failures. " +
              s"Most recent failure cause:", e)
            return None
          }

          logWarning(s"Failed to fetch remote block $blockId " +
            s"from $loc (failed attempt $runningFailureCount)", e)

          // 如果有大量执行器，则位置列表可能包含大量
          // 过时条目，导致大量重试，可能需要大量时间。
          // 为了摆脱这些过时条目，我们在一定数量的获取失败后刷新块位置
          if (runningFailureCount >= maxFailuresBeforeLocationRefresh) {
            locationIterator = sortLocations(master.getLocations(blockId)).iterator
            logDebug(s"Refreshed locations from the driver " +
              s"after ${runningFailureCount} fetch failures.")
            runningFailureCount = 0
          }

          // 此位置失败，所以我们通过在此处返回 null 从不同位置重试获取
          null
      }

      if (data != null) {
        // 如果 ManagedBuffer 是 BlockManagerManagedBuffer，
        // 支持它的字节缓冲区的处理可能需要在读取字节后处理。
        // 在这种情况下，由于我们刚刚远程获取了字节，我们没有 BlockManagerManagedBuffer。
        // 这里的断言是为了确保这成立 （或者处理已处理）。
        assert(!data.isInstanceOf[BlockManagerManagedBuffer])
        return Some(data)
      }
      logDebug(s"The value of block $blockId is null")
    }
    logDebug(s"Block $blockId not found")
    None
  }


  /**
   * 从远程块管理器获取块作为序列化字节。
   */
  def getRemoteBytes(blockId: BlockId): Option[ChunkedByteBuffer] = {
    getRemoteBlock(blockId, (data: ManagedBuffer) => {
      // SPARK-24307 未记录的"逃生舱"，以防在转换为
      // ChunkedByteBuffer 时出现任何问题，回到旧代码路径。
      // 如果新路径稳定，可以在 Spark 2.4 后删除。
      if (remoteReadNioBufferConversion) {
        new ChunkedByteBuffer(data.nioByteBuffer())
      } else {
        ChunkedByteBuffer.fromManagedBuffer(data)
      }
    })
  }

  private def preferExecutors(locations: Seq[BlockManagerId]): Seq[BlockManagerId] = {
    val (executors, shuffleServers) = locations.partition(_.port != externalShuffleServicePort)
    executors ++ shuffleServers
  }

  /**
   * 返回给定块的位置列表，优先考虑本地机器，因为多个块管理器可以共享同一主机，然后是同一机架上的主机。
   * 在上述每个组（同一主机、同一机架和其他）中，执行器优先于外部 shuffle 服务。
   */
  private[spark] def sortLocations(locations: Seq[BlockManagerId]): Seq[BlockManagerId] = {
    val locs = Random.shuffle(locations)
    val (preferredLocs, otherLocs) = locs.partition(_.host == blockManagerId.host)
    val orderedParts = blockManagerId.topologyInfo match {
      case None => Seq(preferredLocs, otherLocs)
      case Some(_) =>
        val (sameRackLocs, differentRackLocs) = otherLocs.partition {
          loc => blockManagerId.topologyInfo == loc.topologyInfo
        }
        Seq(preferredLocs, sameRackLocs, differentRackLocs)
    }
    orderedParts.map(preferExecutors).reduce(_ ++ _)
  }


  /**
   * 将独占写锁降级为共享读锁。
   */
  def downgradeLock(blockId: BlockId): Unit = {
    blockInfoManager.downgradeLock(blockId)
  }

  /**
   * 使用显式 TaskContext 释放给定块的锁。
   * 参数 `taskContext` 应该在我们无法获取正确的 TaskContext 时传入，
   * 例如，缓存 RDD 的输入迭代器在子线程中迭代到末尾。
   */
  def releaseLock(blockId: BlockId, taskContext: Option[TaskContext] = None): Unit = {
    val taskAttemptId = taskContext.map(_.taskAttemptId())
    // SPARK-27666. 当任务完成时，Spark 自动释放此任务锁定的所有块。
    // 我们不应该为已经完成的任务释放任何锁。
    if (taskContext.isDefined && taskContext.get.isCompleted) {
      logWarning(s"Task ${taskAttemptId.get} already completed, not releasing lock for $blockId")
    } else {
      blockInfoManager.unlock(blockId, taskAttemptId)
    }
  }

  /**
   * 向 BlockManager 注册任务，以便初始化每个任务的簿记结构。
   */
  def registerTask(taskAttemptId: Long): Unit = {
    blockInfoManager.registerTask(taskAttemptId)
  }

  /**
   * 释放给定任务的所有锁。
   *
   * @return 锁被释放的块。
   */
  def releaseAllLocksForTask(taskAttemptId: Long): Seq[BlockId] = {
    blockInfoManager.releaseAllLocksForTask(taskAttemptId)
  }

  /**
   * 获取可以直接将数据写入磁盘的块写入器的快捷方法。
   * 块将附加到由 filename 指定的文件。调用者应处理错误情况。
   */
  def getDiskWriter(blockId: BlockId,
                     file: File,
                     serializerInstance: SerializerInstance,
                     bufferSize: Int,
                     writeMetrics: ShuffleWriteMetricsReporter): DiskBlockObjectWriter = {
    val syncWrites = conf.get(config.SHUFFLE_SYNC)
    new DiskBlockObjectWriter(file, serializerManager, serializerInstance, bufferSize,
      syncWrites, writeMetrics, blockId)
  }

  // ======================================================================================== //
  // 读取或写入对应的BlockId->BlockManager                                                       //
  // ======================================================================================== //


  /**
   * 用于从已在内存中的字节存储块的辅助器。
   * '''重要！''' 调用者不得改变或释放 `bytes` 底层的数据缓冲区。
   * 这样做可能会损坏或更改 `BlockManager` 存储的数据。
   */
  private case class ByteBufferBlockStoreUpdater[T](blockId: BlockId,
                                                     level: StorageLevel,
                                                     classTag: ClassTag[T],
                                                     bytes: ChunkedByteBuffer,
                                                     tellMaster: Boolean = true,
                                                     keepReadLock: Boolean = false)
    extends BlockStoreUpdater[T](bytes.size, blockId, level, classTag, tellMaster, keepReadLock) {

    override def readToByteBuffer(): ChunkedByteBuffer = bytes

    /**
     * ByteBufferBlockData 包装器不会被释放，以避免释放调用者拥有的缓冲区。
     */
    override def blockData(): BlockData = new ByteBufferBlockData(bytes, false)

    override def saveToDiskStore(): Unit = diskStore.putBytes(blockId, bytes)

  }

  /**
   * 从本地块管理器获取块作为序列化字节。
   */
  def getLocalBytes(blockId: BlockId): Option[BlockData] = {
    logDebug(s"获取本地块 $blockId 作为字节")
    assert(!blockId.isShuffle, s"意外的 ShuffleBlockId $blockId")
    blockInfoManager.lockForReading(blockId).map { info => doGetLocalBytes(blockId, info) }
  }


  /**
   * 从本地块管理器获取块作为序列化字节。
   *
   * 必须在持有块的读锁时调用。
   * 在异常时释放读锁；在成功返回时保持读锁。
   */
  private def doGetLocalBytes(blockId: BlockId, info: BlockInfo): BlockData = {
    val level = info.level
    logDebug(s"Level for block $blockId is $level")
    // 按顺序，尝试从内存读取序列化字节，然后从磁盘，然后回退到
    // 序列化内存中的对象，最后，如果块不存在则抛出异常。
    if (level.deserialized) {
      // 尝试通过从磁盘读取预序列化副本来避免昂贵的序列化：
      if (level.useDisk && diskStore.contains(blockId)) {
        // 注意：我们故意不尝试将块放回内存。由于此分支
        // 处理反序列化的块，此块可能只作为对象缓存在内存中，而不是
        // 序列化字节。因为调用者只请求字节，缓存
        // 块的反序列化对象没有意义，因为该缓存可能没有回报。
        diskStore.getBytes(blockId)
      } else if (level.useMemory && memoryStore.contains(blockId)) {
        // 在磁盘上未找到块，所以序列化内存中的副本：
        new ByteBufferBlockData(serializerManager.dataSerializeWithExplicitClassTag(
          blockId, memoryStore.getValues(blockId).get, info.classTag), true)
      } else {
        handleLocalReadFailure(blockId)
      }
    } else {  // 存储级别是序列化的
      if (level.useMemory && memoryStore.contains(blockId)) {
        new ByteBufferBlockData(memoryStore.getBytes(blockId).get, false)
      } else if (level.useDisk && diskStore.contains(blockId)) {
        val diskData = diskStore.getBytes(blockId)
        maybeCacheDiskBytesInMemory(info, blockId, level, diskData)
          .map(new ByteBufferBlockData(_, false))
          .getOrElse(diskData)
      } else {
        handleLocalReadFailure(blockId)
      }
    }
  }

  /**
   * 从本地块管理器获取块作为 Java 对象的迭代器。
   */
  def getLocalValues(blockId: BlockId): Option[BlockResult] = {
    logDebug(s"获取本地块 $blockId")
    blockInfoManager.lockForReading(blockId) match {
      case None =>
        logDebug(s"未找到块 $blockId")
        None
      case Some(info) =>
        val level = info.level
        logDebug(s"Level for block $blockId is $level")
        val taskContext = Option(TaskContext.get())
        // 从内存中读取数据
        if (level.useMemory && memoryStore.contains(blockId)) {
          val iter: Iterator[Any] = if (level.deserialized) {
            memoryStore.getValues(blockId).get
          } else {
            // 反序列化数据
            serializerManager.dataDeserializeStream(blockId, memoryStore.getBytes(blockId).get.toInputStream())(info.classTag)
          }
          // 我们需要捕获当前的 taskId，以防迭代器完成从
          // 没有设置 TaskContext 的不同线程触发；参见 SPARK-18406 的讨论。
          val ci = CompletionIterator[Any, Iterator[Any]](iter, {
            releaseLock(blockId, taskContext)
          })
          Some(new BlockResult(ci, DataReadMethod.Memory, info.size))
        } else if (level.useDisk && diskStore.contains(blockId)) {
          // 从磁盘中读取数据
          try {
            val diskData = diskStore.getBytes(blockId)
            val iterToReturn: Iterator[Any] = {
              if (level.deserialized) {
                val diskValues = serializerManager.dataDeserializeStream(
                  blockId,
                  diskData.toInputStream())(info.classTag)
                maybeCacheDiskValuesInMemory(info, blockId, level, diskValues)
              } else {
                val stream = maybeCacheDiskBytesInMemory(info, blockId, level, diskData)
                  .map { _.toInputStream(dispose = false) }
                  .getOrElse { diskData.toInputStream() }
                serializerManager.dataDeserializeStream(blockId, stream)(info.classTag)
              }
            }
            val ci = CompletionIterator[Any, Iterator[Any]](iterToReturn, {
              releaseLockAndDispose(blockId, diskData, taskContext)
            })
            Some(new BlockResult(ci, DataReadMethod.Disk, info.size))
          } catch {
            case ex: KryoException if ex.getCause.isInstanceOf[IOException] =>
              // 我们需要详细的日志消息来轻松捕获环境问题。
              // 更多详情：https://issues.apache.org/jira/browse/SPARK-37710
              processKryoException(ex, blockId)
              throw ex
          }
        } else {
          handleLocalReadFailure(blockId)
        }
    }
  }

  /**
   * 主要是提供给 RDD cache 使用。
   * @return 如果块成功缓存则返回 BlockResult，如果块无法缓存则返回迭代器。
   */
  def getOrElseUpdate[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[T],
      makeIterator: () => Iterator[T]): Either[BlockResult, Iterator[T]] = {
    // 尝试从本地或远程存储读取块。如果存在，那么我们不需要
    // 通过本地获取或放置路径。
    get[T](blockId)(classTag) match {
      case Some(block) =>
        return Left(block)
      case _ =>
      // 需要计算块。
    }
    // 最初我们对此块不持有任何锁。
    doPutIterator(blockId, makeIterator, level, classTag, keepReadLock = true) match {
      case None =>
        // doPut() 没有将工作交还给我们，所以块已经存在或成功存储。 因此，我们现在持有块的读锁。
        val blockResult = getLocalValues(blockId).getOrElse {
          // 由于我们在 doPut() 和 get() 调用之间持有读锁，块不应该被驱逐，所以 get() 不返回块表示某些内部错误。
          releaseLock(blockId)
          throw SparkCoreErrors.failToGetBlockWithLockError(blockId)
        }
        // 我们已经从 doPut() 调用持有块的读锁，getLocalValues() 再次获取锁，所以我们需要在这里调用 releaseLock()，
        // 以便净锁获取数为 1（因为调用者只会调用 release() 一次）。
        releaseLock(blockId)
        Left(blockResult)
      case Some(iter) =>
        // 放置失败，可能是因为数据太大无法放入内存且无法放到磁盘。
        // 因此，我们需要将输入迭代器传回调用者， 以便他们可以决定如何处理值（例如，不缓存地处理它们）。
       Right(iter)
    }
  }

  /**
   * @return 如果块被存储则返回 true，如果发生错误则返回 false。
   */
  def putIterator[T: ClassTag](blockId: BlockId, values: Iterator[T],
      level: StorageLevel, tellMaster: Boolean = true): Boolean = {

    require(values != null, "Values is null")
    doPutIterator(blockId, () => values, level, implicitly[ClassTag[T]], tellMaster) match {
      case None =>
        true
      case Some(iter) =>
        // 调用者不关心迭代器值，所以我们可以在这里关闭迭代器
        // 以更早地释放资源
        iter.close()
        false
    }
  }

  /**
   * 将新的序列化字节块放入块管理器。
   *
   * '''重要！''' 调用者不得改变或释放 `bytes` 底层的数据缓冲区。
   * 这样做可能会损坏或更改 `BlockManager` 存储的数据。
   *
   * @return 如果块被存储则返回 true，如果发生错误则返回 false。
   */
  def putBytes[T: ClassTag](blockId: BlockId, bytes: ChunkedByteBuffer,
                             level: StorageLevel, tellMaster: Boolean = true): Boolean = {
    require(bytes != null, "Bytes is null")

    val blockStoreUpdater =
      ByteBufferBlockStoreUpdater(blockId, level, implicitly[ClassTag[T]], bytes, tellMaster)

    blockStoreUpdater.save()
  }

  /**
   * 使用给定的存储级别在本地放置块。
   *
   * '''重要！''' 调用者不得改变或释放 `bytes` 底层的数据缓冲区。
   * 这样做可能会损坏或更改 `BlockManager` 存储的数据。
   */
  override def putBlockData(blockId: BlockId,
                             data: ManagedBuffer,
                             level: StorageLevel,
                             classTag: ClassTag[_]): Boolean = {
    putBytes(blockId, new ChunkedByteBuffer(data.nioByteBuffer()), level)(classTag)
  }

  /**
   * 用于从 [[BlockStoreUpdater.save()]] 和 [[doPutIterator()]] 抽象公共代码的辅助方法。
   *
   * @param putBody 尝试实际 put() 的函数，成功时返回 None，失败时返回 Some。
   */
  private def doPut[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[_],
      tellMaster: Boolean,
      keepReadLock: Boolean)(putBody: BlockInfo => Option[T]): Option[T] = {

    require(blockId != null, "BlockId 为 null")
    require(level != null && level.isValid, "StorageLevel 为 null 或无效")
    checkShouldStore(blockId)

    val putBlockInfo = {
      val newInfo = new BlockInfo(level, classTag, tellMaster)
      if (blockInfoManager.lockNewBlockForWriting(blockId, newInfo)) {
        newInfo
      } else {
        logWarning(s"块 $blockId 已经存在于此机器上；不重新添加")
        if (!keepReadLock) {
          // lockNewBlockForWriting 在现有块上返回了读锁，所以我们必须释放它：
          releaseLock(blockId)
        }
        return None
      }
    }

    val startTimeNs = System.nanoTime()
    var exceptionWasThrown: Boolean = true
    val result: Option[T] = try {
      val res = putBody(putBlockInfo)
      exceptionWasThrown = false
      if (res.isEmpty) {
        // 块成功存储
        if (keepReadLock) {
          blockInfoManager.downgradeLock(blockId)
        } else {
          blockInfoManager.unlock(blockId)
        }
      } else {
        removeBlockInternal(blockId, tellMaster = false)
        logWarning(s"Putting block $blockId failed")
      }
      res
    } catch {
      // 由于 removeBlockInternal 可能抛出异常，
      // 我们应该首先打印异常以显示根本原因。
      case NonFatal(e) =>
        logWarning(s"Putting block $blockId failed due to exception $e.")
        throw e
    } finally {
      // 此清理在 finally 块中执行，而不是在 `catch` 中，以避免必须
      // 捕获并正确重新抛出 InterruptedException。
      if (exceptionWasThrown) {
        // 如果抛出了异常，那么 `putBody` 中的代码可能已经
        // 通知主节点此块的可用性，所以我们需要发送更新
        // 来删除此块位置。.
        removeBlockInternal(blockId, tellMaster = tellMaster)
        removeBlockInternal(blockId, tellMaster = tellMaster)
        // `putBody` 代码也可能已经向 TaskMetrics 添加了新的块状态，
        // 所以我们需要通过用空块状态覆盖它来取消。我们只在
        // finally 块通过异常进入时才这样做，因为无条件地这样做会
        // 导致我们为每个由于内存不足而无法缓存的块发送空块状态
        // （这是预期的失败，与未捕获的异常不同）。
        addUpdatedBlockStatusToTaskMetrics(blockId, BlockStatus.empty)
      }
    }
    val usedTimeMs = Utils.getUsedTimeNs(startTimeNs)
    if (level.replication > 1) {
      logDebug(s"放置带复制的块 ${blockId} 耗时 $usedTimeMs")
    } else {
      logDebug(s"放置不带复制的块 ${blockId} 耗时 ${usedTimeMs}")
    }
    result
  }


  /**
   * 根据给定级别将给定块放入其中一个块存储中，必要时复制值。
   * 如果块已经存在，此方法不会覆盖它。
   *
   * @param keepReadLock 如果为 true，此方法在返回时将持有读锁（即使块已经存在）。
   *                     如果为 false，此方法在返回时将不持有任何锁。
   * @return 如果块已经存在或放置成功则返回 None，否则返回 Some(iterator)。
   */
  private def doPutIterator[T](
      blockId: BlockId,
      iterator: () => Iterator[T],
      level: StorageLevel,
      classTag: ClassTag[T],
      tellMaster: Boolean = true,
      keepReadLock: Boolean = false): Option[PartiallyUnrolledIterator[T]] = {
    doPut(blockId, level, classTag, tellMaster = tellMaster, keepReadLock = keepReadLock) { info =>
      val startTimeNs = System.nanoTime()
      var iteratorFromFailedMemoryStorePut: Option[PartiallyUnrolledIterator[T]] = None
      // 块的大小（字节）
      var size = 0L
      if (level.useMemory) {
        // 首先放入内存，即使它也设置了 useDisk 为 true；
        // 如果内存存储无法容纳，我们稍后会将其放到磁盘上。
        if (level.deserialized) {
          memoryStore.putIteratorAsValues(blockId, iterator(), level.memoryMode, classTag) match {
            case Right(s) =>
              size = s
            case Left(iter) =>
              // 没有足够的空间展开此块；如果适用，放到磁盘
              if (level.useDisk) {
                logWarning(s"Persisting block $blockId to disk instead.")
                diskStore.put(blockId) { channel =>
                  val out = Channels.newOutputStream(channel)
                  serializerManager.dataSerializeStream(blockId, out, iter)(classTag)
                }
                size = diskStore.getSize(blockId)
              } else {
                iteratorFromFailedMemoryStorePut = Some(iter)
              }
          }
        } else {  // !level.deserialized
          memoryStore.putIteratorAsBytes(blockId, iterator(), classTag, level.memoryMode) match {
            case Right(s) =>
              size = s
            case Left(partiallySerializedValues) =>
              // 没有足够的空间展开此块；如果适用，放到磁盘
              if (level.useDisk) {
                logWarning(s"将块 $blockId 持久化到磁盘。")
                diskStore.put(blockId) { channel =>
                  val out = Channels.newOutputStream(channel)
                  partiallySerializedValues.finishWritingToStream(out)
                }
                size = diskStore.getSize(blockId)
              } else {
                iteratorFromFailedMemoryStorePut = Some(partiallySerializedValues.valuesIterator)
              }
          }
        }

      } else if (level.useDisk) {
        diskStore.put(blockId) { channel =>
          val out = Channels.newOutputStream(channel)
          serializerManager.dataSerializeStream(blockId, out, iterator())(classTag)
        }
        size = diskStore.getSize(blockId)
      }

      val putBlockStatus = getCurrentBlockStatus(blockId, info)
      val blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid
      if (blockWasSuccessfullyStored) {
        // 现在块在内存或磁盘存储中，告诉主节点。
        info.size = size
        if (tellMaster && info.tellMaster) {
          reportBlockStatus(blockId, putBlockStatus)
        }
        addUpdatedBlockStatusToTaskMetrics(blockId, putBlockStatus)
        logDebug(s"Put block $blockId locally took ${Utils.getUsedTimeNs(startTimeNs)}")
        if (level.replication > 1) {
          val remoteStartTimeNs = System.nanoTime()
          val bytesToReplicate = doGetLocalBytes(blockId, info)
          // [SPARK-16550] 使用默认序列化时擦除类型化的 classTag，
          // 因为 NettyBlockRpcServer 在反序列化 repl 定义的类时崩溃。
          // TODO(ekl) 一旦远程端的类加载器问题得到修复，就删除这个。
          val remoteClassTag = if (!serializerManager.canUseKryo(classTag)) {
            scala.reflect.classTag[Any]
          } else {
            classTag
          }
          try {
            replicate(blockId, bytesToReplicate, level, remoteClassTag)
          } finally {
            bytesToReplicate.dispose()
          }
          logDebug(s"Put block $blockId remotely took ${Utils.getUsedTimeNs(remoteStartTimeNs)}")
        }
      }
      assert(blockWasSuccessfullyStored == iteratorFromFailedMemoryStorePut.isEmpty)
      iteratorFromFailedMemoryStorePut
    }
  }

  /**
   * 尝试将从磁盘读取的溢出字节缓存到 MemoryStore 中，以加速后续读取。此方法要求调用者持有块的读锁。
   * @return 如果放置成功则返回内存存储中字节的副本，否则返回 None。
   *         如果这返回内存存储中的字节，则原始磁盘存储字节将自动释放，调用者不应继续使用它们。
   *         否则，如果这返回 None，则原始磁盘存储字节将不受影响。
   */
  private def maybeCacheDiskBytesInMemory(blockInfo: BlockInfo,
      blockId: BlockId, level: StorageLevel,
      diskData: BlockData): Option[ChunkedByteBuffer] = {

    require(!level.deserialized)
    if (level.useMemory) {
      // 在 blockInfo 上同步以防止两个读取器都尝试
      // 将从磁盘读取的值放入 MemoryStore 的竞争条件。
      blockInfo.synchronized {
        if (memoryStore.contains(blockId)) {
          diskData.dispose()
          Some(memoryStore.getBytes(blockId).get)
        } else {
          val allocator = level.memoryMode match {
            case MemoryMode.ON_HEAP => ByteBuffer.allocate _
            case MemoryMode.OFF_HEAP => Platform.allocateDirectBuffer _
          }
          val putSucceeded = memoryStore.putBytes(blockId, diskData.size, level.memoryMode, () => {
            // https://issues.apache.org/jira/browse/SPARK-6076
            // 如果文件大小大于可用内存，将发生 OOM。所以如果我们
            // 无法将其放入 MemoryStore，就不应该创建 copyForMemory。这就是为什么
            // 此操作放入 `() => ChunkedByteBuffer` 并惰性创建。
            diskData.toChunkedByteBuffer(allocator)
          })
          if (putSucceeded) {
            diskData.dispose()
            Some(memoryStore.getBytes(blockId).get)
          } else {
            None
          }
        }
      }
    } else {
      None
    }
  }

  /**
   * 尝试将从磁盘读取的溢出值缓存到 MemoryStore 中，以加速后续读取。
   * 此方法要求调用者持有块的读锁。
   *
   * @return 迭代器的副本。传递给此方法的原始迭代器在此方法返回后不应再使用。
   */
  private def maybeCacheDiskValuesInMemory[T](
      blockInfo: BlockInfo,
      blockId: BlockId,
      level: StorageLevel,
      diskIterator: Iterator[T]): Iterator[T] = {
    require(level.deserialized)
    val classTag = blockInfo.classTag.asInstanceOf[ClassTag[T]]
    if (level.useMemory) {
      // 在 blockInfo 上同步以防止两个读取器都尝试
      // 将从磁盘读取的值放入 MemoryStore 的竞争条件。
      blockInfo.synchronized {
        if (memoryStore.contains(blockId)) {
          // 注意：如果我们有丢弃磁盘迭代器的方法，我们会在这里这样做。
          memoryStore.getValues(blockId).get
        } else {
          memoryStore.putIteratorAsValues(blockId, diskIterator, level.memoryMode, classTag) match {
            case Left(iter) =>
              // 内存存储 put() 失败，所以它将迭代器返回给我们：
              iter
            case Right(_) =>
              // put() 成功，所以我们可以读回值：
              memoryStore.getValues(blockId).get
          }
        }
      }.asInstanceOf[Iterator[T]]
    } else {
      diskIterator
    }
  }

  /**
   * 获取系统中的对等块管理器。
   */
  private[storage] def getPeers(forceFetch: Boolean): Seq[BlockManagerId] = {
    peerFetchLock.synchronized {
      val cachedPeersTtl = conf.get(config.STORAGE_CACHED_PEERS_TTL) // milliseconds
      val diff = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastPeerFetchTimeNs)
      val timeout = diff > cachedPeersTtl
      if (cachedPeers == null || forceFetch || timeout) {
        cachedPeers = master.getPeers(blockManagerId).sortBy(_.hashCode)
        lastPeerFetchTimeNs = System.nanoTime()
        logDebug("Fetched peers from master: " + cachedPeers.mkString("[", ",", "]"))
      }
      if (cachedPeers.isEmpty &&
          conf.get(config.STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).isDefined) {
        Seq(FallbackStorage.FALLBACK_BLOCK_MANAGER_ID)
      } else {
        cachedPeers
      }
    }
  }

  /**
   * 基于 existingReplicas 和 maxReplicas 将块复制到对等块管理器
   *
   * @param blockId 正在复制的 blockId
   * @param existingReplicas 具有副本的现有块管理器
   * @param maxReplicas 所需的最大副本数
   * @param maxReplicationFailures 在放弃之前容忍的复制失败次数
   * @return 块是否成功复制
   */
  def replicateBlock(
      blockId: BlockId,
      existingReplicas: Set[BlockManagerId],
      maxReplicas: Int,
      maxReplicationFailures: Option[Int] = None): Boolean = {

    logInfo(s"使用 $blockManagerId 主动复制 $blockId")

    blockInfoManager.lockForReading(blockId).forall { info =>
      val data = doGetLocalBytes(blockId, info)
      val storageLevel = StorageLevel(
        useDisk = info.level.useDisk,
        useMemory = info.level.useMemory,
        useOffHeap = info.level.useOffHeap,
        deserialized = info.level.deserialized,
        replication = maxReplicas)

      // 我们知道我们被调用是因为执行器移除或因为当前执行器
      // 正在被停用。所以我们在尝试复制之前刷新对等缓存，我们不会
      // 尝试复制到丢失的执行器/另一个正在停用的执行器

      getPeers(forceFetch = true)
      try {
        replicate(
          blockId, data, storageLevel, info.classTag, existingReplicas, maxReplicationFailures)
      } finally {
        logDebug(s"Releasing lock for $blockId")
        releaseLockAndDispose(blockId, data)
      }
    }
  }

  /**
   * 将块复制到另一个节点。注意这是一个阻塞调用，在块复制后返回。
   */
  private def replicate(blockId: BlockId,
      data: BlockData,
      level: StorageLevel,
      classTag: ClassTag[_],
      existingReplicas: Set[BlockManagerId] = Set.empty,
      maxReplicationFailures: Option[Int] = None): Boolean = {

    val maxReplicationFailureCount = maxReplicationFailures.getOrElse(
      conf.get(config.STORAGE_MAX_REPLICATION_FAILURE))

    val tLevel = StorageLevel(
      useDisk = level.useDisk,
      useMemory = level.useMemory,
      useOffHeap = level.useOffHeap,
      deserialized = level.deserialized,
      replication = 1)

    val numPeersToReplicateTo = level.replication - 1
    val startTime = System.nanoTime

    val peersReplicatedTo = mutable.HashSet.empty ++ existingReplicas
    val peersFailedToReplicateTo = mutable.HashSet.empty[BlockManagerId]
    var numFailures = 0

    val initialPeers = getPeers(false).filterNot(existingReplicas.contains)

    var peersForReplication = blockReplicationPolicy.prioritize(
      blockManagerId,
      initialPeers,
      peersReplicatedTo,
      blockId,
      numPeersToReplicateTo)

    while(numFailures <= maxReplicationFailureCount && !peersForReplication.isEmpty &&
      peersReplicatedTo.size < numPeersToReplicateTo) {
      val peer = peersForReplication.head
      try {
        val onePeerStartTime = System.nanoTime
        logTrace(s"Trying to replicate $blockId of ${data.size} bytes to $peer")
        // 此线程保持块的锁，所以我们不希望 netty 线程在
        // 完成发送消息时解锁块。
        val buffer = new BlockManagerManagedBuffer(blockInfoManager, blockId, data, false,
          unlockOnDeallocate = false)
        blockTransferService.uploadBlockSync(
          peer.host,
          peer.port,
          peer.executorId,
          blockId,
          buffer,
          tLevel,
          classTag)
        logTrace(s"Replicated $blockId of ${data.size} bytes to $peer" +
          s" in ${(System.nanoTime - onePeerStartTime).toDouble / 1e6} ms")
        peersForReplication = peersForReplication.tail
        peersReplicatedTo += peer
      } catch {
        // 重新抛出中断异常
        case e: InterruptedException =>
          throw e
        // 其他一切我们可能重试
        case NonFatal(e) =>
          logWarning(s"Failed to replicate $blockId to $peer, failure #$numFailures", e)
          peersFailedToReplicateTo += peer
          // 我们有一个失败的复制，所以我们再次获取对等列表
          // 我们不想要已经复制到的对等节点和之前失败的对等节点
          val filteredPeers = getPeers(true).filter { p =>
            !peersFailedToReplicateTo.contains(p) && !peersReplicatedTo.contains(p)
          }

          numFailures += 1
          peersForReplication = blockReplicationPolicy.prioritize(
            blockManagerId,
            filteredPeers,
            peersReplicatedTo,
            blockId,
            numPeersToReplicateTo - peersReplicatedTo.size)
      }
    }
    logDebug(s"Replicating $blockId of ${data.size} bytes to " +
      s"${peersReplicatedTo.size} peer(s) took ${(System.nanoTime - startTime) / 1e6} ms")
    if (peersReplicatedTo.size < numPeersToReplicateTo) {
      logWarning(s"Block $blockId replicated to only " +
        s"${peersReplicatedTo.size} peer(s) instead of $numPeersToReplicateTo peers")
      return false
    }

    logDebug(s"block $blockId replicated to ${peersReplicatedTo.mkString(", ")}")
    return true
  }

  /**
   * 读取由单个对象组成的块。
   */
  def getSingle[T: ClassTag](blockId: BlockId): Option[T] = {
    get[T](blockId).map(_.data.next().asInstanceOf[T])
  }

  /**
   * 写入由单个对象组成的块。
   *
   * @return 如果块被存储则返回 true，如果块已经存储或发生错误则返回 false。
   */
  def putSingle[T: ClassTag](
      blockId: BlockId,
      value: T,
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean = {
    putIterator(blockId, Iterator(value), level, tellMaster)
  }

  /**
   * 从内存中删除块，如果适用可能将其放在磁盘上。当内存存储
   * 达到其限制并需要释放空间时调用。
   *
   * 如果 `data` 没有放在磁盘上，它不会被创建。
   *
   * 此方法的调用者必须在调用此方法之前持有块的写锁。
   * 此方法不释放写锁。
   *
   * @return 块的新有效 StorageLevel。
   */
  private[storage] override def dropFromMemory[T: ClassTag](
      blockId: BlockId,
      data: () => Either[Array[T], ChunkedByteBuffer]): StorageLevel = {
    logInfo(s"Dropping block $blockId from memory")
    val info = blockInfoManager.assertBlockIsLockedForWriting(blockId)
    var blockIsUpdated = false
    val level = info.level

    // 如果存储级别需要，删除到磁盘
    if (level.useDisk && !diskStore.contains(blockId)) {
      logInfo(s"Writing block $blockId to disk")
      data() match {
        case Left(elements) =>
          diskStore.put(blockId) { channel =>
            val out = Channels.newOutputStream(channel)
            serializerManager.dataSerializeStream(
              blockId,
              out,
              elements.iterator)(info.classTag.asInstanceOf[ClassTag[T]])
          }
        case Right(bytes) =>
          diskStore.putBytes(blockId, bytes)
      }
      blockIsUpdated = true
    }

    // 实际从内存存储中删除
    val droppedMemorySize =
      if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
    val blockIsRemoved = memoryStore.remove(blockId)
    if (blockIsRemoved) {
      blockIsUpdated = true
    } else {
      logWarning(s"Block $blockId could not be dropped from memory as it does not exist")
    }

    val status = getCurrentBlockStatus(blockId, info)
    if (info.tellMaster) {
      reportBlockStatus(blockId, status, droppedMemorySize)
    }
    if (blockIsUpdated) {
      addUpdatedBlockStatusToTaskMetrics(blockId, status)
    }
    status.storageLevel
  }

  /**
   * 删除属于给定 RDD 的所有块。
   *
   * @return 删除的块数。
   */
  def removeRdd(rddId: Int): Int = {
    // TODO: 通过创建 RDD.id 到块的另一个映射来避免线性扫描。
    logInfo(s"Removing RDD $rddId")
    val blocksToRemove = blockInfoManager.entries.flatMap(_._1.asRDDId).filter(_.rddId == rddId)
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster = false) }
    blocksToRemove.size
  }

  def decommissionBlockManager(): Unit = storageEndpoint.ask(DecommissionBlockManager)

  private[spark] def decommissionSelf(): Unit = synchronized {
    decommissioner match {
      case None =>
        logInfo("开始块管理器停用过程...")
        decommissioner = Some(new BlockManagerDecommissioner(conf, this))
        decommissioner.foreach(_.start())
      case Some(_) =>
        logDebug("块管理器已经处于停用状态")
    }
  }

  /**
   * 返回最后迁移时间和表示是否所有块都已迁移的布尔值。
   * 如果自那时以来有任何任务运行，布尔值可能不正确。
   */
  private[spark] def lastMigrationInfo(): (Long, Boolean) = {
    decommissioner.map(_.lastMigrationInfo()).getOrElse((0, false))
  }

  private[storage] def getMigratableRDDBlocks(): Seq[ReplicateBlock] =
    master.getReplicateInfoForRDDBlocks(blockManagerId)

  /**
   * 删除属于给定广播的所有块。
   */
  def removeBroadcast(broadcastId: Long, tellMaster: Boolean): Int = {
    logDebug(s"删除广播 $broadcastId")
    val blocksToRemove = blockInfoManager.entries.map(_._1).collect {
      case bid @ BroadcastBlockId(`broadcastId`, _) => bid
    }
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster) }
    blocksToRemove.size
  }

  /**
   * 从内存和磁盘中删除块。
   */
  def removeBlock(blockId: BlockId, tellMaster: Boolean = true): Unit = {
    logDebug(s"删除块 $blockId")
    blockInfoManager.lockForWriting(blockId) match {
      case None =>
        // 块已经被删除；什么都不做。
        logWarning(s"要求删除块 $blockId，但它不存在")
      case Some(info) =>
        removeBlockInternal(blockId, tellMaster = tellMaster && info.tellMaster)
        addUpdatedBlockStatusToTaskMetrics(blockId, BlockStatus.empty)
    }
  }


  /**
   * [[removeBlock()]] 的内部版本，假设调用者已经持有块的写锁。
   */
  private def removeBlockInternal(blockId: BlockId, tellMaster: Boolean): Unit = {
    val blockStatus = if (tellMaster) {
      val blockInfo = blockInfoManager.assertBlockIsLockedForWriting(blockId)
      Some(getCurrentBlockStatus(blockId, blockInfo))
    } else None

    // 在磁盘存储和内存存储中删除是幂等的。最坏情况下，我们得到一个警告。
    val removedFromMemory = memoryStore.remove(blockId)
    val removedFromDisk = diskStore.remove(blockId)
    if (!removedFromMemory && !removedFromDisk) {
      logWarning(s"Block $blockId could not be removed as it was not found on disk or in memory")
    }

    blockInfoManager.removeBlock(blockId)
    if (tellMaster) {
      // 只从删除前捕获的块状态更新存储级别，以便
      // 保留内存大小和磁盘大小用于计算增量。
      reportBlockStatus(blockId, blockStatus.get.copy(storageLevel = StorageLevel.NONE))
    }
  }

  private def addUpdatedBlockStatusToTaskMetrics(blockId: BlockId, status: BlockStatus): Unit = {
    if (conf.get(config.TASK_METRICS_TRACK_UPDATED_BLOCK_STATUSES)) {
      Option(TaskContext.get()).foreach { c =>
        c.taskMetrics().incUpdatedBlockStatuses(blockId -> status)
      }
    }
  }

  def releaseLockAndDispose(
      blockId: BlockId,
      data: BlockData,
      taskContext: Option[TaskContext] = None): Unit = {
    releaseLock(blockId, taskContext)
    data.dispose()
  }

  def stop(): Unit = {
    decommissioner.foreach(_.stop())
    blockTransferService.close()
    if (blockStoreClient ne blockTransferService) {
      // 关闭应该是幂等的，但对于 NioBlockTransferService 可能不是。
      blockStoreClient.close()
    }
    remoteBlockTempFileManager.stop()
    diskBlockManager.stop()
    rpcEnv.stop(storageEndpoint)
    blockInfoManager.clear()
    memoryStore.clear()
    futureExecutionContext.shutdownNow()
    logInfo("BlockManager stopped")
  }
}


private[spark] object BlockManager {
  private val ID_GENERATOR = new IdGenerator

  def blockIdsToLocations(
      blockIds: Array[BlockId],
      env: SparkEnv,
      blockManagerMaster: BlockManagerMaster = null): Map[BlockId, Seq[String]] = {

    // blockManagerMaster != null 用于测试
    assert(env != null || blockManagerMaster != null)
    val blockLocations: Seq[Seq[BlockManagerId]] = if (blockManagerMaster == null) {
      env.blockManager.getLocationBlockIds(blockIds)
    } else {
      blockManagerMaster.getLocations(blockIds)
    }

    val blockManagers = new HashMap[BlockId, Seq[String]]
    for (i <- 0 until blockIds.length) {
      blockManagers(blockIds(i)) = blockLocations(i).map { loc =>
        ExecutorCacheTaskLocation(loc.host, loc.executorId).toString
      }
    }
    blockManagers.toMap
  }

  private class ShuffleMetricsSource(
      override val sourceName: String,
      metricSet: MetricSet) extends Source {

    override val metricRegistry = new MetricRegistry
    metricRegistry.registerAll(metricSet)
  }

  class RemoteBlockDownloadFileManager(
       blockManager: BlockManager,
       encryptionKey: Option[Array[Byte]])
      extends DownloadFileManager with Logging {

    private class ReferenceWithCleanup(
        file: DownloadFile,
        referenceQueue: JReferenceQueue[DownloadFile]
        ) extends WeakReference[DownloadFile](file, referenceQueue) {

      // 我们不能在这里使用 `file.delete()`，否则它不会被垃圾回收
      val filePath = file.path()

      def cleanUp(): Unit = {
        logDebug(s"Clean up file $filePath")

        if (!new File(filePath).delete()) {
          logDebug(s"Fail to delete file $filePath")
        }
      }
    }

    private val referenceQueue = new JReferenceQueue[DownloadFile]
    private val referenceBuffer = Collections.newSetFromMap[ReferenceWithCleanup](
      new ConcurrentHashMap)

    private val POLL_TIMEOUT = 1000
    @volatile private var stopped = false

    private val cleaningThread = new Thread() { override def run(): Unit = { keepCleaning() } }
    cleaningThread.setDaemon(true)
    cleaningThread.setName("RemoteBlock-temp-file-clean-thread")
    cleaningThread.start()

    override def createTempFile(transportConf: TransportConf): DownloadFile = {
      val file = blockManager.diskBlockManager.createTempLocalBlock()._2
      encryptionKey match {
        case Some(key) =>
          // 启用了加密，所以当我们从网络读取解密数据时，我们需要
          // 在写入磁盘时加密它。注意数据可能在远程端缓存到磁盘时
          // 已经加密，但现在已经解密（参见 EncryptedBlockData）。
          new EncryptedDownloadFile(file, key)
        case None =>
          new SimpleDownloadFile(file, transportConf)
      }
    }

    override def registerTempFileToClean(file: DownloadFile): Boolean = {
      referenceBuffer.add(new ReferenceWithCleanup(file, referenceQueue))
    }

    def stop(): Unit = {
      stopped = true
      cleaningThread.interrupt()
      cleaningThread.join()
    }

    private def keepCleaning(): Unit = {
      while (!stopped) {
        try {
          Option(referenceQueue.remove(POLL_TIMEOUT))
            .map(_.asInstanceOf[ReferenceWithCleanup])
            .foreach { ref =>
              referenceBuffer.remove(ref)
              ref.cleanUp()
            }
        } catch {
          case _: InterruptedException =>
            // no-op
          case NonFatal(e) =>
            logError("Error in cleaning thread", e)
        }
      }
    }
  }

  /**
   * 一个在写入时加密数据，在读取时解密的 DownloadFile。
   */
  private class EncryptedDownloadFile(
      file: File,
      key: Array[Byte]) extends DownloadFile {

    private val env = SparkEnv.get

    override def delete(): Boolean = file.delete()

    override def openForWriting(): DownloadFileWritableChannel = {
      new EncryptedDownloadWritableChannel()
    }

    override def path(): String = file.getAbsolutePath

    private class EncryptedDownloadWritableChannel extends DownloadFileWritableChannel {
      private val countingOutput: CountingWritableChannel = new CountingWritableChannel(
        Channels.newChannel(env.serializerManager.wrapForEncryption(new FileOutputStream(file))))

      override def closeAndRead(): ManagedBuffer = {
        countingOutput.close()
        val size = countingOutput.getCount
        new EncryptedManagedBuffer(new EncryptedBlockData(file, size, env.conf, key))
      }

      override def write(src: ByteBuffer): Int = countingOutput.write(src)

      override def isOpen: Boolean = countingOutput.isOpen()

      override def close(): Unit = countingOutput.close()
    }
  }
}
