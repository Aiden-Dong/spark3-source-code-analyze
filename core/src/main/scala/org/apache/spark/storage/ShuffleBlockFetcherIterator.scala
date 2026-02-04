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

import java.io.{InputStream, IOException}
import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.CheckedInputStream
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Queue}
import scala.util.{Failure, Success}

import io.netty.util.internal.OutOfDirectMemoryError
import org.apache.commons.io.IOUtils
import org.roaringbitmap.RoaringBitmap

import org.apache.spark.{MapOutputTracker, TaskContext}
import org.apache.spark.MapOutputTracker.SHUFFLE_PUSH_MAP_ID
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.shuffle._
import org.apache.spark.network.shuffle.checksum.{Cause, ShuffleChecksumHelper}
import org.apache.spark.network.util.{NettyUtils, TransportConf}
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.util.{CompletionIterator, TaskCompletionListener, Utils}

/**
 * 一个获取多个 block 的迭代器。对于本地 block，它从本地 block manager 获取。
 * 对于远程 block，它使用提供的 BlockTransferService 来获取。
 *
 * 这创建了一个 (BlockID, InputStream) 元组的迭代器，以便调用者可以在接收到 block 时以流水线方式处理它们。
 * 该实现对远程获取进行节流，使它们不超过 maxBytesInFlight，以避免使用过多内存。
 *
 * @param context [[TaskContext]]，用于指标更新
 * @param shuffleClient [[BlockStoreClient]]，用于获取远程 block
 * @param blockManager [[BlockManager]]，用于读取本地 block
 * @param blocksByAddress 按 [[BlockManagerId]] 分组的要获取的 block 列表。
 *                     对于每个 block，我们还需要两个信息：
 *                       1. 大小（以字节为单位的 long 字段） 以便限制内存使用；
 *                       2. 此 block 的 mapIndex，表示在 map 阶段的索引。
 *                   注意零大小的 block 已被排除，这发生在 [[org.apache.spark.MapOutputTracker.convertMapStatuses]] 中。
 * @param mapOutputTracker [[MapOutputTracker]]，当启用基于推送的 shuffle 时， 如果获取 shuffle 块失败，则回退到获取原始 block。
 * @param streamWrapper 包装返回的输入流的函数。
 * @param maxBytesInFlight 在任何给定时间点获取的远程 block 的最大大小（以字节为单位）。
 * @param maxReqsInFlight 在任何给定时间点获取 block 的最大远程请求数。
 * @param maxBlocksInFlightPerAddress 对于给定的远程 host:port，在任何给定时间点 正在获取的 shuffle block 的最大数量。
 * @param maxReqSizeShuffleToMem 可以 shuffle 到内存的请求的最大大小（以字节为单位）。
 * @param maxAttemptsOnNettyOOM  由于 Netty OOM 而抛出获取失败之前，block 可以重试的最大次数。
 * @param detectCorrupt          是否检测获取的 block 中的任何损坏。
 * @param checksumEnabled        是否启用 shuffle 校验和。启用时，Spark 将尝试诊断 block 损坏的原因。
 * @param checksumAlgorithm      计算 block 数据校验和值时使用的校验和算法。
 * @param shuffleMetrics 用于报告 shuffle 指标。
 * @param doBatchFetch 如果服务器端支持，则批量获取来自同一执行器的连续 shuffle block。
 */
private[spark]
final class ShuffleBlockFetcherIterator(
    context: TaskContext,
    shuffleClient: BlockStoreClient,
    blockManager: BlockManager,
    mapOutputTracker: MapOutputTracker,
    blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])],
    streamWrapper: (BlockId, InputStream) => InputStream,
    maxBytesInFlight: Long,
    maxReqsInFlight: Int,
    maxBlocksInFlightPerAddress: Int,
    val maxReqSizeShuffleToMem: Long,
    maxAttemptsOnNettyOOM: Int,
    detectCorrupt: Boolean,
    detectCorruptUseExtraMemory: Boolean,
    checksumEnabled: Boolean,
    checksumAlgorithm: String,
    shuffleMetrics: ShuffleReadMetricsReporter,
    doBatchFetch: Boolean)
  extends Iterator[(BlockId, InputStream)] with DownloadFileManager with Logging {

  import ShuffleBlockFetcherIterator._

  // 使远程请求最多为 maxBytesInFlight / 5 的长度；
  // 保持它们小于 maxBytesInFlight 是允许从最多 5 个节点进行多个并行获取，而不是阻塞在从一个节点读取输出上。
  private val targetRemoteRequestSize = math.max(maxBytesInFlight / 5, 1L)

  // 要获取的 block 总数。
  private[this] var numBlocksToFetch = 0

  /**
   * 调用者处理的 block 数量。当 [[numBlocksProcessed]] == [[numBlocksToFetch]] 时，迭代器耗尽。
   */
  private[this] var numBlocksProcessed = 0
  private[this] val startTimeNs = System.nanoTime()

  /** 要获取的主机本地 block，排除零大小的 block。*/
  private[this] val hostLocalBlocks = scala.collection.mutable.LinkedHashSet[(BlockId, Int)]()

  /**
   * 保存我们结果的队列。这将 [[org.apache.spark.network.BlockTransferService]]
   * 提供的异步模型转换为同步模型（迭代器）。
   */
  private[this] val results = new LinkedBlockingQueue[FetchResult]

  /**
   * 当前正在处理的 [[FetchResult]]。我们跟踪这个，以便在处理当前缓冲区时
   * 发生运行时异常的情况下可以释放当前缓冲区。
   */
  @volatile private[this] var currentResult: SuccessFetchResult = null

  /**
   * 要发出的获取请求队列；我们将逐渐从中拉取请求，以确保传输中的字节数
   * 限制在 maxBytesInFlight 内。
   */
  private[this] val fetchRequests = new Queue[FetchRequest]

  /**
   * 第一次出队时无法发出的获取请求队列。当满足获取约束时，
   * 这些请求会再次尝试。
   */
  private[this] val deferredFetchRequests = new HashMap[BlockManagerId, Queue[FetchRequest]]()

  /** 我们请求中当前传输的字节数 */
  private[this] var bytesInFlight = 0L

  /** 当前传输中的请求数 */
  private[this] var reqsInFlight = 0

  /** 每个 host:port 当前传输中的 block 数 */
  private[this] val numBlocksInFlightPerAddress = new HashMap[BlockManagerId, Int]()

  /** 计算由于 Netty OOM 导致的 block 重试次数。如果重试次数超过 [[maxAttemptsOnNettyOOM]]，block 将停止重试。*/
  private[this] val blockOOMRetryCounts = new HashMap[String, Int]

  /** 无法成功解压缩的 block，用于保证我们对那些损坏的 block 最多重试一次。 */
  private[this] val corruptedBlocks = mutable.HashSet[BlockId]()

  /** 迭代器是否仍然活跃。如果 isZombie 为 true，回调接口将不再将获取的 block 放入 [[results]] 中。 */
  @GuardedBy("this")
  private[this] var isZombie = false

  /**
   * 存储用于 shuffle 远程大 block 的文件集合。此集合中的文件将在清理时删除。
   * 这是防止磁盘文件泄漏的一层防御。
   */
  @GuardedBy("this")
  private[this] val shuffleFilesSet = mutable.HashSet[DownloadFile]()

  // shuffle 完成时的回调清理工作
  private[this] val onCompleteCallback = new ShuffleFetchCompletionListener(this)

  private[this] val pushBasedFetchHelper = new PushBasedFetchHelper(this, shuffleClient, blockManager, mapOutputTracker)

  initialize()

  // 减少缓冲区引用计数。
  // currentResult 设置为 null 以防止在 cleanup() 时再次释放缓冲
  private[storage] def releaseCurrentResultBuffer(): Unit = {
    // 如果需要，释放当前缓冲区
    if (currentResult != null) {
      currentResult.buf.release()
    }
    currentResult = null
  }

  override def createTempFile(transportConf: TransportConf): DownloadFile = {
    // 无论配置如何，我们都不需要在这里进行任何加密或解密，因为这在代码的另一层处理。
    // 当启用加密时，shuffle 数据首先以加密形式写入磁盘，并通过网络仍以加密形式发送。
    new SimpleDownloadFile(
      blockManager.diskBlockManager.createTempLocalBlock()._2, transportConf)
  }

  override def registerTempFileToClean(file: DownloadFile): Boolean = synchronized {
    if (isZombie) {
      false
    } else {
      shuffleFilesSet += file
      true
    }
  }

  /**
   *将迭代器标记为僵尸，并释放所有尚未反序列化的缓冲区。
   */
  private[storage] def cleanup(): Unit = {
    synchronized {
      isZombie = true
    }
    releaseCurrentResultBuffer()
    // 释放结果队列中的缓冲区
    val iter = results.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessFetchResult(blockId, mapIndex, address, _, buf, _) =>
          if (address != blockManager.blockManagerId) {
            if (hostLocalBlocks.contains(blockId -> mapIndex)) {
              shuffleMetrics.incLocalBlocksFetched(1)
              shuffleMetrics.incLocalBytesRead(buf.size)
            } else {
              shuffleMetrics.incRemoteBytesRead(buf.size)
              if (buf.isInstanceOf[FileSegmentManagedBuffer]) {
                shuffleMetrics.incRemoteBytesReadToDisk(buf.size)
              }
              shuffleMetrics.incRemoteBlocksFetched(1)
            }
          }
          buf.release()
        case _ =>
      }
    }
    shuffleFilesSet.foreach { file =>
      if (!file.delete()) {
        logWarning("Failed to cleanup shuffle fetch temp file " + file.path())
      }
    }
  }

  private[this] def sendRequest(req: FetchRequest): Unit = {
    logDebug("Sending request for %d blocks (%s) from %s".format(
      req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
    bytesInFlight += req.size
    reqsInFlight += 1

    // 这样我们就可以查找每个 blockID 的 block 信息
    val infoMap = req.blocks.map {
      case FetchBlockInfo(blockId, size, mapIndex) => (blockId.toString, (size, mapIndex))
    }.toMap
    val remainingBlocks = new HashSet[String]() ++= infoMap.keys
    val deferredBlocks = new ArrayBuffer[String]()
    val blockIds = req.blocks.map(_.blockId.toString)
    val address = req.address

    @inline def enqueueDeferredFetchRequestIfNecessary(): Unit = {
      if (remainingBlocks.isEmpty && deferredBlocks.nonEmpty) {
        val blocks = deferredBlocks.map { blockId =>
          val (size, mapIndex) = infoMap(blockId)
          FetchBlockInfo(BlockId(blockId), size, mapIndex)
        }
        results.put(DeferFetchRequestResult(FetchRequest(address, blocks.toSeq)))
        deferredBlocks.clear()
      }
    }

    // 通过请求监听器处理块成功跟失败到达时间
    val blockFetchingListener = new BlockFetchingListener {
      override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
        // 只有当迭代器不是僵尸时，才将缓冲区添加到结果队列中，
        // 即尚未调用 cleanup()。
        ShuffleBlockFetcherIterator.this.synchronized {
          if (!isZombie) {
            // 增加引用计数，因为我们需要将其传递给不同的线程。
            // 使用后需要释放。
            buf.retain()
            remainingBlocks -= blockId
            blockOOMRetryCounts.remove(blockId)
            results.put(new SuccessFetchResult(BlockId(blockId), infoMap(blockId)._2,
              address, infoMap(blockId)._1, buf, remainingBlocks.isEmpty))
            logDebug("remainingBlocks: " + remainingBlocks)
            enqueueDeferredFetchRequestIfNecessary()
          }
        }
        logTrace(s"Got remote block $blockId after ${Utils.getUsedTimeNs(startTimeNs)}")
      }

      override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
        ShuffleBlockFetcherIterator.this.synchronized {
          logError(s"Failed to get block(s) from ${req.address.host}:${req.address.port}", e)
          e match {
            // 捕获 Netty OOM 并尽早将标志 `isNettyOOMOnShuffle`（在任务间共享）
            //// 设置为 true。之后的待处理获取请求将不会发送，直到标志在以下情况下设置为 false：
            //// 1) Netty 空闲内存 >= maxReqSizeShuffleToMem
            ////    - 每当有获取请求成功时，我们都会检查这一点。
            //// 2) 传输中的请求数变为 0
            ////    - 每当调用 `fetchUpToMaxBytes` 时，我们都会检查这一点。
            //// 虽然 Netty 内存在多个模块间共享，例如 shuffle、rpc，但由于实现简单性考虑，
            //// 该标志仅对 shuffle 生效。
            //// 我们将缓冲由 OOM 错误引起的连续 block 失败，直到当前请求中没有剩余 block。
            //// 然后，我们将这些 block 打包到同一个获取请求中以便稍后重试。这样，
            //// 而不是为每个 block 创建获取请求，它将有助于减少远程服务器的并发连接和数据负载压力。
            //// 注意，捕获 OOM 并基于它做一些事情只是处理 Netty OOM 问题的一种解决方法，
            //// 这不是内存管理的最佳方式。当我们找到精确管理 Netty 内存的方法时，我们可以摆脱它。
            case _: OutOfDirectMemoryError
                if blockOOMRetryCounts.getOrElseUpdate(blockId, 0) < maxAttemptsOnNettyOOM =>
              if (!isZombie) {
                val failureTimes = blockOOMRetryCounts(blockId)
                blockOOMRetryCounts(blockId) += 1
                if (isNettyOOMOnShuffle.compareAndSet(false, true)) {
                  // 获取器可能会因相同错误而批量失败剩余 block。所以我们只记录一次警告以避免日志泛滥。
                  logInfo(s"Block $blockId has failed $failureTimes times " +
                    s"due to Netty OOM, will retry")
                }
                remainingBlocks -= blockId
                deferredBlocks += blockId
                enqueueDeferredFetchRequestIfNecessary()
              }

            case _ =>
              val block = BlockId(blockId)
              if (block.isShuffleChunk) {
                remainingBlocks -= blockId
                results.put(FallbackOnPushMergedFailureResult(
                  block, address, infoMap(blockId)._1, remainingBlocks.isEmpty))
              } else {
                results.put(FailureFetchResult(block, infoMap(blockId)._2, address, e))
              }
          }
        }
      }
    }

    // 当请求太大时，将远程 shuffle block 获取到磁盘。由于 shuffle 数据在网络上已经加密和压缩
    //（相对于相关配置），我们可以直接获取数据并将其写入文件。
    if (req.size > maxReqSizeShuffleToMem) {
      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
        blockFetchingListener, this)
    } else {
      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
        blockFetchingListener, null)
    }
  }

  /** 基于当前的Blocks 集合，分类出来： push-merge / local / host / remote 四种块集合 */
  private[this] def partitionBlocksByFetchMode(
      blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])],
      localBlocks: mutable.LinkedHashSet[(BlockId, Int)],
      hostLocalBlocksByExecutor: mutable.LinkedHashMap[BlockManagerId, Seq[(BlockId, Long, Int)]],
      pushMergedLocalBlocks: mutable.LinkedHashSet[BlockId]): ArrayBuffer[FetchRequest] = {
    logDebug(s"maxBytesInFlight: $maxBytesInFlight, targetRemoteRequestSize: "
      + s"$targetRemoteRequestSize, maxBlocksInFlightPerAddress: $maxBlocksInFlightPerAddress")

    // 分区为本地、主机本地、推送合并本地、远程（包括推送合并远程）block。
    // 远程 block 进一步分割为最多 maxBytesInFlight 大小的 FetchRequests，以限制传输中的数据量
    val collectedRemoteRequests = new ArrayBuffer[FetchRequest]
    var localBlockBytes = 0L          // 本地字节数量统计
    var hostLocalBlockBytes = 0L      // 当前主机字节数量统计
    var numHostLocalBlocks = 0
    var pushMergedLocalBlockBytes = 0L
    val prevNumBlocksToFetch = numBlocksToFetch

    val fallback = FallbackStorage.FALLBACK_BLOCK_MANAGER_ID.executorId
    val localExecIds = Set(blockManager.blockManagerId.executorId, fallback)


    for ((address, blockInfos) <- blocksByAddress) {     // 遍历迭代所有的请求块信息
      checkBlockSizes(blockInfos)

      if (pushBasedFetchHelper.isPushMergedShuffleBlockAddress(address)) {  // push-merge
        // 这些是推送合并 block 或这些 block 的 shuffle 块。
        if (address.host == blockManager.blockManagerId.host) {
          numBlocksToFetch += blockInfos.size
          pushMergedLocalBlocks ++= blockInfos.map(_._1)
          pushMergedLocalBlockBytes += blockInfos.map(_._2).sum
        } else {
          collectFetchRequests(address, blockInfos, collectedRemoteRequests)
        }
      } else if (localExecIds.contains(address.executorId)) {  // 本地块
        val mergedBlockInfos = mergeContinuousShuffleBlockIdsIfNeeded(
          blockInfos.map(info => FetchBlockInfo(info._1, info._2, info._3)), doBatchFetch)
        numBlocksToFetch += mergedBlockInfos.size
        localBlocks ++= mergedBlockInfos.map(info => (info.blockId, info.mapIndex))
        localBlockBytes += mergedBlockInfos.map(_.size).sum
      } else if (blockManager.hostLocalDirManager.isDefined &&
        address.host == blockManager.blockManagerId.host) {      // 当前主机块
        val mergedBlockInfos = mergeContinuousShuffleBlockIdsIfNeeded(
          blockInfos.map(info => FetchBlockInfo(info._1, info._2, info._3)), doBatchFetch)
        numBlocksToFetch += mergedBlockInfos.size
        val blocksForAddress =
          mergedBlockInfos.map(info => (info.blockId, info.size, info.mapIndex))
        hostLocalBlocksByExecutor += address -> blocksForAddress
        numHostLocalBlocks += blocksForAddress.size
        hostLocalBlockBytes += mergedBlockInfos.map(_.size).sum
      } else {   // 远程块
        val (_, timeCost) = Utils.timeTakenMs[Unit] {
          collectFetchRequests(address, blockInfos, collectedRemoteRequests)
        }
        logDebug(s"Collected remote fetch requests for $address in $timeCost ms")
      }
    }
    val (remoteBlockBytes, numRemoteBlocks) =
      collectedRemoteRequests.foldLeft((0L, 0))((x, y) => (x._1 + y.size, x._2 + y.blocks.size))
    val totalBytes = localBlockBytes + remoteBlockBytes + hostLocalBlockBytes +
      pushMergedLocalBlockBytes
    val blocksToFetchCurrentIteration = numBlocksToFetch - prevNumBlocksToFetch
    assert(blocksToFetchCurrentIteration == localBlocks.size +
      numHostLocalBlocks + numRemoteBlocks + pushMergedLocalBlocks.size,
        s"The number of non-empty blocks $blocksToFetchCurrentIteration doesn't equal to the sum " +
        s"of the number of local blocks ${localBlocks.size} + " +
        s"the number of host-local blocks ${numHostLocalBlocks} " +
        s"the number of push-merged-local blocks ${pushMergedLocalBlocks.size} " +
        s"+ the number of remote blocks ${numRemoteBlocks} ")
    logInfo(s"Getting $blocksToFetchCurrentIteration " +
      s"(${Utils.bytesToString(totalBytes)}) non-empty blocks including " +
      s"${localBlocks.size} (${Utils.bytesToString(localBlockBytes)}) local and " +
      s"${numHostLocalBlocks} (${Utils.bytesToString(hostLocalBlockBytes)}) " +
      s"host-local and ${pushMergedLocalBlocks.size} " +
      s"(${Utils.bytesToString(pushMergedLocalBlockBytes)}) " +
      s"push-merged-local and $numRemoteBlocks (${Utils.bytesToString(remoteBlockBytes)}) " +
      s"remote blocks")
    this.hostLocalBlocks ++= hostLocalBlocksByExecutor.values
      .flatMap { infos => infos.map(info => (info._1, info._3)) }
    collectedRemoteRequests
  }

  private def createFetchRequest(
      blocks: Seq[FetchBlockInfo],
      address: BlockManagerId,
      forMergedMetas: Boolean): FetchRequest = {
    logDebug(s"Creating fetch request of ${blocks.map(_.size).sum} at $address "
      + s"with ${blocks.size} blocks")
    FetchRequest(address, blocks, forMergedMetas)
  }

  private def createFetchRequests(
      curBlocks: Seq[FetchBlockInfo],
      address: BlockManagerId,
      isLast: Boolean,
      collectedRemoteRequests: ArrayBuffer[FetchRequest],
      enableBatchFetch: Boolean,
      forMergedMetas: Boolean = false): ArrayBuffer[FetchBlockInfo] = {
    val mergedBlocks = mergeContinuousShuffleBlockIdsIfNeeded(curBlocks, enableBatchFetch)
    numBlocksToFetch += mergedBlocks.size
    val retBlocks = new ArrayBuffer[FetchBlockInfo]
    if (mergedBlocks.length <= maxBlocksInFlightPerAddress) {
      collectedRemoteRequests += createFetchRequest(mergedBlocks, address, forMergedMetas)
    } else {
      mergedBlocks.grouped(maxBlocksInFlightPerAddress).foreach { blocks =>
        if (blocks.length == maxBlocksInFlightPerAddress || isLast) {
          collectedRemoteRequests += createFetchRequest(blocks, address, forMergedMetas)
        } else {
          // 最后一组不超过 `maxBlocksInFlightPerAddress`。将其放回 `curBlocks`。
          retBlocks ++= blocks
          numBlocksToFetch -= blocks.size
        }
      }
    }
    retBlocks
  }

  private def collectFetchRequests(
      address: BlockManagerId,
      blockInfos: Seq[(BlockId, Long, Int)],
      collectedRemoteRequests: ArrayBuffer[FetchRequest]): Unit = {
    val iterator = blockInfos.iterator
    var curRequestSize = 0L
    var curBlocks = new ArrayBuffer[FetchBlockInfo]()

    while (iterator.hasNext) {
      val (blockId, size, mapIndex) = iterator.next()
      curBlocks += FetchBlockInfo(blockId, size, mapIndex)
      curRequestSize += size
      blockId match {
        // 要么所有 block 都是推送合并 block、shuffle 块，要么是原始 block。
        // 基于这些类型，我们决定进行批量获取并创建设置了 forMergedMetas 的 FetchRequests。
        case ShuffleBlockChunkId(_, _, _, _) =>
          if (curRequestSize >= targetRemoteRequestSize ||
            curBlocks.size >= maxBlocksInFlightPerAddress) {
            curBlocks = createFetchRequests(curBlocks.toSeq, address, isLast = false,
              collectedRemoteRequests, enableBatchFetch = false)
            curRequestSize = curBlocks.map(_.size).sum
          }
        case ShuffleMergedBlockId(_, _, _) =>
          if (curBlocks.size >= maxBlocksInFlightPerAddress) {
            curBlocks = createFetchRequests(curBlocks.toSeq, address, isLast = false,
              collectedRemoteRequests, enableBatchFetch = false, forMergedMetas = true)
          }
        case _ =>
          // 对于批量获取，实际传输中的 block 应该计算合并 block。
          val mayExceedsMaxBlocks = !doBatchFetch && curBlocks.size >= maxBlocksInFlightPerAddress
          if (curRequestSize >= targetRemoteRequestSize || mayExceedsMaxBlocks) {
            curBlocks = createFetchRequests(curBlocks.toSeq, address, isLast = false,
              collectedRemoteRequests, doBatchFetch)
            curRequestSize = curBlocks.map(_.size).sum
          }
      }
    }
    // 添加最终请求
    if (curBlocks.nonEmpty) {
      val (enableBatchFetch, forMergedMetas) = {
        curBlocks.head.blockId match {
          case ShuffleBlockChunkId(_, _, _, _) => (false, false)
          case ShuffleMergedBlockId(_, _, _) => (false, true)
          case _ => (doBatchFetch, false)
        }
      }
      createFetchRequests(curBlocks.toSeq, address, isLast = true, collectedRemoteRequests,
        enableBatchFetch = enableBatchFetch, forMergedMetas = forMergedMetas)
    }
  }

  private def assertPositiveBlockSize(blockId: BlockId, blockSize: Long): Unit = {
    if (blockSize < 0) {
      throw BlockException(blockId, "Negative block size " + size)
    } else if (blockSize == 0) {
      throw BlockException(blockId, "Zero-sized blocks should be excluded.")
    }
  }

  private def checkBlockSizes(blockInfos: Seq[(BlockId, Long, Int)]): Unit = {
    blockInfos.foreach { case (blockId, size, _) => assertPositiveBlockSize(blockId, size) }
  }
  /**
   * 在获取远程 block 时获取本地 block。
   * 这是可以的，因为 `ManagedBuffer` 的内存在我们创建输入流时延迟分配，所以我们在内存中跟踪的只是 ManagedBuffer 引用本身。
   */
  private[this] def fetchLocalBlocks(localBlocks: mutable.LinkedHashSet[(BlockId, Int)]): Unit = {
    logDebug(s"Start fetching local blocks: ${localBlocks.mkString(", ")}")
    val iter = localBlocks.iterator
    while (iter.hasNext) {
      val (blockId, mapIndex) = iter.next()
      try {
        val buf = blockManager.getLocalBlockData(blockId)
        shuffleMetrics.incLocalBlocksFetched(1)
        shuffleMetrics.incLocalBytesRead(buf.size)
        buf.retain()   // 增加引用计数
        results.put(new SuccessFetchResult(blockId, mapIndex, blockManager.blockManagerId,
          buf.size(), buf, false))
      } catch {
        // 如果我们看到异常，立即停止。
        case e: Exception =>
          e match {
            // ClosedByInterruptException 是杀死任务时的预期异常，
            // 不要记录异常堆栈跟踪以避免混淆用户。
            // 参见：SPARK-28340
            case ce: ClosedByInterruptException =>
              logError("Error occurred while fetching local blocks, " + ce.getMessage)
            case ex: Exception => logError("Error occurred while fetching local blocks", ex)
          }
          results.put(new FailureFetchResult(blockId, mapIndex, blockManager.blockManagerId, e))
          return
      }
    }
  }

  private[this] def fetchHostLocalBlock(
      blockId: BlockId,
      mapIndex: Int,
      localDirs: Array[String],
      blockManagerId: BlockManagerId): Boolean = {
    try {
      val buf = blockManager.getHostLocalShuffleData(blockId, localDirs)
      buf.retain()
      results.put(SuccessFetchResult(blockId, mapIndex, blockManagerId, buf.size(), buf,
        isNetworkReqDone = false))
      true
    } catch {
      case e: Exception =>
        // If we see an exception, stop immediately.
        logError(s"Error occurred while fetching local blocks", e)
        results.put(FailureFetchResult(blockId, mapIndex, blockManagerId, e))
        false
    }
  }

  /**
   * 在获取远程 block 时获取主机本地 block。这是可以的，因为 `ManagedBuffer` 的内存
   * 在我们创建输入流时延迟分配，所以我们在内存中跟踪的只是 ManagedBuffer 引用本身。
   */
  private[this] def fetchHostLocalBlocks(
      hostLocalDirManager: HostLocalDirManager,
      hostLocalBlocksByExecutor: mutable.LinkedHashMap[BlockManagerId, Seq[(BlockId, Long, Int)]]):
    Unit = {
    val cachedDirsByExec = hostLocalDirManager.getCachedHostLocalDirs
    val (hostLocalBlocksWithCachedDirs, hostLocalBlocksWithMissingDirs) = {
      val (hasCache, noCache) = hostLocalBlocksByExecutor.partition { case (hostLocalBmId, _) =>
        cachedDirsByExec.contains(hostLocalBmId.executorId)
      }
      (hasCache.toMap, noCache.toMap)
    }

    if (hostLocalBlocksWithMissingDirs.nonEmpty) {
      logDebug(s"Asynchronous fetching host-local blocks without cached executors' dir: " +
        s"${hostLocalBlocksWithMissingDirs.mkString(", ")}")

      // 如果启用了外部 shuffle 服务，我们将一次性从外部 shuffle 服务获取多个执行器的本地目录，
      // 该服务与执行器位于同一主机上。否则，我们将直接从这些执行器逐一获取本地目录。
      // 获取请求不会太多，因为实际上一个主机几乎不可能同时有很多执行器。
      val dirFetchRequests = if (blockManager.externalShuffleServiceEnabled) {
        val host = blockManager.blockManagerId.host
        val port = blockManager.externalShuffleServicePort
        Seq((host, port, hostLocalBlocksWithMissingDirs.keys.toArray))
      } else {
        hostLocalBlocksWithMissingDirs.keys.map(bmId => (bmId.host, bmId.port, Array(bmId))).toSeq
      }

      dirFetchRequests.foreach { case (host, port, bmIds) =>
        hostLocalDirManager.getHostLocalDirs(host, port, bmIds.map(_.executorId)) {
          case Success(dirsByExecId) =>
            fetchMultipleHostLocalBlocks(
              hostLocalBlocksWithMissingDirs.filterKeys(bmIds.contains).toMap,
              dirsByExecId,
              cached = false)

          case Failure(throwable) =>
            logError("Error occurred while fetching host local blocks", throwable)
            val bmId = bmIds.head
            val blockInfoSeq = hostLocalBlocksWithMissingDirs(bmId)
            val (blockId, _, mapIndex) = blockInfoSeq.head
            results.put(FailureFetchResult(blockId, mapIndex, bmId, throwable))
        }
      }
    }

    if (hostLocalBlocksWithCachedDirs.nonEmpty) {
      logDebug(s"Synchronous fetching host-local blocks with cached executors' dir: " +
          s"${hostLocalBlocksWithCachedDirs.mkString(", ")}")
      fetchMultipleHostLocalBlocks(hostLocalBlocksWithCachedDirs, cachedDirsByExec, cached = true)
    }
  }

  private def fetchMultipleHostLocalBlocks(
      bmIdToBlocks: Map[BlockManagerId, Seq[(BlockId, Long, Int)]],
      localDirsByExecId: Map[String, Array[String]],
      cached: Boolean): Unit = {
    // 我们使用 `forall`，因为一旦有 block 获取失败，`fetchHostLocalBlock` 将立即
    // 将 `FailureFetchResult` 放入 `results`。所以没有理由获取剩余的 block。
    val allFetchSucceeded = bmIdToBlocks.forall { case (bmId, blockInfos) =>
      blockInfos.forall { case (blockId, _, mapIndex) =>
        fetchHostLocalBlock(blockId, mapIndex, localDirsByExecId(bmId.executorId), bmId)
      }
    }
    if (allFetchSucceeded) {
      logDebug(s"Got host-local blocks from ${bmIdToBlocks.keys.mkString(", ")} " +
        s"(${if (cached) "with" else "without"} cached executors' dir) " +
        s"in ${Utils.getUsedTimeNs(startTimeNs)}")
    }
  }

  private[this] def initialize(): Unit = {
    //TODO-1 : 添加任务完成回调

    // 添加任务完成回调（在成功和失败情况下都会调用）以进行清理。
    context.addTaskCompletionListener(onCompleteCallback)

    // TODO-2 : 按照获取模式分区块
    // 要获取的本地 block，排除零大小的 block。
    val localBlocks = mutable.LinkedHashSet[(BlockId, Int)]()    // 本地块集合
    val hostLocalBlocksByExecutor = mutable.LinkedHashMap[BlockManagerId, Seq[(BlockId, Long, Int)]]()
    val pushMergedLocalBlocks = mutable.LinkedHashSet[BlockId]()
    // 按不同的获取模式分区 block：本地、主机本地、推送合并本地和远程 block。
    val remoteRequests = partitionBlocksByFetchMode(
      blocksByAddress, localBlocks, hostLocalBlocksByExecutor, pushMergedLocalBlocks)

    // TODO-3 : 随机远程请求并加入队列
    fetchRequests ++= Utils.randomize(remoteRequests)
    assert ((0 == reqsInFlight) == (0 == bytesInFlight),
      "expected reqsInFlight = 0 but found reqsInFlight = " + reqsInFlight +
      ", expected bytesInFlight = 0 but found bytesInFlight = " + bytesInFlight)

    // TODO-4 : 开始远程块请求
    fetchUpToMaxBytes()

    val numDeferredRequest = deferredFetchRequests.values.map(_.size).sum
    val numFetches = remoteRequests.size - fetchRequests.size - numDeferredRequest
    logInfo(s"Started $numFetches remote fetches in ${Utils.getUsedTimeNs(startTimeNs)}" +
      (if (numDeferredRequest > 0 ) s", deferred $numDeferredRequest requests" else ""))

    // TODO-5 : 获取本地块 - 所有本地块的引用
    fetchLocalBlocks(localBlocks)
    logDebug(s"Got local blocks in ${Utils.getUsedTimeNs(startTimeNs)}")
    // 获取主机本地 block（如果有）v
    fetchAllHostLocalBlocks(hostLocalBlocksByExecutor)

    // TODO-6 : 获取推送合并的本地块
    pushBasedFetchHelper.fetchAllPushMergedLocalBlocks(pushMergedLocalBlocks)
  }

  private def fetchAllHostLocalBlocks(
      hostLocalBlocksByExecutor: mutable.LinkedHashMap[BlockManagerId, Seq[(BlockId, Long, Int)]]):
    Unit = {
    if (hostLocalBlocksByExecutor.nonEmpty) {
      blockManager.hostLocalDirManager.foreach(fetchHostLocalBlocks(_, hostLocalBlocksByExecutor))
    }
  }

  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch

  /**
   * 获取下一个 (BlockId, InputStream)。如果任务失败，每个 InputStream 底层的 ManagedBuffers
   * 将通过注册到 TaskCompletionListener 的 cleanup() 方法释放。但是，调用者应该在不再需要时
   * 尽快 close() 这些 InputStreams，以便尽早释放内存。
   *
   * 如果无法获取下一个 block，则抛出 FetchFailedException。
   */
  override def next(): (BlockId, InputStream) = {
    if (!hasNext) {
      throw SparkCoreErrors.noSuchElementError()
    }

    numBlocksProcessed += 1

    var result: FetchResult = null
    var input: InputStream = null
    // 这只在启用 shuffle 校验和时初始化。
    var checkedIn: CheckedInputStream = null
    var streamCompressedOrEncrypted: Boolean = false
    // 获取下一个获取结果并尝试解压缩它以检测数据损坏，
    // 如果损坏则再次获取一次，如果第二次获取也损坏则抛出 FailureFetchResult，
    // 以便可以重试前一阶段。
    // 对于本地 shuffle block，在第一次 IOException 时抛出 FailureFetchResult。
    while (result == null) {
      val startFetchWait = System.nanoTime()
      result = results.take()   // 取出头部结果
      val fetchWaitTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startFetchWait)
      shuffleMetrics.incFetchWaitTime(fetchWaitTime)

      result match {
        case r @ SuccessFetchResult(blockId, mapIndex, address, size, buf, isNetworkReqDone) =>
          if (address != blockManager.blockManagerId) {
            if (hostLocalBlocks.contains(blockId -> mapIndex) ||
              pushBasedFetchHelper.isLocalPushMergedBlockAddress(address)) {
              // 它是主机本地 block 或本地 shuffle 块
              shuffleMetrics.incLocalBlocksFetched(1)
              shuffleMetrics.incLocalBytesRead(buf.size)
            } else {
              numBlocksInFlightPerAddress(address) = numBlocksInFlightPerAddress(address) - 1
              shuffleMetrics.incRemoteBytesRead(buf.size)
              if (buf.isInstanceOf[FileSegmentManagedBuffer]) {
                shuffleMetrics.incRemoteBytesReadToDisk(buf.size)
              }
              shuffleMetrics.incRemoteBlocksFetched(1)
              bytesInFlight -= size
            }
          }
          if (isNetworkReqDone) {
            reqsInFlight -= 1
            resetNettyOOMFlagIfPossible(maxReqSizeShuffleToMem)
            logDebug("Number of requests in flight " + reqsInFlight)
          }

          if (buf.size == 0) {
            // 我们永远不会合法地接收到零大小的 block。所有零记录的 block 都有零大小，
            // 所有零大小的 block 都没有记录（因此首先不应该被请求）。
            // 此语句依赖于 shuffle 写入器的行为，这由以下测试用例保证：
            //
            // - BypassMergeSortShuffleWriterSuite: "write with some empty partitions"
            // - UnsafeShuffleWriterSuite: "writeEmptyIterator"
            // - DiskBlockObjectWriterSuite: "commit() and close() without ever opening or writing"
            //
            // SortShuffleWriter 没有明确的测试，但它使用的底层 API 由 UnsafeShuffleWriter 共享
            //（两个写入器都使用 DiskBlockObjectWriter，如果自上次调用以来没有写入记录，
            // 它会从 commitAndGet() 返回零大小。
            val msg = s"Received a zero-size buffer for block $blockId from $address " +
              s"(expectedApproxSize = $size, isNetworkReqDone=$isNetworkReqDone)"
            throwFetchFailedException(blockId, mapIndex, address, new IOException(msg))
          }

          val in = try {
            // 创建数据流读取
            val bufIn = buf.createInputStream()
            if (checksumEnabled) {
              val checksum = ShuffleChecksumHelper.getChecksumByAlgorithm(checksumAlgorithm)
              checkedIn = new CheckedInputStream(bufIn, checksum)
              checkedIn
            } else {
              bufIn
            }
          } catch {
            // 异常只能由本地 shuffle block 抛出
            case e: IOException =>
              assert(buf.isInstanceOf[FileSegmentManagedBuffer])
              e match {
                case ce: ClosedByInterruptException =>
                  logError("Failed to create input stream from local block, " +
                    ce.getMessage)
                case e: IOException => logError("Failed to create input stream from local block", e)
              }
              buf.release()
              if (blockId.isShuffleChunk) {
                pushBasedFetchHelper.initiateFallbackFetchForPushMergedBlock(blockId, address)
                // 将 result 设置为 null 以触发 while 循环的另一次迭代来获取。
                result = null
                null
              } else {
                throwFetchFailedException(blockId, mapIndex, address, e)
              }
          }
          if (in != null) {
            try {
              input = streamWrapper(blockId, in)
              // 如果流被压缩或包装，那么我们可选择将前 maxBytesInFlight/3 字节
              // 解压缩/解包到内存中，以检查该部分数据的损坏。
              // 但即使 'detectCorruptUseExtraMemory' 配置关闭，或者损坏在后面，
              // 我们仍然会在流的后面检测到损坏。
              streamCompressedOrEncrypted = !input.eq(in)
              if (streamCompressedOrEncrypted && detectCorruptUseExtraMemory) {
                // TODO: 管理这里使用的内存，并在 OOM 情况下将其溢出到磁盘。
                input = Utils.copyStreamUpTo(input, maxBytesInFlight / 3)
              }
            } catch {
              case e: IOException =>
                // 当启用 shuffle 校验和时，对于损坏两次的 block，
                // 我们会通过消耗 buf 中的剩余数据来计算 block 的校验和。
                // 所以，我们应该稍后释放 buf。
                if (!(checksumEnabled && corruptedBlocks.contains(blockId))) {
                  buf.release()
                }

                if (blockId.isShuffleChunk) {
                  // TODO (SPARK-36284): 为基于推送的 shuffle 添加 shuffle 校验和支持
                  // 重试损坏的 block 可能再次导致损坏的 block。
                  // 对于 shuffle 块，我们选择立即回退到属于该损坏 shuffle 块的原始 shuffle block， 而不是重试获取损坏的块。
                  // 这也使代码更简单，因为对应于 shuffle 块的 chunkMeta 在处理 shuffle 块时总是从 chunksMetaMap 中删除。
                  // 如果我们尝试重新获取损坏的 shuffle 块，那么它必须重新添加到 chunksMetaMap 中。
                  pushBasedFetchHelper.initiateFallbackFetchForPushMergedBlock(blockId, address)
                  // 将 result 设置为 null 以触发 while 循环的另一次迭代。
                  result = null
                } else if (buf.isInstanceOf[FileSegmentManagedBuffer]) {
                  throwFetchFailedException(blockId, mapIndex, address, e)
                } else if (corruptedBlocks.contains(blockId)) {
                  // 这是第二次检测到此 block 损坏
                  if (checksumEnabled) {
                    // 如果启用了 shuffle 校验和，则诊断数据损坏的原因
                    val diagnosisResponse = diagnoseCorruption(checkedIn, address, blockId)
                    buf.release()
                    logError(diagnosisResponse)
                    throwFetchFailedException(
                      blockId, mapIndex, address, e, Some(diagnosisResponse))
                  } else {
                    throwFetchFailedException(blockId, mapIndex, address, e)
                  }
                } else {
                  // 这是第一次检测到此 block 损坏
                  logWarning(s"got an corrupted block $blockId from $address, fetch again", e)
                  corruptedBlocks += blockId
                  fetchRequests += FetchRequest(address, Array(FetchBlockInfo(blockId, size, mapIndex)))
                  result = null
                }
            } finally {
              if (blockId.isShuffleChunk) {
                pushBasedFetchHelper.removeChunk(blockId.asInstanceOf[ShuffleBlockChunkId])
              }
              // TODO: 在这里释放 buf 以更早地释放内存
              if (input == null) {
                // 如果使用 streamWrapper 包装流时出现问题，则关闭底层流
                in.close()
              }
            }
          }

        case FailureFetchResult(blockId, mapIndex, address, e) =>
          var errorMsg: String = null
          if (e.isInstanceOf[OutOfDirectMemoryError]) {
            errorMsg = s"Block $blockId fetch failed after $maxAttemptsOnNettyOOM " +
              s"retries due to Netty OOM"
            logError(errorMsg)
          }
          throwFetchFailedException(blockId, mapIndex, address, e, Some(errorMsg))

        case DeferFetchRequestResult(request) =>
          val address = request.address
          numBlocksInFlightPerAddress(address) = numBlocksInFlightPerAddress(address) - request.blocks.size
          bytesInFlight -= request.size
          reqsInFlight -= 1
          logDebug("Number of requests in flight " + reqsInFlight)
          val defReqQueue = deferredFetchRequests.getOrElseUpdate(address, new Queue[FetchRequest]())
          defReqQueue.enqueue(request)
          result = null

        case FallbackOnPushMergedFailureResult(blockId, address, size, isNetworkReqDone) =>
          // 我们在 3 种情况下得到这个结果：
          // 1. 获取远程 shuffle 块的数据失败。在这种情况下，blockId 是 ShuffleBlockChunkId。
          // 2. 读取推送合并本地元数据失败。在这种情况下，blockId 是 ShuffleBlockId。
          // 3. 从外部 shuffle 服务获取推送合并本地目录失败。在这种情况下，blockId 是 ShuffleBlockId。
          if (pushBasedFetchHelper.isRemotePushMergedBlockAddress(address)) {
            numBlocksInFlightPerAddress(address) = numBlocksInFlightPerAddress(address) - 1
            bytesInFlight -= size
          }
          if (isNetworkReqDone) {
            reqsInFlight -= 1
            logDebug("Number of requests in flight " + reqsInFlight)
          }
          pushBasedFetchHelper.initiateFallbackFetchForPushMergedBlock(blockId, address)
          // 将 result 设置为 null 以触发 while 循环的另一次迭代来获取
          // SuccessFetchResult 或 FailureFetchResult。
          result = null

          case PushMergedLocalMetaFetchResult(
            shuffleId, shuffleMergeId, reduceId, bitmaps, localDirs) =>
            // 将推送合并本地 shuffle block 数据作为多个 shuffle 块获取
            val shuffleBlockId = ShuffleMergedBlockId(shuffleId, shuffleMergeId, reduceId)
            try {
              val bufs: Seq[ManagedBuffer] = blockManager.getLocalMergedBlockData(shuffleBlockId, localDirs)
              // 由于本地 block 元数据请求成功完成，numBlocksToFetch 递减。
              numBlocksToFetch -= 1
              // 更新要获取的 block 总数，反映多个本地 shuffle 块。
              numBlocksToFetch += bufs.size
              bufs.zipWithIndex.foreach { case (buf, chunkId) =>
                buf.retain()
                val shuffleChunkId = ShuffleBlockChunkId(shuffleId, shuffleMergeId, reduceId,
                  chunkId)
                pushBasedFetchHelper.addChunk(shuffleChunkId, bitmaps(chunkId))
                results.put(SuccessFetchResult(shuffleChunkId, SHUFFLE_PUSH_MAP_ID,
                  pushBasedFetchHelper.localShuffleMergerBlockMgrId, buf.size(), buf,
                  isNetworkReqDone = false))
              }
            } catch {
              case e: Exception =>
                // 如果我们在读取推送合并本地索引文件时看到异常，我们回退到获取原始 block。
                // 我们不报告 block 获取失败，将继续剩余的本地 block 读取。
                logWarning(s"Error occurred while reading push-merged-local index, " +
                  s"prepare to fetch the original blocks", e)
                pushBasedFetchHelper.initiateFallbackFetchForPushMergedBlock(
                  shuffleBlockId, pushBasedFetchHelper.localShuffleMergerBlockMgrId)
            }
            result = null

        case PushMergedRemoteMetaFetchResult(
          shuffleId, shuffleMergeId, reduceId, blockSize, bitmaps, address) =>
          // 原始元数据请求已处理，所以我们将 numBlocksToFetch 和 numBlocksInFlightPerAddress 减 1。
          // 我们将收集新的 shuffle 块请求，其计数在 collectFetchReqsFromMergedBlocks 中添加到 numBlocksToFetch。
          numBlocksInFlightPerAddress(address) = numBlocksInFlightPerAddress(address) - 1
          numBlocksToFetch -= 1
          val blocksToFetch = pushBasedFetchHelper.createChunkBlockInfosFromMetaResponse(
            shuffleId, shuffleMergeId, reduceId, blockSize, bitmaps)
          val additionalRemoteReqs = new ArrayBuffer[FetchRequest]
          collectFetchRequests(address, blocksToFetch.toSeq, additionalRemoteReqs)
          fetchRequests ++= additionalRemoteReqs
          // 将 result 设置为 null 以强制另一次迭代。
          result = null

        case PushMergedRemoteMetaFailedFetchResult(
          shuffleId, shuffleMergeId, reduceId, address) =>
          // 原始元数据请求失败，所以我们将 numBlocksInFlightPerAddress 减 1。
          numBlocksInFlightPerAddress(address) = numBlocksInFlightPerAddress(address) - 1
          // 如果我们无法获取推送合并 block 的元数据，我们回退到获取原始 block。
          pushBasedFetchHelper.initiateFallbackFetchForPushMergedBlock(
            ShuffleMergedBlockId(shuffleId, shuffleMergeId, reduceId), address)
          // 将 result 设置为 null 以强制另一次迭代。
          result = null
      }

      // 发送获取请求直到 maxBytesInFlight
      fetchUpToMaxBytes()
    }

    currentResult = result.asInstanceOf[SuccessFetchResult]
    (currentResult.blockId,
      new BufferReleasingInputStream(
        input,
        this,
        currentResult.blockId,
        currentResult.mapIndex,
        currentResult.address,
        detectCorrupt && streamCompressedOrEncrypted,
        currentResult.isNetworkReqDone,
        Option(checkedIn)))
  }

  /**
   * 获取损坏 block 的可疑损坏原因。只有在启用校验和且至少检测到一次损坏时才应调用。
   *
   * 这将首先消耗损坏 block 流的其余部分以计算 block 的校验和。
   * 然后，它将发起一个同步 RPC 调用，连同校验和一起询问服务器（获取损坏 block 的地方）
   * 诊断损坏的原因并返回它。
   *
   * 过程中引发的任何异常都将导致损坏原因的 [[Cause.UNKNOWN_ISSUE]]，
   * 因为损坏诊断只是尽力而为。
   *
   * @param checkedIn 用于计算校验和的 [[CheckedInputStream]]。
   * @param address 获取损坏 block 的地址。
   * @param blockId 损坏 block 的 blockId。
   * @return 不同原因的损坏诊断响应。
   */
  private[storage] def diagnoseCorruption(
      checkedIn: CheckedInputStream,
      address: BlockManagerId,
      blockId: BlockId): String = {
    logInfo("Start corruption diagnosis.")
    blockId match {
      case shuffleBlock: ShuffleBlockId =>
        val startTimeNs = System.nanoTime()
        val buffer = new Array[Byte](ShuffleChecksumHelper.CHECKSUM_CALCULATION_BUFFER)
        // 消耗剩余数据以计算校验和
        var cause: Cause = null
        try {
          while (checkedIn.read(buffer) != -1) {}
          val checksum = checkedIn.getChecksum.getValue
          cause = shuffleClient.diagnoseCorruption(address.host, address.port, address.executorId,
            shuffleBlock.shuffleId, shuffleBlock.mapId, shuffleBlock.reduceId, checksum,
            checksumAlgorithm)
        } catch {
          case e: Exception =>
            logWarning("Unable to diagnose the corruption cause of the corrupted block", e)
            cause = Cause.UNKNOWN_ISSUE
        }
        val duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)
        val diagnosisResponse = cause match {
          case Cause.UNSUPPORTED_CHECKSUM_ALGORITHM =>
            s"Block $blockId is corrupted but corruption diagnosis failed due to " +
              s"unsupported checksum algorithm: $checksumAlgorithm"

          case Cause.CHECKSUM_VERIFY_PASS =>
            s"Block $blockId is corrupted but checksum verification passed"

          case Cause.UNKNOWN_ISSUE =>
            s"Block $blockId is corrupted but the cause is unknown"

          case otherCause =>
            s"Block $blockId is corrupted due to $otherCause"
        }
        logInfo(s"Finished corruption diagnosis in $duration ms. $diagnosisResponse")
        diagnosisResponse
      case shuffleBlockChunk: ShuffleBlockChunkId =>
        // TODO SPARK-36284 为基于推送的 shuffle 添加 shuffle 校验和支持
        val diagnosisResponse = s"BlockChunk $shuffleBlockChunk is corrupted but corruption " +
          s"diagnosis is skipped due to lack of shuffle checksum support for push-based shuffle."
        logWarning(diagnosisResponse)
        diagnosisResponse
      case unexpected: BlockId =>
        throw new IllegalArgumentException(s"Unexpected type of BlockId, $unexpected")
    }
  }

  def toCompletionIterator: Iterator[(BlockId, InputStream)] = {
    CompletionIterator[(BlockId, InputStream), this.type](this,
      onCompleteCallback.onComplete(context))
  }

  private def fetchUpToMaxBytes(): Unit = {
    if (isNettyOOMOnShuffle.get()) {
      if (reqsInFlight > 0) {
        // 如果 Netty 仍然 OOM 且有正在进行的获取请求，则立即返回
        return
      } else {
        resetNettyOOMFlagIfPossible(0)
      }
    }

    // 优先处理被延迟的请求
    if (deferredFetchRequests.nonEmpty) {
      for ((remoteAddress, defReqQueue) <- deferredFetchRequests) {
        while (isRemoteBlockFetchable(defReqQueue) &&
            !isRemoteAddressMaxedOut(remoteAddress, defReqQueue.front)) {
          val request = defReqQueue.dequeue()
          logDebug(s"Processing deferred fetch request for $remoteAddress with "
            + s"${request.blocks.length} blocks")
          send(remoteAddress, request)
          if (defReqQueue.isEmpty) {
            deferredFetchRequests -= remoteAddress
          }
        }
      }
    }


    while (isRemoteBlockFetchable(fetchRequests)) {    // 开始处理请求块
      val request = fetchRequests.dequeue()
      val remoteAddress = request.address

      if (isRemoteAddressMaxedOut(remoteAddress, request)) {   // 当前主机请求数过多，则延迟请求，现在先不请求
        logDebug(s"Deferring fetch request for $remoteAddress with ${request.blocks.size} blocks")
        val defReqQueue = deferredFetchRequests.getOrElse(remoteAddress, new Queue[FetchRequest]())
        defReqQueue.enqueue(request)
        deferredFetchRequests(remoteAddress) = defReqQueue
      } else {
        send(remoteAddress, request)
      }
    }

    def send(remoteAddress: BlockManagerId, request: FetchRequest): Unit = {
      if (request.forMergedMetas) {
        pushBasedFetchHelper.sendFetchMergedStatusRequest(request)
      } else {
        sendRequest(request)
      }
      numBlocksInFlightPerAddress(remoteAddress) =
        numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size
    }

    def isRemoteBlockFetchable(fetchReqQueue: Queue[FetchRequest]): Boolean = {
      fetchReqQueue.nonEmpty &&
        (bytesInFlight == 0 ||
          (reqsInFlight + 1 <= maxReqsInFlight &&
            bytesInFlight + fetchReqQueue.front.size <= maxBytesInFlight))
    }

    // 检查发送新的获取请求是否会超过从给定远程地址获取的 block 的最大数量。
    def isRemoteAddressMaxedOut(remoteAddress: BlockManagerId, request: FetchRequest): Boolean = {
      numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size >
        maxBlocksInFlightPerAddress
    }
  }

  private[storage] def throwFetchFailedException(
      blockId: BlockId,
      mapIndex: Int,
      address: BlockManagerId,
      e: Throwable,
      message: Option[String] = None) = {
    val msg = message.getOrElse(e.getMessage)
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        throw SparkCoreErrors.fetchFailedError(address, shufId, mapId, mapIndex, reduceId, msg, e)
      case ShuffleBlockBatchId(shuffleId, mapId, startReduceId, _) =>
        throw SparkCoreErrors.fetchFailedError(address, shuffleId, mapId, mapIndex, startReduceId,
          msg, e)
      case _ => throw SparkCoreErrors.failToGetNonShuffleBlockError(blockId, e)
    }
  }

  /**
   * 以下所有方法都由 [[PushBasedFetchHelper]] 用于与迭代器通信
   */
  private[storage] def addToResultsQueue(result: FetchResult): Unit = {
    results.put(result)
  }

  private[storage] def decreaseNumBlocksToFetch(blocksFetched: Int): Unit = {
    numBlocksToFetch -= blocksFetched
  }

  /**
   * 当前由 [[PushBasedFetchHelper]] 用于在与推送合并 block 或 shuffle 块相关的获取失败时
   * 获取回退 block。
   * 这由任务线程在调用 `iterator.next()` 时执行，如果这启动了回退。
   */
  private[storage] def fallbackFetch(
      originalBlocksByAddr: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])]): Unit = {
    val originalLocalBlocks = mutable.LinkedHashSet[(BlockId, Int)]()
    val originalHostLocalBlocksByExecutor =
      mutable.LinkedHashMap[BlockManagerId, Seq[(BlockId, Long, Int)]]()
    val originalMergedLocalBlocks = mutable.LinkedHashSet[BlockId]()
    val originalRemoteReqs = partitionBlocksByFetchMode(originalBlocksByAddr,
      originalLocalBlocks, originalHostLocalBlocksByExecutor, originalMergedLocalBlocks)
    // 以随机顺序将远程请求添加到我们的队列中
    fetchRequests ++= Utils.randomize(originalRemoteReqs)
    logInfo(s"Created ${originalRemoteReqs.size} fallback remote requests for push-merged")
    // 获取所有本地的回退 block。
    fetchLocalBlocks(originalLocalBlocks)
    // 回退期间合并的本地 block 应该为空
    assert(originalMergedLocalBlocks.isEmpty, "There should be zero push-merged blocks during fallback")
    // 一些回退本地 block 可能是主机本地 block
    fetchAllHostLocalBlocks(originalHostLocalBlocksByExecutor)
  }

  /**
   * 辅助类，确保在 InputStream.close() 时释放 ManagedBuffer， 如果 streamCompressedOrEncrypted 为 true，还检测流损坏
   */
  private[storage] def removePendingChunks(
      failedBlockId: ShuffleBlockChunkId,
      address: BlockManagerId): mutable.HashSet[ShuffleBlockChunkId] = {
    val removedChunkIds = new mutable.HashSet[ShuffleBlockChunkId]()

    def sameShuffleReducePartition(block: BlockId): Boolean = {
      val chunkId = block.asInstanceOf[ShuffleBlockChunkId]
      chunkId.shuffleId == failedBlockId.shuffleId && chunkId.reduceId == failedBlockId.reduceId
    }

    def filterRequests(queue: mutable.Queue[FetchRequest]): Unit = {
      val fetchRequestsToRemove = new mutable.Queue[FetchRequest]()
      fetchRequestsToRemove ++= queue.dequeueAll { req =>
        val firstBlock = req.blocks.head
        firstBlock.blockId.isShuffleChunk && req.address.equals(address) &&
          sameShuffleReducePartition(firstBlock.blockId)
      }
      fetchRequestsToRemove.foreach { _ =>
        removedChunkIds ++=
          fetchRequestsToRemove.flatMap(_.blocks.map(_.blockId.asInstanceOf[ShuffleBlockChunkId]))
      }
    }

    filterRequests(fetchRequests)
    deferredFetchRequests.get(address).foreach { defRequests =>
      filterRequests(defRequests)
      if (defRequests.isEmpty) deferredFetchRequests.remove(address)
    }
    removedChunkIds
  }
}

/**
 * 辅助类，确保在 InputStream.close() 时释放 ManagedBuffer，
 * 如果 streamCompressedOrEncrypted 为 true，还检测流损坏
 */
private class BufferReleasingInputStream(
    // 这对测试可见
    private[storage] val delegate: InputStream,
    private val iterator: ShuffleBlockFetcherIterator,
    private val blockId: BlockId,
    private val mapIndex: Int,
    private val address: BlockManagerId,
    private val detectCorruption: Boolean,
    private val isNetworkReqDone: Boolean,
    private val checkedInOpt: Option[CheckedInputStream])
  extends InputStream {
  private[this] var closed = false

  override def read(): Int =
    tryOrFetchFailedException(delegate.read())

  override def close(): Unit = {
    if (!closed) {
      try {
        delegate.close()
        iterator.releaseCurrentResultBuffer()
      } finally {
        // 当远程请求完成且空闲内存足够时，取消设置标志。
        if (isNetworkReqDone) {
          ShuffleBlockFetcherIterator.resetNettyOOMFlagIfPossible(iterator.maxReqSizeShuffleToMem)
        }
        closed = true
      }
    }
  }

  override def available(): Int = delegate.available()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long =
    tryOrFetchFailedException(delegate.skip(n))

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int =
    tryOrFetchFailedException(delegate.read(b))

  override def read(b: Array[Byte], off: Int, len: Int): Int =
    tryOrFetchFailedException(delegate.read(b, off, len))

  override def reset(): Unit = delegate.reset()

  /**
   * 执行返回值的代码块，静默关闭此流，并在 detectCorruption 为 true 时
   * 将 IOException 重新抛出为 FetchFailedException。此方法目前仅由
   * `BufferReleasingInputStream` 内的 `read` 和 `skip` 方法使用。
   */
  private def tryOrFetchFailedException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException if detectCorruption =>
        val diagnosisResponse = checkedInOpt.map { checkedIn =>
          iterator.diagnoseCorruption(checkedIn, address, blockId)
        }
        IOUtils.closeQuietly(this)
        // 无论原因如何，我们都不会重试 block，因为 block 已被下游 RDD 部分消耗。
        iterator.throwFetchFailedException(blockId, mapIndex, address, e, diagnosisResponse)
    }
  }
}

/**
 * 在 ShuffleBlockFetcherIterator 完成时调用的监听器
 * @param data 要处理的 ShuffleBlockFetcherIterator
 */
private class ShuffleFetchCompletionListener(var data: ShuffleBlockFetcherIterator)
  extends TaskCompletionListener {

  override def onTaskCompletion(context: TaskContext): Unit = {
    if (data != null) {
      data.cleanup()
      // 在这里将引用设为 null，以确保在我们完成从中读取后，
      // 不保留对此 ShuffleBlockFetcherIterator 的引用，让它在 GC 期间被收集。
      // 否则我们可以保留 block 位置的元数据（blocksByAddress）
      data = null
    }
  }

  // Just an alias for onTaskCompletion to avoid confusing
  def onComplete(context: TaskContext): Unit = this.onTaskCompletion(context)
}

private[storage]
object ShuffleBlockFetcherIterator {

  /**
   * 一个标志，指示在 shuffle 期间是否出现了 Netty OOM 错误。
   * 如果为 true，除非没有传输中的获取请求，否则所有待处理的 shuffle 获取请求
   * 将被推迟，直到标志被取消设置（每当有完整的获取请求时）。
   */
  val isNettyOOMOnShuffle = new AtomicBoolean(false)

  def resetNettyOOMFlagIfPossible(freeMemoryLowerBound: Long): Unit = {
    if (isNettyOOMOnShuffle.get() && NettyUtils.freeDirectMemory() >= freeMemoryLowerBound) {
      isNettyOOMOnShuffle.compareAndSet(true, false)
    }
  }

  /**
   * 当 doBatchFetch 为 true 时，此函数用于合并 block。具有相同 `mapId` 的 block
   * 可以合并为一个 block 批次。block 批次由 reduceId 范围指定，
   * 这意味着我们可以批量获取的连续 shuffle block。
   * 例如，像 (shuffle_0_0_0, shuffle_0_0_1, shuffle_0_1_0) 这样的输入 block
   * 可以合并为 (shuffle_0_0_0_2, shuffle_0_1_0_1)，
   * 像 (shuffle_0_0_0_2, shuffle_0_0_2, shuffle_0_0_3) 这样的输入 block
   * 可以合并为 (shuffle_0_0_0_4)。
   *
   * @param blocks 如果可能要合并的 block。可能包含已合并的 block。
   * @param doBatchFetch 是否合并 block。
   * @return 如果 doBatchFetch=false 则返回输入 block，如果 doBatchFetch=true 则返回合并的 block。
   */
  def mergeContinuousShuffleBlockIdsIfNeeded(
      blocks: Seq[FetchBlockInfo],
      doBatchFetch: Boolean): Seq[FetchBlockInfo] = {
    val result = if (doBatchFetch) {
      val curBlocks = new ArrayBuffer[FetchBlockInfo]
      val mergedBlockInfo = new ArrayBuffer[FetchBlockInfo]

      def mergeFetchBlockInfo(toBeMerged: ArrayBuffer[FetchBlockInfo]): FetchBlockInfo = {
        val startBlockId = toBeMerged.head.blockId.asInstanceOf[ShuffleBlockId]

        // 最后合并的 block 可能来自输入，如果 map id 相同，我们可以将更多 block 合并到其中。
        def shouldMergeIntoPreviousBatchBlockId =
          mergedBlockInfo.last.blockId.asInstanceOf[ShuffleBlockBatchId].mapId == startBlockId.mapId

        val (startReduceId, size) =
          if (mergedBlockInfo.nonEmpty && shouldMergeIntoPreviousBatchBlockId) {
            // 删除之前的批次 block id，因为我们将添加一个新的来替换它。
            val removed = mergedBlockInfo.remove(mergedBlockInfo.length - 1)
            (removed.blockId.asInstanceOf[ShuffleBlockBatchId].startReduceId,
              removed.size + toBeMerged.map(_.size).sum)
          } else {
            (startBlockId.reduceId, toBeMerged.map(_.size).sum)
          }

        FetchBlockInfo(
          ShuffleBlockBatchId(
            startBlockId.shuffleId,
            startBlockId.mapId,
            startReduceId,
            toBeMerged.last.blockId.asInstanceOf[ShuffleBlockId].reduceId + 1),
          size,
          toBeMerged.head.mapIndex)
      }

      val iter = blocks.iterator
      while (iter.hasNext) {
        val info = iter.next()
        // 输入 block id 可能已经是批次 ID。例如，我们合并一些 block，
        // 然后根据"每个请求的最大 block 数"使用合并的 block 发出获取请求。
        // 最后的获取请求可能太小，我们放弃并将剩余的合并 block 放回输入列表。
        if (info.blockId.isInstanceOf[ShuffleBlockBatchId]) {
          mergedBlockInfo += info
        } else {
          if (curBlocks.isEmpty) {
            curBlocks += info
          } else {
            val curBlockId = info.blockId.asInstanceOf[ShuffleBlockId]
            val currentMapId = curBlocks.head.blockId.asInstanceOf[ShuffleBlockId].mapId
            if (curBlockId.mapId != currentMapId) {
              mergedBlockInfo += mergeFetchBlockInfo(curBlocks)
              curBlocks.clear()
            }
            curBlocks += info
          }
        }
      }
      if (curBlocks.nonEmpty) {
        mergedBlockInfo += mergeFetchBlockInfo(curBlocks)
      }
      mergedBlockInfo
    } else {
      blocks
    }
    result.toSeq
  }

  /**
   * 在 FetchRequest 中使用的要获取的 block 信息。
   * @param blockId block id
   * @param size block 的估计大小。注意这不是确切的字节数。
   *             远程 block 的大小用于计算 bytesInFlight。
   * @param mapIndex 此 block 的 mapIndex，表示在 map 阶段的索引。
   */
  private[storage] case class FetchBlockInfo(blockId: BlockId, size: Long, mapIndex: Int)

  /**
   * 从远程 BlockManager 获取 block 的请求。
   * @param address 要从中获取的远程 BlockManager。
   * @param blocks 要从同一地址获取的 block 信息序列。
   * @param forMergedMetas 如果此请求是为了请求推送合并元信息则为 true；
   *                       如果是为了常规或 shuffle 块则为 false。
   */
  case class FetchRequest(
      address: BlockManagerId,
      blocks: Seq[FetchBlockInfo],
      forMergedMetas: Boolean = false) {
    val size = blocks.map(_.size).sum
  }

  /**
   * 从远程 block 获取的结果。
   */
  private[storage] sealed trait FetchResult

  /**
   * 成功从远程 block 获取的结果。
   * @param blockId block id
   * @param mapIndex 此 block 的 mapIndex，表示在 map 阶段的索引。
   * @param address 获取 block 的 BlockManager。
   * @param size block 的估计大小。注意这不是确切的字节数。
   *             远程 block 的大小用于计算 bytesInFlight。
   * @param buf 内容的 `ManagedBuffer`。
   * @param isNetworkReqDone 这是否是此获取请求中此主机的最后一个网络请求。
   */
  private[storage] case class SuccessFetchResult(
      blockId: BlockId,
      mapIndex: Int,
      address: BlockManagerId,
      size: Long,
      buf: ManagedBuffer,
      isNetworkReqDone: Boolean) extends FetchResult {
    require(buf != null)
    require(size >= 0)
  }


  /**
   * 从远程 block 获取失败的结果。
   * @param blockId block id
   * @param mapIndex 此 block 的 mapIndex，表示在 map 阶段的索引
   * @param address 尝试获取 block 的 BlockManager
   * @param e 失败异常
   */
  private[storage] case class FailureFetchResult(
      blockId: BlockId,
      mapIndex: Int,
      address: BlockManagerId,
      e: Throwable)
    extends FetchResult


  /**
   * 由于某些原因应该推迟的获取请求的结果，例如 Netty OOM
   */
  private[storage]
  case class DeferFetchRequestResult(fetchRequest: FetchRequest) extends FetchResult

  /**
   * 以下任一项的不成功获取结果：
   * 1) 远程 shuffle 块。
   * 2) 本地推送合并 block。
   *
   * 我们不将其视为 [[FailureFetchResult]]，而是回退到获取原始 block。
   *
   * @param blockId block id
   * @param address 尝试获取推送合并 block 的 BlockManager
   * @param size block 的大小，用于更新 bytesInFlight。
   * @param isNetworkReqDone 这是否是此获取请求中此主机的最后一个网络请求。用于更新 reqsInFlight。
   */
  private[storage] case class FallbackOnPushMergedFailureResult(blockId: BlockId,
      address: BlockManagerId,
      size: Long,
      isNetworkReqDone: Boolean) extends FetchResult

  /**
   * 成功获取远程推送合并 block 的元信息的结果。
   *
   * @param shuffleId shuffle id。
   * @param shuffleMergeId shuffleMergeId 用于唯一标识不确定阶段尝试的 shuffle 合并过程。
   * @param reduceId reduce id。
   * @param blockSize 每个推送合并 block 的大小。
   * @param bitmaps 每个块的位图。
   * @param address 获取元数据的 BlockManager。
   */
  private[storage] case class PushMergedRemoteMetaFetchResult(
      shuffleId: Int,
      shuffleMergeId: Int,
      reduceId: Int,
      blockSize: Long,
      bitmaps: Array[RoaringBitmap],
      address: BlockManagerId) extends FetchResult

  /**
   * 获取远程推送合并 block 的元信息时失败的结果。
   *
   * @param shuffleId shuffle id。
   * @param shuffleMergeId shuffleMergeId 用于唯一标识不确定阶段尝试的 shuffle 合并过程。
   * @param reduceId reduce id。
   * @param address 获取元数据的 BlockManager。
   */
  private[storage] case class PushMergedRemoteMetaFailedFetchResult(
      shuffleId: Int,
      shuffleMergeId: Int,
      reduceId: Int,
      address: BlockManagerId) extends FetchResult

  /**
   * 成功获取推送合并本地 block 的元信息的结果。
   *
   * @param shuffleId shuffle id。
   * @param shuffleMergeId shuffleMergeId 用于唯一标识不确定阶段尝试的 shuffle 合并过程。
   * @param reduceId reduce id。
   * @param bitmaps 每个块的位图。
   * @param localDirs 存储推送合并 shuffle 文件的本地目录
   */
  private[storage] case class PushMergedLocalMetaFetchResult(
      shuffleId: Int,
      shuffleMergeId: Int,
      reduceId: Int,
      bitmaps: Array[RoaringBitmap],
      localDirs: Array[String]) extends FetchResult
}
