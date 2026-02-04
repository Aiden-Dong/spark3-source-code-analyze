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

package org.apache.spark.shuffle

import org.apache.spark.{Partition, ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus

/**
 * 用于自定义 shuffle 写入过程的接口。
 * 驱动程序创建一个 ShuffleWriteProcessor 并将其放入 [[ShuffleDependency]] 中，执行器在每个 ShuffleMapTask 中使用它。
 */
private[spark] class ShuffleWriteProcessor extends Serializable with Logging {

  /**
   * 从任务上下文创建一个 [[ShuffleWriteMetricsReporter]]。
   * 由于报告器是一个按行操作的算子，这里需要仔细考虑性能问题。
   */
  protected def createMetricsReporter(context: TaskContext): ShuffleWriteMetricsReporter = {
    context.taskMetrics().shuffleWriteMetrics
  }

  /**
   * 特定分区的写入过程，它控制从 [[ShuffleManager]] 获取的 [[ShuffleWriter]]的生命周期
   * 触发 RDD 计算，最终返回此任务的 [[MapStatus]]。
   */
  def write(
      rdd: RDD[_],
      dep: ShuffleDependency[_, _, _],
      mapId: Long,
      context: TaskContext,
      partition: Partition): MapStatus = {
    var writer: ShuffleWriter[Any, Any] = null
    try {
      // 获取ShuffleManager
      val manager = SparkEnv.get.shuffleManager
      // 获取 ShuffleWriter
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, mapId, context, createMetricsReporter(context))
      // ShuffleWriter 写出操作
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      val mapStatus = writer.stop(success = true)

      if (mapStatus.isDefined) {
        // 检查现在是否有足够的 shuffle 合并器可供 ShuffleMapTask 推送
        if (dep.shuffleMergeAllowed && dep.getMergerLocs.isEmpty) {
          val mapOutputTracker = SparkEnv.get.mapOutputTracker
          val mergerLocs = mapOutputTracker.getShufflePushMergerLocations(dep.shuffleId)
          if (mergerLocs.nonEmpty) {
            dep.setMergerLocs(mergerLocs)
          }
        }
        // 如果启用了基于推送的 shuffle，则启动 shuffle 推送过程
        // Map 任务只负责将 shuffle 数据文件转换为多个块推送请求。
        // 它将推送块的工作委托给不同的线程池 - ShuffleBlockPusher.BLOCK_PUSHER_POOL。
        if (!dep.shuffleMergeFinalized) {
          manager.shuffleBlockResolver match {
            case resolver: IndexShuffleBlockResolver =>
              logInfo(s"Shuffle merge enabled with ${dep.getMergerLocs.size} merger locations " +
                s" for stage ${context.stageId()} with shuffle ID ${dep.shuffleId}")
              logDebug(s"Starting pushing blocks for the task ${context.taskAttemptId()}")
              val dataFile = resolver.getDataFile(dep.shuffleId, mapId)
              new ShuffleBlockPusher(SparkEnv.get.conf)
                .initiateBlockPush(dataFile, writer.getPartitionLengths(), dep, partition.index)
            case _ =>
          }
        }
      }
      mapStatus.get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }
}
