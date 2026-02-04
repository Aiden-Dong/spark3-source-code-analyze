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

import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.Properties

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.rdd.RDD

/**
 * ShuffleMapTask 将 RDD 的元素分割到多个桶中（基于 ShuffleDependency 中指定的分区器）。
 *
 * 更多信息请参见 [[org.apache.spark.scheduler.Task]]。
 *
 * @param stageId 此任务所属阶段的 ID
 * @param stageAttemptId 此任务所属阶段的尝试 ID
 * @param taskBinary RDD 和 ShuffleDependency 的广播版本。反序列化后，
 *                   类型应该是 (RDD[_], ShuffleDependency[_, _, _])。
 * @param partition 此任务关联的 RDD 分区
 * @param locs 用于本地性调度的首选任务执行位置
 * @param localProperties 用户在驱动端设置的线程本地属性的副本
 * @param serializedTaskMetrics 在驱动端创建并序列化的 `TaskMetrics`，发送到执行器端
 *
 * 以下参数是可选的：
 * @param jobId 此任务所属作业的 ID
 * @param appId 此任务所属应用的 ID
 * @param appAttemptId 此任务所属应用的尝试 ID
 * @param isBarrier 此任务是否属于屏障阶段。对于屏障阶段，Spark 必须同时启动所有任务
 */
private[spark] class ShuffleMapTask(
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation],
    localProperties: Properties,
    serializedTaskMetrics: Array[Byte],
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None,
    isBarrier: Boolean = false)
  extends Task[MapStatus](stageId, stageAttemptId, partition.index, localProperties,
    serializedTaskMetrics, jobId, appId, appAttemptId, isBarrier)
  with Logging {

  /** 仅在测试套件中使用的构造函数。这不需要传入 RDD。 */
  def this(partitionId: Int) = {
    this(0, 0, null, new Partition { override def index: Int = 0 }, null, new Properties, null)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.distinct
  }

  override def runTask(context: TaskContext): MapStatus = {
    // 使用广播变量反序列化 RDD
    val threadMXBean = ManagementFactory.getThreadMXBean
    val deserializeStartTimeNs = System.nanoTime()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    val ser = SparkEnv.get.closureSerializer.newInstance()
    // 反序列化RDD
    val rddAndDep = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTimeNs = System.nanoTime() - deserializeStartTimeNs
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L

    val rdd = rddAndDep._1
    val dep = rddAndDep._2
    // 当我们使用旧的 shuffle 获取协议时，在 ShuffleBlockId 构造中使用 partitionId 作为 mapId
    val mapId = if (SparkEnv.get.conf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)) {
      partitionId
    } else context.taskAttemptId()

    // 进行Shuffle 写入操作
    dep.shuffleWriterProcessor.write(rdd, dep, mapId, context, partition)
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}
