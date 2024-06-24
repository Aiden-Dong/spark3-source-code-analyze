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

package org.apache.spark

import java.util.concurrent.ScheduledFuture

import scala.reflect.ClassTag

import org.roaringbitmap.RoaringBitmap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleWriteProcessor}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * Base class for dependencies.
 */
@DeveloperApi
abstract class Dependency[T] extends Serializable {
  def rdd: RDD[T]
}


/**
 * :: DeveloperApi ::
 * 这是一个基类，用于表示子RDD的每个分区都依赖于父RDD的少数分区的依赖关系。
 * 窄依赖关系允许进行流水线式执行。
 */
@DeveloperApi
abstract class NarrowDependency[T](_rdd: RDD[T]) extends Dependency[T] {
  /**
   * Get the parent partitions for a child partition.
   * @param partitionId a partition of the child RDD
   * @return the partitions of the parent RDD that the child partition depends upon
   */
  def getParents(partitionId: Int): Seq[Int]

  override def rdd: RDD[T] = _rdd
}


/**
 * :: DeveloperApi ::
 *  表示对Shuffle阶段输出的依赖关系。
 *  需要注意，在进行shuffle的情况下，RDD是临时的，因为我们在executor端不需要它
 *
 * @param _rdd 上游依赖的 RDD
 * @param partitioner 用于进行shuffle分区的分区器
 * @param serializer rdd 数据序列化工具 spark.serializer 参数可以用于指定，如果不指定，将使用默认序列化器
 * @param keyOrdering 用于对key进行排序的比较器
 * @param aggregator map/reduce 端聚合器
 * @param mapSideCombine 是否进行map端聚合
 */
@DeveloperApi
class ShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient private val _rdd: RDD[_ <: Product2[K, V]],    // 上游的RDD
    val partitioner: Partitioner,                             // 上游RDD算完后的分区方式
    val serializer: Serializer = SparkEnv.get.serializer,     // RDD 的序列化工具
    val keyOrdering: Option[Ordering[K]] = None,
    val aggregator: Option[Aggregator[K, V, C]] = None,
    val mapSideCombine: Boolean = false,
    val shuffleWriterProcessor: ShuffleWriteProcessor = new ShuffleWriteProcessor)
  extends Dependency[Product2[K, V]] with Logging {

  if (mapSideCombine) {
    require(aggregator.isDefined, "Map-side combine without Aggregator specified!")
  }
  override def rdd: RDD[Product2[K, V]] = _rdd.asInstanceOf[RDD[Product2[K, V]]]

  private[spark] val keyClassName: String = reflect.classTag[K].runtimeClass.getName
  private[spark] val valueClassName: String = reflect.classTag[V].runtimeClass.getName
  // Note: It's possible that the combiner class tag is null, if the combineByKey
  // methods in PairRDDFunctions are used instead of combineByKeyWithClassTag.
  private[spark] val combinerClassName: Option[String] =
    Option(reflect.classTag[C]).map(_.runtimeClass.getName)

  // 生成一个ShuffleId
  val shuffleId: Int = _rdd.context.newShuffleId()

  // shufffle 操作句柄信息
  val shuffleHandle: ShuffleHandle = _rdd.context.env.shuffleManager.registerShuffle(
    shuffleId, this)

  private[this] val numPartitions = rdd.partitions.length

  // 默认情况下，如果启用了推送式洗牌，则允许对ShuffleDependency进行洗牌合并。
  private[this] var _shuffleMergeAllowed = canShuffleMergeBeEnabled()

  private[spark] def setShuffleMergeAllowed(shuffleMergeAllowed: Boolean): Unit = {
    _shuffleMergeAllowed = shuffleMergeAllowed
  }

  def shuffleMergeEnabled : Boolean = shuffleMergeAllowed && mergerLocs.nonEmpty

  def shuffleMergeAllowed : Boolean = _shuffleMergeAllowed

  /**
   * 此配置项存储选定的外部shuffle服务列表的位置，
   * 用于处理此shuffle map阶段中来自mapper的shuffle merge请求。
   */
  private[spark] var mergerLocs: Seq[BlockManagerId] = Nil

  /**
   * Stores the information about whether the shuffle merge is finalized for the shuffle map stage
   * associated with this shuffle dependency
   */
  private[this] var _shuffleMergeFinalized: Boolean = false

  /**
   *shuffleMergeId 用于唯一标识由一个非确定阶段尝试的混洗合并过程。
   */
  private[this] var _shuffleMergeId: Int = 0

  def shuffleMergeId: Int = _shuffleMergeId

  def setMergerLocs(mergerLocs: Seq[BlockManagerId]): Unit = {
    assert(shuffleMergeAllowed)
    this.mergerLocs = mergerLocs
  }

  def getMergerLocs: Seq[BlockManagerId] = mergerLocs

  private[spark] def markShuffleMergeFinalized(): Unit = {
    _shuffleMergeFinalized = true
  }

  private[spark] def isShuffleMergeFinalizedMarked: Boolean = {
    _shuffleMergeFinalized
  }

  /**
   * Returns true if push-based shuffle is disabled or if the shuffle merge for
   * this shuffle is finalized.
   */
  def shuffleMergeFinalized: Boolean = {
    if (shuffleMergeEnabled) {
      isShuffleMergeFinalizedMarked
    } else {
      true
    }
  }

  def newShuffleMergeState(): Unit = {
    _shuffleMergeFinalized = false
    mergerLocs = Nil
    _shuffleMergeId += 1
    finalizeTask = None
    shufflePushCompleted.clear()
  }

  private def canShuffleMergeBeEnabled(): Boolean = {
    val isPushShuffleEnabled = Utils.isPushBasedShuffleEnabled(rdd.sparkContext.getConf,
      // invoked at driver
      isDriver = true)
    if (isPushShuffleEnabled && rdd.isBarrier()) {
      logWarning("Push-based shuffle is currently not supported for barrier stages")
    }
    isPushShuffleEnabled && numPartitions > 0 &&
      // TODO: SPARK-35547: Push based shuffle is currently unsupported for Barrier stages
      !rdd.isBarrier()
  }

  @transient private[this] val shufflePushCompleted = new RoaringBitmap()

  /**
   * Mark a given map task as push completed in the tracking bitmap.
   * Using the bitmap ensures that the same map task launched multiple times due to
   * either speculation or stage retry is only counted once.
   * @param mapIndex Map task index
   * @return number of map tasks with block push completed
   */
  private[spark] def incPushCompleted(mapIndex: Int): Int = {
    shufflePushCompleted.add(mapIndex)
    shufflePushCompleted.getCardinality
  }

  // Only used by DAGScheduler to coordinate shuffle merge finalization
  @transient private[this] var finalizeTask: Option[ScheduledFuture[_]] = None

  private[spark] def getFinalizeTask: Option[ScheduledFuture[_]] = finalizeTask

  private[spark] def setFinalizeTask(task: ScheduledFuture[_]): Unit = {
    finalizeTask = Option(task)
  }

  _rdd.sparkContext.cleaner.foreach(_.registerShuffleForCleanup(this))
  _rdd.sparkContext.shuffleDriverComponents.registerShuffle(shuffleId)
}



/**
 * :: DeveloperApi :: 像类似map/flatmap相关的依赖关系
 * 表示父RDD和子RDD之间分区之间的一对一依赖关系。.
 */
@DeveloperApi
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int): List[Int] = List(partitionId)
}


/**
 * :: DeveloperApi ::
 * 表示父RDD和子RDD之间分区范围之间的一对一依赖关系, 再Union 场景中使用
 * @param rdd 依赖的RDD
 * @param inStart 在父RDD中范围的起始位置。
 * @param outStart 在子RDD中范围的起始位置。
 * @param length the length of the range
 */
@DeveloperApi
class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
  extends NarrowDependency[T](rdd) {

  override def getParents(partitionId: Int): List[Int] = {
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
  }
}
