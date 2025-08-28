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

package org.apache.spark.sql.execution.adaptive

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{MapOutputStatistics, MapOutputTrackerMaster, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.{CoalescedPartitionSpec, PartialReducerPartitionSpec, ShufflePartitionSpec}

object ShufflePartitionsUtil extends Logging {
  final val SMALL_PARTITION_FACTOR = 0.2
  final val MERGED_PARTITION_FACTOR = 1.2

  /**
   * 合并多个 shuffle 的分区，可以是它们的原始状态，或应用了倾斜处理的分区规格。
   * 如果在包含倾斜分区规格的分区上调用此方法，它将保留倾斜分区规格不变，
   * 仅合并倾斜部分之外的分区。
   *
   * 如果 shuffle 已经合并，或它们没有相同的分区数量，或合并结果与输入分区布局相同，
   * 则此方法将返回空结果。
   *
   * @return [[ShufflePartitionSpec]] 的序列，每个内序列作为合并后相应 shuffle 的新分区规格。
   *         如果返回 Nil，则没有应用合并。
   */
  def coalescePartitions(
      mapOutputStatistics: Seq[Option[MapOutputStatistics]],          // 每个shuffle stage的输出统计信息序列

      inputPartitionSpecs: Seq[Option[Seq[ShufflePartitionSpec]]],    // • 如果为None或空序列，表示使用原始分区（没有经过倾斜处理）
                                                                      // • 如果有值，表示已经应用了倾斜连接优化，包含PartialReducerPartitionSpec等特殊分区规格

      advisoryTargetSize: Long,                                       // 目的调整大小
      minNumPartitions: Int,                                          // 最小分区值
      minPartitionSize: Long                                          // 最小分区大小
  ): Seq[Seq[ShufflePartitionSpec]] = {
    assert(mapOutputStatistics.length == inputPartitionSpecs.length)

    if (mapOutputStatistics.isEmpty) {
      return Seq.empty
    }

    // 计算出当前并联stage 的总的输入数据量
    val totalPostShuffleInputSize = mapOutputStatistics.flatMap(_.map(_.bytesByPartitionId.sum)).sum

    // targetSize ： 每个分区的目标输入数据量
    val maxTargetSize = math.ceil(totalPostShuffleInputSize / minNumPartitions.toDouble).toLong
    val targetSize = maxTargetSize.min(advisoryTargetSize).max(minPartitionSize)

    // 获取每个任务的 shuffleId
    val shuffleIds = mapOutputStatistics.flatMap(_.map(_.shuffleId)).mkString(", ")

    logInfo(s"For shuffle($shuffleIds), advisory target size: $advisoryTargetSize, " +
      s"actual target size $targetSize, minimum partition size: $minPartitionSize")

    // “如果 inputPartitionSpecs 都为空，则表示没有应用倾斜连接优化。”
    if (inputPartitionSpecs.forall(_.isEmpty)) {
      coalescePartitionsWithoutSkew(mapOutputStatistics, targetSize, minPartitionSize)
    } else {
      coalescePartitionsWithSkew(mapOutputStatistics, inputPartitionSpecs, targetSize, minPartitionSize)
    }
  }

  private def coalescePartitionsWithoutSkew(
      mapOutputStatistics: Seq[Option[MapOutputStatistics]],
      targetSize: Long,
      minPartitionSize: Long): Seq[Seq[ShufflePartitionSpec]] = {

    // 当输入 RDD 有 0 个分区时，`ShuffleQueryStageExec#mapStats` 返回 None，
    // 计算 `partitionStartIndices` 时应跳过它。
    val validMetrics = mapOutputStatistics.flatten

    // 获取shuffle 数据量
    val numShuffles = mapOutputStatistics.length

    // If all input RDDs have 0 partition, we create an empty partition for every shuffle read.
    if (validMetrics.isEmpty) {
      return Seq.fill(numShuffles)(Seq(CoalescedPartitionSpec(0, 0, 0)))
    }

    // We may have different pre-shuffle partition numbers, don't reduce shuffle partition number
    // in that case. For example when we union fully aggregated data (data is arranged to a single
    // partition) and a result of a SortMergeJoin (multiple partitions).
    if (validMetrics.map(_.bytesByPartitionId.length).distinct.length > 1) {
      return Seq.empty
    }

    // 获取当前的分区数量
    val numPartitions = validMetrics.head.bytesByPartitionId.length

    // 合并目标分区
    val newPartitionSpecs = coalescePartitions(0, numPartitions, validMetrics, targetSize, minPartitionSize)

    if (newPartitionSpecs.length < numPartitions) {
      attachDataSize(mapOutputStatistics, newPartitionSpecs)
    } else {
      Seq.empty
    }
  }

  private def coalescePartitionsWithSkew(
      mapOutputStatistics: Seq[Option[MapOutputStatistics]],
      inputPartitionSpecs: Seq[Option[Seq[ShufflePartitionSpec]]],
      targetSize: Long,
      minPartitionSize: Long): Seq[Seq[ShufflePartitionSpec]] = {
    // Do not coalesce if any of the map output stats are missing or if not all shuffles have
    // partition specs, which should not happen in practice.
    if (!mapOutputStatistics.forall(_.isDefined) || !inputPartitionSpecs.forall(_.isDefined)) {
      logWarning("Could not apply partition coalescing because of missing MapOutputStatistics " +
        "or shuffle partition specs.")
      return Seq.empty
    }

    val validMetrics = mapOutputStatistics.map(_.get)
    // Extract the start indices of each partition spec. Give invalid index -1 to unexpected
    // partition specs. When we reach here, it means skew join optimization has been applied.
    val partitionIndicesSeq = inputPartitionSpecs.map(_.get.map {
      case CoalescedPartitionSpec(start, end, _) if start + 1 == end => start
      case PartialReducerPartitionSpec(reducerId, _, _, _) => reducerId
      case _ => -1 // invalid
    })

    // There should be no unexpected partition specs and the start indices should be identical
    // across all different shuffles.
    assert(partitionIndicesSeq.distinct.length == 1 && partitionIndicesSeq.head.forall(_ >= 0),
      s"Invalid shuffle partition specs: $inputPartitionSpecs")

    // The indices may look like [0, 1, 2, 2, 2, 3, 4, 4, 5], and the repeated `2` and `4` mean
    // skewed partitions.
    val partitionIndices = partitionIndicesSeq.head
    // The fist index must be 0.
    assert(partitionIndices.head == 0)
    val newPartitionSpecsSeq = Seq.fill(mapOutputStatistics.length)(
      ArrayBuffer.empty[ShufflePartitionSpec])
    val numPartitions = partitionIndices.length
    var i = 1
    var start = 0
    while (i < numPartitions) {
      if (partitionIndices(i - 1) == partitionIndices(i)) {
        // a skew section detected, starting from partition(i - 1).
        val repeatValue = partitionIndices(i)
        // coalesce any partitions before partition(i - 1) and after the end of latest skew section.
        if (i - 1 > start) {
          val partitionSpecs = coalescePartitions(
            partitionIndices(start),
            repeatValue,
            validMetrics,
            targetSize,
            minPartitionSize,
            allowReturnEmpty = true)
          newPartitionSpecsSeq.zip(attachDataSize(mapOutputStatistics, partitionSpecs))
            .foreach(spec => spec._1 ++= spec._2)
        }
        // find the end of this skew section, skipping partition(i - 1) and partition(i).
        var repeatIndex = i + 1
        while (repeatIndex < numPartitions && partitionIndices(repeatIndex) == repeatValue) {
          repeatIndex += 1
        }
        // copy the partition specs in the skew section to the new partition specs.
        newPartitionSpecsSeq.zip(inputPartitionSpecs).foreach { case (newSpecs, oldSpecs) =>
          newSpecs ++= oldSpecs.get.slice(i - 1, repeatIndex)
        }
        // start from after the skew section
        start = repeatIndex
        i = repeatIndex
      } else {
        // Indices outside of the skew section should be larger than the previous one by 1.
        assert(partitionIndices(i - 1) + 1 == partitionIndices(i))
        // no skew section detected, advance to the next index.
        i += 1
      }
    }
    // coalesce any partitions after the end of last skew section.
    if (numPartitions > start) {
      val partitionSpecs = coalescePartitions(
        partitionIndices(start),
        partitionIndices.last + 1,
        validMetrics,
        targetSize,
        minPartitionSize,
        allowReturnEmpty = true)
      newPartitionSpecsSeq.zip(attachDataSize(mapOutputStatistics, partitionSpecs))
        .foreach(spec => spec._1 ++= spec._2)
    }
    // only return coalesced result if any coalescing has happened.
    if (newPartitionSpecsSeq.head.length < numPartitions) {
      newPartitionSpecsSeq.map(_.toSeq)
    } else {
      Seq.empty
    }
  }

  /**
   * 合并多个 shuffle 的 [start, end) 分区。该方法假设所有 shuffle 都具有相同数量的分区，
   * 并且同一索引的分区将由一个任务一起读取。
   *
   * 用于确定合并分区数量的策略如下所述。为了确定合并分区的数量，我们设定了一个合并分区的目标大小。
   * 一旦我们获得了所有 shuffle 分区的大小统计信息，我们将对这些统计信息进行遍历，
   * 并将连续索引的 shuffle 分区打包到一个合并分区中，直到添加另一个 shuffle 分区会导致合并分区的大小
   * 大于目标大小。
   *
   * 例如，我们有两个 shuffle，具有以下分区大小统计信息：
   *  - shuffle 1 (5 个分区): [100 MiB, 20 MiB, 100 MiB, 10 MiB, 30 MiB]
   *  - shuffle 2 (5 个分区): [10 MiB, 10 MiB, 70 MiB, 5 MiB, 5 MiB]
   * 假设目标大小为 128 MiB，我们将有 4 个合并分区，分别是：
   *  - 合并分区 0：shuffle 分区 0 (大小 110 MiB)
   *  - 合并分区 1：shuffle 分区 1 (大小 30 MiB)
   *  - 合并分区 2：shuffle 分区 2 (大小 170 MiB)
   *  - 合并分区 3：shuffle 分区 3 和 4 (大小 50 MiB)
   *
   * @return [[CoalescedPartitionSpec]] 的序列。例如，如果分区 [0, 1, 2, 3, 4]
   *          在索引 [0, 2, 3] 处分割，返回的分区规格将是：
   *          CoalescedPartitionSpec(0, 2), CoalescedPartitionSpec(2, 3) 和
   *          CoalescedPartitionSpec(3, 5)。
   */
  private def coalescePartitions(
      start: Int,
      end: Int,
      mapOutputStatistics: Seq[MapOutputStatistics],
      targetSize: Long,
      minPartitionSize: Long,
      allowReturnEmpty: Boolean = false): Seq[CoalescedPartitionSpec] = {
    // `minPartitionSize` is useful for cases like [64MB, 0.5MB, 64MB]: we can't do coalesce,
    // because merging 0.5MB to either the left or right partition will exceed the target size.
    // If 0.5MB is smaller than `minPartitionSize`, we will force-merge it to the left/right side.
    val partitionSpecs = ArrayBuffer.empty[CoalescedPartitionSpec]
    var coalescedSize = 0L
    var i = start
    var latestSplitPoint = i
    var latestPartitionSize = 0L

    def createPartitionSpec(forceCreate: Boolean = false): Unit = {
      // Skip empty inputs, as it is a waste to launch an empty task.
      if (coalescedSize > 0 || forceCreate) {
        partitionSpecs += CoalescedPartitionSpec(latestSplitPoint, i)
      }
    }

    while (i < end) {
      // We calculate the total size of i-th shuffle partitions from all shuffles.
      var totalSizeOfCurrentPartition = 0L
      var j = 0
      while (j < mapOutputStatistics.length) {
        totalSizeOfCurrentPartition += mapOutputStatistics(j).bytesByPartitionId(i)
        j += 1
      }

      // If including the `totalSizeOfCurrentPartition` would exceed the target size and the
      // current size has reached the `minPartitionSize`, then start a new coalesced partition.
      if (i > latestSplitPoint && coalescedSize + totalSizeOfCurrentPartition > targetSize) {
        if (coalescedSize < minPartitionSize) {
          // the current partition size is below `minPartitionSize`.
          // pack it with the smaller one between the two adjacent partitions (before and after).
          if (latestPartitionSize > 0 && latestPartitionSize < totalSizeOfCurrentPartition) {
            // pack with the before partition.
            partitionSpecs(partitionSpecs.length - 1) =
              CoalescedPartitionSpec(partitionSpecs.last.startReducerIndex, i)
            latestSplitPoint = i
            latestPartitionSize += coalescedSize
            // reset postShuffleInputSize.
            coalescedSize = totalSizeOfCurrentPartition
          } else {
            // pack with the after partition.
            coalescedSize += totalSizeOfCurrentPartition
          }
        } else {
          createPartitionSpec()
          latestSplitPoint = i
          latestPartitionSize = coalescedSize
          // reset postShuffleInputSize.
          coalescedSize = totalSizeOfCurrentPartition
        }
      } else {
        coalescedSize += totalSizeOfCurrentPartition
      }
      i += 1
    }

    if (coalescedSize < minPartitionSize && latestPartitionSize > 0) {
      // pack with the last partition.
      partitionSpecs(partitionSpecs.length - 1) =
        CoalescedPartitionSpec(partitionSpecs.last.startReducerIndex, end)
    } else {
      // If do not allowReturnEmpty, create at least one partition if all partitions are empty.
      createPartitionSpec(!allowReturnEmpty && partitionSpecs.isEmpty)
    }
    partitionSpecs.toSeq
  }

  private def attachDataSize(
      mapOutputStatistics: Seq[Option[MapOutputStatistics]],
      partitionSpecs: Seq[CoalescedPartitionSpec]): Seq[Seq[CoalescedPartitionSpec]] = {
    mapOutputStatistics.map {
      case Some(mapStats) =>
        partitionSpecs.map { spec =>
          val dataSize = spec.startReducerIndex.until(spec.endReducerIndex)
            .map(mapStats.bytesByPartitionId).sum
          spec.copy(dataSize = Some(dataSize))
        }.toSeq
      case None => partitionSpecs.map(_.copy(dataSize = Some(0))).toSeq
    }.toSeq
  }

  /**
   * Given a list of size, return an array of indices to split the list into multiple partitions,
   * so that the size sum of each partition is close to the target size. Each index indicates the
   * start of a partition.
   */
  // Visible for testing
  private[sql] def splitSizeListByTargetSize(
      sizes: Array[Long],
      targetSize: Long,
      smallPartitionFactor: Double): Array[Int] = {
    val partitionStartIndices = ArrayBuffer[Int]()
    partitionStartIndices += 0
    var i = 0
    var currentPartitionSize = 0L
    var lastPartitionSize = -1L

    def tryMergePartitions() = {
      // When we are going to start a new partition, it's possible that the current partition or
      // the previous partition is very small and it's better to merge the current partition into
      // the previous partition.
      val shouldMergePartitions = lastPartitionSize > -1 &&
        ((currentPartitionSize + lastPartitionSize) < targetSize * MERGED_PARTITION_FACTOR ||
        (currentPartitionSize < targetSize * smallPartitionFactor ||
          lastPartitionSize < targetSize * smallPartitionFactor))
      if (shouldMergePartitions) {
        // We decide to merge the current partition into the previous one, so the start index of
        // the current partition should be removed.
        partitionStartIndices.remove(partitionStartIndices.length - 1)
        lastPartitionSize += currentPartitionSize
      } else {
        lastPartitionSize = currentPartitionSize
      }
    }

    while (i < sizes.length) {
      // If including the next size in the current partition exceeds the target size, package the
      // current partition and start a new partition.
      if (i > 0 && currentPartitionSize + sizes(i) > targetSize) {
        tryMergePartitions()
        partitionStartIndices += i
        currentPartitionSize = sizes(i)
      } else {
        currentPartitionSize += sizes(i)
      }
      i += 1
    }
    tryMergePartitions()
    partitionStartIndices.toArray
  }

  /*******
   * 获取特定shuffle和reduce ID对应的map输出大小。
   * 需要注意的是，由于某些问题（如执行器丢失），部分map输出可能缺失。
   * 对于缺失的map输出，其大小将显示为-1，调用方需要自行处理这种情况
   */
  private def getMapSizesForReduceId(shuffleId: Int, partitionId: Int): Array[Long] = {
    val mapOutputTracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    mapOutputTracker.shuffleStatuses(shuffleId).withMapStatuses(_.map { stat =>
      if (stat == null) -1 else stat.getSizeForBlock(partitionId)
    })
  }

  /**
   * Splits the skewed partition based on the map size and the target partition size
   * after split, and create a list of `PartialReducerPartitionSpec`. Returns None if can't split.
   */
  def createSkewPartitionSpecs(
      shuffleId: Int,                                    // Shuffle ID
      reducerId: Int,                                    // 要分割的 Reduce 分区 ID (分区 ID)
      targetSize: Long,                                  // 目标分割大小
      smallPartitionFactor: Double = SMALL_PARTITION_FACTOR)
  : Option[Seq[PartialReducerPartitionSpec]] = {

    // 步骤1：获取 Map 输出大小统计
    // 每个 Shuffle 分区的数据来自多个 Map 任务的输出
    // getMapSizesForReduceId 获取所有 Map 任务对特定 Reducer 分区的输出大小
    // 如果有 Map 输出丢失（-1），则无法进行分割
    val mapPartitionSizes = getMapSizesForReduceId(shuffleId, reducerId)
    if (mapPartitionSizes.exists(_ < 0)) return None

    // 步骤2：计算分割点
    val mapStartIndices = splitSizeListByTargetSize(
      mapPartitionSizes, targetSize, smallPartitionFactor)


    // 步骤3：生成 PartialReducerPartitionSpec
    if (mapStartIndices.length > 1) {
      Some(mapStartIndices.indices.map { i =>
        val startMapIndex = mapStartIndices(i)
        val endMapIndex = if (i == mapStartIndices.length - 1) {
          mapPartitionSizes.length
        } else {
          mapStartIndices(i + 1)
        }
        var dataSize = 0L
        var mapIndex = startMapIndex
        while (mapIndex < endMapIndex) {
          dataSize += mapPartitionSizes(mapIndex)
          mapIndex += 1
        }
        PartialReducerPartitionSpec(reducerId, startMapIndex, endMapIndex, dataSize)
      })
    } else {
      None
    }
  }
}
