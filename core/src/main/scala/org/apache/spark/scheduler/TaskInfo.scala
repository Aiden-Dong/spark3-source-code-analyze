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

import org.apache.spark.TaskState
import org.apache.spark.TaskState.TaskState
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.errors.SparkCoreErrors

/**
 * :: DeveloperApi ::
 * TaskSet内运行任务尝试的信息。
 */
@DeveloperApi
class TaskInfo(
    val taskId: Long,
    /**
     * 这个任务在其任务集中的索引。
     * 不一定与任务正在计算的RDD分区的ID相同。
     */
    val index: Int,
    val attemptNumber: Int,
    /**
     * 这个任务中的实际RDD分区ID。
     * RDD分区的ID在所有任务尝试中始终相同。
     * 对于历史数据，这将为-1，并且自Spark 3.3以来对所有应用程序可用。
     */
    val partitionId: Int,
    val launchTime: Long,
    val executorId: String,
    val host: String,
    val taskLocality: TaskLocality.TaskLocality,
    val speculative: Boolean) {

  /**
   * This api doesn't contains partitionId, please use the new api.
   * Remain it for backward compatibility before Spark 3.3.
   */
  def this(
      taskId: Long,
      index: Int,
      attemptNumber: Int,
      launchTime: Long,
      executorId: String,
      host: String,
      taskLocality: TaskLocality.TaskLocality,
      speculative: Boolean) = {
    this(taskId, index, attemptNumber, -1, launchTime, executorId, host, taskLocality, speculative)
  }

  /**
   * The time when the task started remotely getting the result. Will not be set if the
   * task result was sent immediately when the task finished (as opposed to sending an
   * IndirectTaskResult and later fetching the result from the block manager).
   */
  var gettingResultTime: Long = 0

  /**
   * Intermediate updates to accumulables during this task. Note that it is valid for the same
   * accumulable to be updated multiple times in a single task or for two accumulables with the
   * same name but different IDs to exist in a task.
   */
  def accumulables: Seq[AccumulableInfo] = _accumulables

  private[this] var _accumulables: Seq[AccumulableInfo] = Nil

  private[spark] def setAccumulables(newAccumulables: Seq[AccumulableInfo]): Unit = {
    _accumulables = newAccumulables
  }

  /**
   * The time when the task has completed successfully (including the time to remotely fetch
   * results, if necessary).
   */
  var finishTime: Long = 0

  var failed = false

  var killed = false

  private[spark] def markGettingResult(time: Long): Unit = {
    gettingResultTime = time
  }

  private[spark] def markFinished(state: TaskState, time: Long): Unit = {
    // finishTime should be set larger than 0, otherwise "finished" below will return false.
    assert(time > 0)
    finishTime = time
    if (state == TaskState.FAILED) {
      failed = true
    } else if (state == TaskState.KILLED) {
      killed = true
    }
  }

  def gettingResult: Boolean = gettingResultTime != 0

  def finished: Boolean = finishTime != 0

  def successful: Boolean = finished && !failed && !killed

  def running: Boolean = !finished

  def status: String = {
    if (running) {
      if (gettingResult) {
        "GET RESULT"
      } else {
        "RUNNING"
      }
    } else if (failed) {
      "FAILED"
    } else if (killed) {
      "KILLED"
    } else if (successful) {
      "SUCCESS"
    } else {
      "UNKNOWN"
    }
  }

  def id: String = s"$index.$attemptNumber"

  def duration: Long = {
    if (!finished) {
      throw SparkCoreErrors.durationCalledOnUnfinishedTaskError()
    } else {
      finishTime - launchTime
    }
  }

  private[spark] def timeRunning(currentTime: Long): Long = currentTime - launchTime
}
