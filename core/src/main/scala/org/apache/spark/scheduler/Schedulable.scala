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

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * 调度实体的接口。
 * 有两种类型的调度实体(Pool和TasksetManager)。
 */
private[spark] trait Schedulable {
  var parent: Pool
  // child queues
  def schedulableQueue: ConcurrentLinkedQueue[Schedulable]
  def schedulingMode: SchedulingMode
  def weight: Int         // 权重
  def minShare: Int
  def runningTasks: Int   // 运行的任务数
  def priority: Int       // 优先级
  def stageId: Int
  def name: String

  def isSchedulable: Boolean
  def addSchedulable(schedulable: Schedulable): Unit
  def removeSchedulable(schedulable: Schedulable): Unit
  def getSchedulableByName(name: String): Schedulable
  // Executor 丢失
  def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Unit
  // Executor 处于 Decommision状态
  def executorDecommission(executorId: String): Unit
  def checkSpeculatableTasks(minTimeToSpeculation: Long): Boolean
  def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager]
}
