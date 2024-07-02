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

import java.util.Properties

/**
 * 一组任务一起提交给底层的任务调度器，通常表示特定阶段的缺失分区。
 */
private[spark] class TaskSet(
    val tasks: Array[Task[_]],      // task 任务集合
    val stageId: Int,               // stage 标识
    val stageAttemptId: Int,        // stage attempt 标识
    val priority: Int,              // 优先级权重
    val properties: Properties,     // 任务属性
    val resourceProfileId: Int) {   // 资源文件描述
  val id: String = stageId + "." + stageAttemptId

  override def toString: String = "TaskSet " + id
}
