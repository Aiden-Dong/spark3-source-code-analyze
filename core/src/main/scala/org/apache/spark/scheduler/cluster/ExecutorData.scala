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

package org.apache.spark.scheduler.cluster

import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef}
import org.apache.spark.scheduler.ExecutorResourceInfo

/**
 * 由 CoarseGrainedSchedulerBackend 使用的 Executor 数据分组。
 *
 * @param executorEndpoint 表示此 Executor 的 RpcEndpointRef
 * @param executorAddress 此Executor的网络地址
 * @param executorHost 此Executor运行的主机名
 * @param freeCores Executor上当前可用的核心数量
 * @param totalCores Executor可用的总核心数量
 * @param resourcesInfo 执行器上当前可用资源的信息
 * @param resourceProfileId 此执行器使用的 ResourceProfile 的 ID
 * @param registrationTs 此执行器的注册时间戳
 */
private[cluster] class ExecutorData(
    val executorEndpoint: RpcEndpointRef,
    val executorAddress: RpcAddress,
    override val executorHost: String,
    var freeCores: Int,
    override val totalCores: Int,
    override val logUrlMap: Map[String, String],
    override val attributes: Map[String, String],
    override val resourcesInfo: Map[String, ExecutorResourceInfo],
    override val resourceProfileId: Int,
    val registrationTs: Long
) extends ExecutorInfo(executorHost, totalCores, logUrlMap, attributes,
  resourcesInfo, resourceProfileId)
