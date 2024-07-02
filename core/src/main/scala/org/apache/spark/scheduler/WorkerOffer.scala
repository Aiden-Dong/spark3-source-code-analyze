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

import scala.collection.mutable.Buffer

import org.apache.spark.resource.ResourceProfile

/**
 * 表示在执行器上可用的空闲资源。
 */
private[spark]
case class WorkerOffer(
    executorId: String,    // executor Id
    host: String,          // 主机名
    cores: Int,            // cpu 数量
    address: Option[String] = None,  // `address` 是一个可选的 hostPort 字符串，当在同一个主机上启动多个执行器时，
                                     // 它比 `host` 提供了更多有用的信息。
    resources: Map[String, Buffer[String]] = Map.empty,
    resourceProfileId: Int = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
