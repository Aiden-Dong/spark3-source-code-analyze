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

import org.apache.spark.resource.{ResourceAllocator, ResourceInformation}

/**
 * 用于保存Executor上某种资源信息的类。此信息由 SchedulerBackend 管理，
 * TaskScheduler 将根据这些信息在空闲的执行器上调度任务。
 * @param name 资源名称
 * @param addresses 执行器提供的资源地址
 * @param numParts 调度任务时每种资源的细分方式数量
 */
private[spark] class ExecutorResourceInfo(
    name: String,
    addresses: Seq[String],
    numParts: Int)
  extends ResourceInformation(name, addresses.toArray) with ResourceAllocator {

  override protected def resourceName = this.name
  override protected def resourceAddresses = this.addresses
  override protected def slotsPerAddress: Int = numParts
  def totalAddressAmount: Int = resourceAddresses.length * slotsPerAddress
}
