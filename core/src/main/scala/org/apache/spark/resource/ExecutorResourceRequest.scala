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

package org.apache.spark.resource

import org.apache.spark.annotation.{Evolving, Since}

/**
 * Executor 资源请求。这与 [[ResourceProfile]] 结合使用，以编程方式指定将在阶段级别应用的 RDD 所需的资源。
 *
 * 这用于指定Executor的资源需求以及 Spark 如何获取这些资源的具体细节。并非所有参数都适用于每种资源类型。
 * 支持 GPU 等资源，并且具有与使用全局 Spark 配置 spark.executor.resource.gpu.* 相同的限制。
 * 用户通过配置指定的资源数量、发现脚本和供应商参数与全局配置参数相同：spark.executor.resource.{resourceName}.{amount, discoveryScript, vendor}。
 *
 * 例如，用户希望在 YARN 上分配带有 GPU 资源的执行器。用户需要指定资源名称（gpu），每个执行器的 GPU 数量，
 * 还需要指定发现脚本，以便执行器启动时可以发现可用的 GPU 地址，因为 YARN 不会告诉 Spark 这些信息。
 * 供应商参数不需要使用，因为它特定于 Kubernetes。
 *
 * 有关更多详细信息，请参阅配置和集群特定的文档。
 *
 * 使用 [[ExecutorResourceRequests]] 类作为便捷的 API。
 *
 * @param resourceName 资源名称
 * @param amount 请求的数量
 * @param discoveryScript 可选的脚本，用于发现资源。在某些集群管理器上这是必需的，因为它们不会告诉 Spark 分配资源的地址。
 *                        该脚本在执行器启动时运行，以发现可用资源的地址。
 * @param vendor 可选的供应商参数，对某些集群管理器是必需的。
 */
@Evolving
@Since("3.1.0")
class ExecutorResourceRequest(
    val resourceName: String,
    val amount: Long,
    val discoveryScript: String = "",
    val vendor: String = "") extends Serializable {

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ExecutorResourceRequest =>
        that.getClass == this.getClass &&
          that.resourceName == resourceName && that.amount == amount &&
        that.discoveryScript == discoveryScript && that.vendor == vendor
      case _ =>
        false
    }
  }

  override def hashCode(): Int =
    Seq(resourceName, amount, discoveryScript, vendor).hashCode()

  override def toString(): String = {
    s"name: $resourceName, amount: $amount, script: $discoveryScript, vendor: $vendor"
  }
}
