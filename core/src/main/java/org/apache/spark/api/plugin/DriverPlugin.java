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

package org.apache.spark.api.plugin;

import java.util.Collections;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.annotation.DeveloperApi;

/**
 * :: DeveloperApi ::
 * Driver component of a {@link SparkPlugin}.
 *
 * @since 3.0.0
 */
@DeveloperApi
public interface DriverPlugin {

  /**
   * 初始化插件。
   * <p>
   *   此方法在Spark驱动程序初始化的早期阶段被调用。具体来说，它在Spark驱动程序的任务调度器初始化之前被调用。
   *   这意味着许多其他Spark子系统可能尚未初始化。此调用也会阻塞驱动程序的初始化过程。
   * <p>
   * 建议插件谨慎处理在此调用中执行的操作，最好在单独的线程中执行昂贵的操作，或者将它们
   * 推迟到应用程序完全启动后再执行。
   *
   * @param sc 加载插件的SparkContext。
   * @param pluginContext 关于运行插件的Spark应用程序的额外插件特定信息。
   * @return 一个映射，将提供给 {@link ExecutorPlugin#init(PluginContext,Map)} 方法。
   */
  default Map<String, String> init(SparkContext sc, PluginContext pluginContext) {
    return Collections.emptyMap();
  }

  /**
   * 向Spark的指标系统注册插件发布的指标。
   * <p>
   * 此方法在Spark应用程序初始化的后期阶段被调用，此时大多数子系统已经启动且应用程序ID
   * 已知。如果在注册表中注册了指标（{@link PluginContext#metricRegistry()}），
   * 则会创建一个以插件名称命名的指标源。
   * <p>
   * 请注意，即使在调用此方法后指标注册表仍然可访问，但在此方法调用后注册新指标可能会
   * 导致这些指标不可用。
   *
   * @param appId 来自集群管理器的应用程序ID。
   * @param pluginContext 关于运行插件的Spark应用程序的额外插件特定信息。
   */
  default void registerMetrics(String appId, PluginContext pluginContext) {}

  /**
   * RPC消息处理器。
   * <p>
   *  插件可以使用Spark的RPC系统从执行器向驱动程序发送消息（但目前不能反向发送）。
   *  插件的执行器组件发送的消息将被传递到此方法，如果执行器请求了回复，返回值将作为回复发送回执行器。
   * <p>
   *   任何抛出的异常都会作为错误发送回执行器（如果它期望回复）。如果不期望回复，
   *   则会在驱动程序日志中写入日志消息。
   * <p>
   * 此处理器的实现应该是线程安全的。
   * <p>
   * 注意所有插件共享RPC分发线程，此方法是同步调用的。因此在此处理器中执行昂贵的
   * 操作可能会影响其他活跃插件的运行。不过内部Spark端点不会直接受到影响，因为
   * 它们使用不同的线程。
   * <p>
   * Spark保证当执行器启动时，驱动程序组件已准备好通过此处理器接收消息。
   *
   * @param message 传入的消息。
   * @return 要返回给调用者的值。如果调用者不期望回复则忽略。
   */

  default Object receive(Object message) throws Exception {
    throw new UnsupportedOperationException();
  }

  /**
   * Informs the plugin that the Spark application is shutting down.
   * <p>
   * This method is called during the driver shutdown phase. It is recommended that plugins
   * not use any Spark functions (e.g. send RPC messages) during this call.
   */
  default void shutdown() {}

}
