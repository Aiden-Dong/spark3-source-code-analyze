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

import java.util.Map;

import org.apache.spark.TaskFailedReason;
import org.apache.spark.annotation.DeveloperApi;

/**
 * :: DeveloperApi ::
 * Executor component of a {@link SparkPlugin}.
 *
 * @since 3.0.0
 */
@DeveloperApi
public interface ExecutorPlugin {

  /**
   * 初始化执行器插件。
   * <p>
   *   当Spark插件提供执行器插件时，此方法将在执行器进程初始化期间被调用。它会阻塞执行器初始化直到返回。
   * <p>
   *   发布指标的执行器插件应该在调用此方法时向上下文的注册表
   *   （{@link PluginContext#metricRegistry()}）注册所有指标。之后注册的指标
   *   不保证会显示。
   *
   * @param ctx 插件运行的执行器的上下文信息。
   * @param extraConf 驱动程序组件在其初始化期间提供的额外配置。
   */
  default void init(PluginContext ctx, Map<String, String> extraConf) {}

  /**
   * Clean up and terminate this plugin.
   * <p>
   * This method is called during the executor shutdown phase, and blocks executor shutdown.
   */
  default void shutdown() {}

  /**
   * Perform any action before the task is run.
   * <p>
   * This method is invoked from the same thread the task will be executed.
   * Task-specific information can be accessed via {@link org.apache.spark.TaskContext#get}.
   * <p>
   * Plugin authors should avoid expensive operations here, as this method will be called
   * on every task, and doing something expensive can significantly slow down a job.
   * It is not recommended for a user to call a remote service, for example.
   * <p>
   * Exceptions thrown from this method do not propagate - they're caught,
   * logged, and suppressed. Therefore exceptions when executing this method won't
   * make the job fail.
   *
   * @since 3.1.0
   */
  default void onTaskStart() {}

  /**
   * Perform an action after tasks completes without exceptions.
   * <p>
   * As {@link #onTaskStart() onTaskStart} exceptions are suppressed, this method
   * will still be invoked even if the corresponding {@link #onTaskStart} call for this
   * task failed.
   * <p>
   * Same warnings of {@link #onTaskStart() onTaskStart} apply here.
   *
   * @since 3.1.0
   */
  default void onTaskSucceeded() {}

  /**
   * Perform an action after tasks completes with exceptions.
   * <p>
   * Same warnings of {@link #onTaskStart() onTaskStart} apply here.
   *
   * @param failureReason the exception thrown from the failed task.
   *
   * @since 3.1.0
   */
  default void onTaskFailed(TaskFailedReason failureReason) {}
}
