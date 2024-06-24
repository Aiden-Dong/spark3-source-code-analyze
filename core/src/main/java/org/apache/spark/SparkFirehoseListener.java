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

package org.apache.spark;

import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.scheduler.*;

/**
 * 允许用户接收所有 SparkListener 事件的类。用户应该重写 onEvent 方法。
 * 这是一个具体的 Java 类，以确保在向 SparkListener 添加新方法时不会忘记更新它：
 * 忘记添加方法会导致编译错误（如果这是一个具体的 Scala 类，新事件处理程序的默认实现将从 SparkListener 特性继承）。
 *
 * 请注意，直到 Spark 3.1.0，这个类缺少 DevelopApi 注解，如果在主要发布之前更改此 API，需要考虑这一点。
 */
@DeveloperApi
public class SparkFirehoseListener implements SparkListenerInterface {

  public void onEvent(SparkListenerEvent event) { }

  @Override
  public final void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
    onEvent(stageCompleted);
  }

  @Override
  public final void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
    onEvent(stageSubmitted);
  }

  @Override
  public final void onTaskStart(SparkListenerTaskStart taskStart) {
    onEvent(taskStart);
  }

  @Override
  public final void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult) {
    onEvent(taskGettingResult);
  }

  @Override
  public final void onTaskEnd(SparkListenerTaskEnd taskEnd) {
    onEvent(taskEnd);
  }

  @Override
  public final void onJobStart(SparkListenerJobStart jobStart) {
    onEvent(jobStart);
  }

  @Override
  public final void onJobEnd(SparkListenerJobEnd jobEnd) {
    onEvent(jobEnd);
  }

  @Override
  public final void onEnvironmentUpdate(SparkListenerEnvironmentUpdate environmentUpdate) {
    onEvent(environmentUpdate);
  }

  @Override
  public final void onBlockManagerAdded(SparkListenerBlockManagerAdded blockManagerAdded) {
    onEvent(blockManagerAdded);
  }

  @Override
  public final void onBlockManagerRemoved(SparkListenerBlockManagerRemoved blockManagerRemoved) {
    onEvent(blockManagerRemoved);
  }

  @Override
  public final void onUnpersistRDD(SparkListenerUnpersistRDD unpersistRDD) {
    onEvent(unpersistRDD);
  }

  @Override
  public final void onApplicationStart(SparkListenerApplicationStart applicationStart) {
    onEvent(applicationStart);
  }

  @Override
  public final void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    onEvent(applicationEnd);
  }

  @Override
  public final void onExecutorMetricsUpdate(
      SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {
    onEvent(executorMetricsUpdate);
  }

  @Override
  public final void onStageExecutorMetrics(
      SparkListenerStageExecutorMetrics executorMetrics) {
    onEvent(executorMetrics);
  }

  @Override
  public final void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
    onEvent(executorAdded);
  }

  @Override
  public final void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
    onEvent(executorRemoved);
  }

  @Override
  public final void onExecutorBlacklisted(SparkListenerExecutorBlacklisted executorBlacklisted) {
    onEvent(executorBlacklisted);
  }

  @Override
  public final void onExecutorExcluded(SparkListenerExecutorExcluded executorExcluded) {
    onEvent(executorExcluded);
  }

  @Override
  public void onExecutorBlacklistedForStage(
      SparkListenerExecutorBlacklistedForStage executorBlacklistedForStage) {
    onEvent(executorBlacklistedForStage);
  }

  @Override
  public void onExecutorExcludedForStage(
      SparkListenerExecutorExcludedForStage executorExcludedForStage) {
    onEvent(executorExcludedForStage);
  }

  @Override
  public void onNodeBlacklistedForStage(
      SparkListenerNodeBlacklistedForStage nodeBlacklistedForStage) {
    onEvent(nodeBlacklistedForStage);
  }

  @Override
  public void onNodeExcludedForStage(
      SparkListenerNodeExcludedForStage nodeExcludedForStage) {
    onEvent(nodeExcludedForStage);
  }

  @Override
  public final void onExecutorUnblacklisted(
      SparkListenerExecutorUnblacklisted executorUnblacklisted) {
    onEvent(executorUnblacklisted);
  }

  @Override
  public final void onExecutorUnexcluded(
      SparkListenerExecutorUnexcluded executorUnexcluded) {
    onEvent(executorUnexcluded);
  }

  @Override
  public final void onNodeBlacklisted(SparkListenerNodeBlacklisted nodeBlacklisted) {
    onEvent(nodeBlacklisted);
  }

  @Override
  public final void onNodeExcluded(SparkListenerNodeExcluded nodeExcluded) {
    onEvent(nodeExcluded);
  }

  @Override
  public final void onNodeUnblacklisted(SparkListenerNodeUnblacklisted nodeUnblacklisted) {
    onEvent(nodeUnblacklisted);
  }

  @Override
  public final void onNodeUnexcluded(SparkListenerNodeUnexcluded nodeUnexcluded) {
    onEvent(nodeUnexcluded);
  }

  @Override
  public void onBlockUpdated(SparkListenerBlockUpdated blockUpdated) {
    onEvent(blockUpdated);
  }

  @Override
  public void onSpeculativeTaskSubmitted(SparkListenerSpeculativeTaskSubmitted speculativeTask) {
    onEvent(speculativeTask);
  }

  public void onUnschedulableTaskSetAdded(
      SparkListenerUnschedulableTaskSetAdded unschedulableTaskSetAdded) {
    onEvent(unschedulableTaskSetAdded);
  }

  public void onUnschedulableTaskSetRemoved(
      SparkListenerUnschedulableTaskSetRemoved unschedulableTaskSetRemoved) {
    onEvent(unschedulableTaskSetRemoved);
  }

  @Override
  public void onResourceProfileAdded(SparkListenerResourceProfileAdded event) {
    onEvent(event);
  }

  @Override
  public void onOtherEvent(SparkListenerEvent event) {
    onEvent(event);
  }
}
