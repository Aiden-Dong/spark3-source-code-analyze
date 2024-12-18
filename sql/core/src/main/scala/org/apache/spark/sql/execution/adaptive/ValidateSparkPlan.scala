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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}

/**
 * 检测由 AQE 重新规划生成的无效物理计划，并在检测到此类计划时抛出 `InvalidAQEPlanException`。
 * 该规则应在 EnsureRequirements 之后调用，在此阶段所有必要的 Exchange 节点已经添加。
 */
object ValidateSparkPlan extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    validate(plan)
    plan
  }

  /**
   * Validate that the plan satisfies the following condition:
   * - BroadcastQueryStage only appears as the immediate child and the build side of a broadcast
   *   hash join or broadcast nested loop join.
   */
  private def validate(plan: SparkPlan): Unit = plan match {
    case b: BroadcastHashJoinExec =>
      val (buildPlan, probePlan) = b.buildSide match {
        case BuildLeft => (b.left, b.right)
        case BuildRight => (b.right, b.left)
      }
      if (!buildPlan.isInstanceOf[BroadcastQueryStageExec]) {
        validate(buildPlan)
      }
      validate(probePlan)
    case b: BroadcastNestedLoopJoinExec =>
      val (buildPlan, probePlan) = b.buildSide match {
        case BuildLeft => (b.left, b.right)
        case BuildRight => (b.right, b.left)
      }
      if (!buildPlan.isInstanceOf[BroadcastQueryStageExec]) {
        validate(buildPlan)
      }
      validate(probePlan)
    case q: BroadcastQueryStageExec => errorOnInvalidBroadcastQueryStage(q)
    case _ => plan.children.foreach(validate)
  }

  private def errorOnInvalidBroadcastQueryStage(plan: SparkPlan): Unit = {
    throw InvalidAQEPlanException("Invalid broadcast query stage", plan)
  }
}
