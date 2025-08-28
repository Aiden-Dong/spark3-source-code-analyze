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

import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, RepartitionOperation, Statistics}
import org.apache.spark.sql.catalyst.trees.TreePattern.{LOGICAL_QUERY_STAGE, REPARTITION_OPERATION, TreePattern}
import org.apache.spark.sql.execution.SparkPlan

/**
 * 这是一个 LogicalPlan 包装器，用于包装一个 [[QueryStageExec]]，或者包含 [[QueryStageExec]] 的物理计划片段，
 * 其中 [[QueryStageExec]] 的所有祖先节点都链接到同一个逻辑节点。
 *
 * 例如，一个逻辑聚合（Aggregate）可以被转换为 FinalAgg - Shuffle - PartialAgg 的形式，
 * 其中 Shuffle 会被包装成一个 [[QueryStageExec]]，因此 [[LogicalQueryStage]]
 * 将拥有 FinalAgg - QueryStageExec 作为其物理计划。
 */
case class LogicalQueryStage(
    logicalPlan: LogicalPlan,
    physicalPlan: SparkPlan) extends LeafNode {

  override def output: Seq[Attribute] = logicalPlan.output
  override val isStreaming: Boolean = logicalPlan.isStreaming
  override val outputOrdering: Seq[SortOrder] = physicalPlan.outputOrdering
  override protected val nodePatterns: Seq[TreePattern] = {
    // Repartition is a special node that it represents a shuffle exchange,
    // then in AQE the repartition will be always wrapped into `LogicalQueryStage`
    val repartitionPattern = logicalPlan match {
      case _: RepartitionOperation => Some(REPARTITION_OPERATION)
      case _ => None
    }
    Seq(LOGICAL_QUERY_STAGE) ++ repartitionPattern
  }

  override def computeStats(): Statistics = {
    // TODO this is not accurate when there is other physical nodes above QueryStageExec.
    val physicalStats = physicalPlan.collectFirst {
      case s: QueryStageExec => s
    }.flatMap(_.computeStats())
    if (physicalStats.isDefined) {
      logDebug(s"Physical stats available as ${physicalStats.get} for plan: $physicalPlan")
    } else {
      logDebug(s"Physical stats not available for plan: $physicalPlan")
    }
    physicalStats.getOrElse(logicalPlan.stats)
  }

  override def maxRows: Option[Long] = stats.rowCount.map(_.min(Long.MaxValue).toLong)
}
