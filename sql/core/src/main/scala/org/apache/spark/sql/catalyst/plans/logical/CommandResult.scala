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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.execution.SparkPlan

/**
 * 逻辑计划节点，用于保存命令数据。
 * commandLogicalPlan 和 commandPhysicalPlan 仅用于显示 EXPLAIN 的计划树。
 * rows 可能不可序列化，理想情况下我们不应该将 rows 发送到执行器。 因此将它们标记为 transient（瞬态）。
 */
case class CommandResult(
    output: Seq[Attribute],
    @transient commandLogicalPlan: LogicalPlan,
    @transient commandPhysicalPlan: SparkPlan,
    @transient rows: Seq[InternalRow]) extends LeafNode {
  override def innerChildren: Seq[QueryPlan[_]] = Seq(commandLogicalPlan)

  override def computeStats(): Statistics =
    Statistics(sizeInBytes = EstimationUtils.getSizePerRow(output) * rows.length)
}
