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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
 * 从 Spark 计划中移除冗余的 SortExec 节点。当其子节点同时满足排序顺序和所需的子节点分布时，排序节点是冗余的。
 * 注意，这个规则与优化器规则 EliminateSorts 不同，因为该规则还检查子节点是否满足所需的分布，
 * 这样不仅可以安全地移除局部排序，还可以在子节点已经满足所需排序顺序时移除全局排序。
 */
object RemoveRedundantSorts extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.REMOVE_REDUNDANT_SORTS_ENABLED)) {
      plan
    } else {
      removeSorts(plan)
    }
  }

  private def removeSorts(plan: SparkPlan): SparkPlan = plan transform {
    case s @ SortExec(orders, _, child, _)
        if SortOrder.orderingSatisfies(child.outputOrdering, orders) &&
          child.outputPartitioning.satisfies(s.requiredChildDistribution.head) =>
      child
  }
}
