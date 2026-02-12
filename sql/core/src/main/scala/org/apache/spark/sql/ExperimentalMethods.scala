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

package org.apache.spark.sql

import org.apache.spark.annotation.{Experimental, Unstable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * :: Experimental ::
 * 为最勇敢的人提供的实验性方法的持有者。我们不保证这里方法的二进制兼容性和源代码兼容性的稳定性。
 *
 * {{{
 *   spark.experimental.extraStrategies += ...
 * }}}
 *
 * @since 1.3.0
 */
@Experimental
@Unstable
class ExperimentalMethods private[sql]() {

  /**
   * 允许在运行时将额外的策略注入到查询规划器中。注意，此 API 应被视为实验性的，
   * 不保证在各个版本之间保持稳定。
   *
   * @since 1.3.0
   */
  @volatile var extraStrategies: Seq[Strategy] = Nil

  @volatile var extraOptimizations: Seq[Rule[LogicalPlan]] = Nil

  override def clone(): ExperimentalMethods = {
    val result = new ExperimentalMethods
    result.extraStrategies = extraStrategies
    result.extraOptimizations = extraOptimizations
    result
  }
}
