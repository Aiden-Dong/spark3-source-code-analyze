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

package org.apache.spark.sql.catalyst

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.errors.QueryExecutionErrors

package object encoders {
  /**
   * 返回一个内部的 encoder 对象，能够将 JVM 对象序列化/反序列化为 Spark SQL 行。
   * 隐式 encoder 应始终是未解析的（即不应包含来自特定 schema 的属性引用）。
   * 这一要求使我们能够在解析时保留给定对象类型是按名称绑定还是按序号绑定。
   */
  def encoderFor[A : Encoder]: ExpressionEncoder[A] = implicitly[Encoder[A]] match {
    case e: ExpressionEncoder[A] =>
      e.assertUnresolved()
      e
    case _ => throw QueryExecutionErrors.unsupportedEncoderError()
  }
}
