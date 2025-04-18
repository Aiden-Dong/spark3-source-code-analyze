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

import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Future => JFuture}
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkContext
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.sql.internal.StaticSQLConf.SQL_EVENT_TRUNCATE_LENGTH
import org.apache.spark.util.Utils

object SQLExecution {

  val EXECUTION_ID_KEY = "spark.sql.execution.id"

  private val _nextExecutionId = new AtomicLong(0)

  private def nextExecutionId: Long = _nextExecutionId.getAndIncrement

  private val executionIdToQueryExecution = new ConcurrentHashMap[Long, QueryExecution]()

  def getQueryExecution(executionId: Long): QueryExecution = {
    executionIdToQueryExecution.get(executionId)
  }

  private val testing = sys.props.contains(IS_TESTING.key)

  private[sql] def checkSQLExecutionId(sparkSession: SparkSession): Unit = {
    val sc = sparkSession.sparkContext
    // only throw an exception during tests. a missing execution ID should not fail a job.
    if (testing && sc.getLocalProperty(EXECUTION_ID_KEY) == null) {
      // Attention testers: when a test fails with this exception, it means that the action that
      // started execution of a query didn't call withNewExecutionId. The execution ID should be
      // set by calling withNewExecutionId in the action that begins execution, like
      // Dataset.collect or DataFrameWriter.insertInto.
      throw new IllegalStateException("Execution ID should be set")
    }
  }

  /**
   * 封装一个将执行“queryExecution”以跟踪主体中的所有 Spark 作业的操作，以便我们可以将它们与执行连接起来。
   */
  def withNewExecutionId[T](
      queryExecution: QueryExecution,
      name: Option[String] = None)
                            // body 为按需传值
                           (body: => T): T = queryExecution.sparkSession.withActive {
    val sparkSession = queryExecution.sparkSession
    val sc = sparkSession.sparkContext
    val oldExecutionId = sc.getLocalProperty(EXECUTION_ID_KEY)
    val executionId = SQLExecution.nextExecutionId
    sc.setLocalProperty(EXECUTION_ID_KEY, executionId.toString)
    executionIdToQueryExecution.put(executionId, queryExecution)
    try {
      // sparkContext.getCallSite() 首先尝试选择先前设置的任何调用位置，然后回退到 Utils.getCallSite()；
      // 在流式查询中直接调用 Utils.getCallSite() 将给出类似 "run at <unknown>:0" 的调用位置。
      val callSite = sc.getCallSite()

      val truncateLength = sc.conf.get(SQL_EVENT_TRUNCATE_LENGTH)

      // Job 描述信息
      val desc = Option(sc.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION))
        .filter(_ => truncateLength > 0)
        .map { sqlStr =>
          val redactedStr = Utils
            .redact(sparkSession.sessionState.conf.stringRedactionPattern, sqlStr)
          redactedStr.substring(0, Math.min(truncateLength, redactedStr.length))
        }.getOrElse(callSite.shortForm)

      val planDescriptionMode =
        ExplainMode.fromString(sparkSession.sessionState.conf.uiExplainMode)

      // 全局配置
      val globalConfigs = sparkSession.sharedState.conf.getAll.toMap
      // session 会话
      val modifiedConfigs = sparkSession.sessionState.conf.getAllConfs
        .filterNot(kv => globalConfigs.get(kv._1).contains(kv._2))
      val redactedConfigs = sparkSession.sessionState.conf.redactOptions(modifiedConfigs)

      withSQLConfPropagated(sparkSession) {
        var ex: Option[Throwable] = None
        val startTime = System.nanoTime()
        try {
          sc.listenerBus.post(SparkListenerSQLExecutionStart(
            executionId = executionId,
            description = desc,
            details = callSite.longForm,
            physicalPlanDescription = queryExecution.explainString(planDescriptionMode),
            // queryExecution.executedPlan 触发查询计划。
            // 如果失败，异常将被捕获并在 SparkListenerSQLExecutionEnd 中报告。
            sparkPlanInfo = SparkPlanInfo.fromSparkPlan(queryExecution.executedPlan),
            time = System.currentTimeMillis(),
            redactedConfigs))
          body
        } catch {
          case e: Throwable =>
            ex = Some(e)
            throw e
        } finally {
          val endTime = System.nanoTime()
          val event = SparkListenerSQLExecutionEnd(executionId, System.currentTimeMillis())
          // Currently only `Dataset.withAction` and `DataFrameWriter.runCommand` specify the `name`
          // parameter. The `ExecutionListenerManager` only watches SQL executions with name. We
          // can specify the execution name in more places in the future, so that
          // `QueryExecutionListener` can track more cases.
          event.executionName = name
          event.duration = endTime - startTime
          event.qe = queryExecution
          event.executionFailure = ex
          sc.listenerBus.post(event)
        }
      }
    } finally {
      executionIdToQueryExecution.remove(executionId)
      sc.setLocalProperty(EXECUTION_ID_KEY, oldExecutionId)
    }
  }

  /**
   * Wrap an action with a known executionId. When running a different action in a different
   * thread from the original one, this method can be used to connect the Spark jobs in this action
   * with the known executionId, e.g., `BroadcastExchangeExec.relationFuture`.
   */
  def withExecutionId[T](sparkSession: SparkSession, executionId: String)(body: => T): T = {
    val sc = sparkSession.sparkContext
    val oldExecutionId = sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    withSQLConfPropagated(sparkSession) {
      try {
        sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, executionId)
        body
      } finally {
        sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, oldExecutionId)
      }
    }
  }

  /**
   * Wrap an action with specified SQL configs. These configs will be propagated to the executor
   * side via job local properties.
   */
  def withSQLConfPropagated[T](sparkSession: SparkSession)(body: => T): T = {
    val sc = sparkSession.sparkContext
    // Set all the specified SQL configs to local properties, so that they can be available at
    // the executor side.
    val allConfigs = sparkSession.sessionState.conf.getAllConfs
    val originalLocalProps = allConfigs.collect {
      case (key, value) if key.startsWith("spark") =>
        val originalValue = sc.getLocalProperty(key)
        sc.setLocalProperty(key, value)
        (key, originalValue)
    }

    try {
      body
    } finally {
      for ((key, value) <- originalLocalProps) {
        sc.setLocalProperty(key, value)
      }
    }
  }

  /**
   * Wrap passed function to ensure necessary thread-local variables like
   * SparkContext local properties are forwarded to execution thread
   */
  def withThreadLocalCaptured[T](
      sparkSession: SparkSession, exec: ExecutorService) (body: => T): JFuture[T] = {
    val activeSession = sparkSession
    val sc = sparkSession.sparkContext
    val localProps = Utils.cloneProperties(sc.getLocalProperties)
    exec.submit(() => {
      val originalSession = SparkSession.getActiveSession
      val originalLocalProps = sc.getLocalProperties
      SparkSession.setActiveSession(activeSession)
      sc.setLocalProperties(localProps)
      val res = body
      // reset active session and local props.
      sc.setLocalProperties(originalLocalProps)
      if (originalSession.nonEmpty) {
        SparkSession.setActiveSession(originalSession.get)
      } else {
        SparkSession.clearActiveSession()
      }
      res
    })
  }
}
