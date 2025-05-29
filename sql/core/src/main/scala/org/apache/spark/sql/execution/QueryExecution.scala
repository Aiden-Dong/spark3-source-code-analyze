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

import java.io.{BufferedWriter, OutputStreamWriter}
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.{InternalRow, QueryPlanningTracker}
import org.apache.spark.sql.catalyst.analysis.UnsupportedOperationChecker
import org.apache.spark.sql.catalyst.expressions.codegen.ByteCodeStats
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, Command, CommandResult, CreateTableAsSelect, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic, ReplaceTableAsSelect, ReturnAnswer}
import org.apache.spark.sql.catalyst.rules.{PlanChangeLogger, Rule}
import org.apache.spark.sql.catalyst.util.StringUtils.PlanStringConcat
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.adaptive.{AdaptiveExecutionContext, InsertAdaptiveSparkPlan}
import org.apache.spark.sql.execution.bucketing.{CoalesceBucketsInJoin, DisableUnnecessaryBucketedScan}
import org.apache.spark.sql.execution.dynamicpruning.PlanDynamicPruningFilters
import org.apache.spark.sql.execution.exchange.EnsureRequirements
import org.apache.spark.sql.execution.reuse.ReuseExchangeAndSubquery
import org.apache.spark.sql.execution.streaming.{IncrementalExecution, OffsetSeqMetadata}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.util.Utils

/**
 * 使用 Spark 执行关系查询的主要工作流程。
 * 设计为使开发人员可以轻松访问查询执行的中间阶段。
 * 虽然这不是一个公共类，但为了避免改变函数名称，我们不应随意更改它们，因为许多开发人员使用此功能进行调试。
 */
class QueryExecution(
    val sparkSession: SparkSession,
    val logical: LogicalPlan,
    val tracker: QueryPlanningTracker = new QueryPlanningTracker,
    val mode: CommandExecutionMode.Value = CommandExecutionMode.ALL) extends Logging {

  val id: Long = QueryExecution.nextExecutionId

  // TODO: Move the planner an optimizer into here from SessionState.
  protected def planner = sparkSession.sessionState.planner

  def assertAnalyzed(): Unit = analyzed

  def assertSupported(): Unit = {
    if (sparkSession.sessionState.conf.isUnsupportedOperationCheckEnabled) {
      UnsupportedOperationChecker.checkForBatch(analyzed)
    }
  }

  // 分析逻辑树- 主要为状态符号填充元信息
  lazy val analyzed: LogicalPlan = executePhase(QueryPlanningTracker.ANALYSIS) {
    // 我们不能在这里克隆 logical，因为这会重置 _analyzed 标志位。
    sparkSession.sessionState.analyzer.executeAndCheck(logical, tracker)
  }

  // 判断是否是 Command 类型SQL, 是否要立即执行
  lazy val commandExecuted: LogicalPlan = mode match {
    case CommandExecutionMode.NON_ROOT => analyzed.mapChildren(eagerlyExecuteCommands)
    case CommandExecutionMode.ALL => eagerlyExecuteCommands(analyzed)                    // 默认情况
    case CommandExecutionMode.SKIP => analyzed
  }

  private def commandExecutionName(command: Command): String = command match {
    case _: CreateTableAsSelect => "create"
    case _: ReplaceTableAsSelect => "replace"
    case _: AppendData => "append"
    case _: OverwriteByExpression => "overwrite"
    case _: OverwritePartitionsDynamic => "overwritePartitions"
    case _ => "command"
  }

  /**
   * 提前执行命令，或者是立刻执行命令
   *  LogicalPlan.transformDown  是一个自顶向下的（需要优先处理父节点）它的核心功能是遍历并修改逻辑计划的结构
   *  transformDown	自顶向下	需要优先处理父节点（如谓词下推）
   *  transformUp	自底向上	需要子节点处理完毕后再处理父节点（如列剪裁）
   */
  private def eagerlyExecuteCommands(p: LogicalPlan) = p transformDown {

    case c: Command =>
      val qe = sparkSession.sessionState.executePlan(c, CommandExecutionMode.NON_ROOT)
      val result = SQLExecution.withNewExecutionId(qe, Some(commandExecutionName(c))) {
        qe.executedPlan.executeCollect()
      }
      CommandResult(
        qe.analyzed.output,
        qe.commandExecuted,
        qe.executedPlan,
        result)
    case other => other
  }

  lazy val withCachedData: LogicalPlan = sparkSession.withActive {
    assertAnalyzed()
    assertSupported()
    // 克隆计划，以避免在分析、优化和规划等不同阶段之间共享计划实例。
    sparkSession.sharedState.cacheManager.useCachedData(commandExecuted.clone())
  }

  def assertCommandExecuted(): Unit = commandExecuted

  // 使用优化器优化 LogicalPlan
  lazy val optimizedPlan: LogicalPlan = {
    // 我们需要在这里物化 commandExecuted
    // 因为 optimizedPlan 也会在优化阶段进行跟踪。
    assertCommandExecuted()
    executePhase(QueryPlanningTracker.OPTIMIZATION) {
      // 克隆计划，以避免在不同阶段（如分析、优化和规划）之间共享计划实例。
      val plan =  sparkSession.sessionState.optimizer.executeAndTrack(withCachedData.clone(), tracker)
      // We do not want optimized plans to be re-analyzed as literals that have been constant
      // folded and such can cause issues during analysis. While `clone` should maintain the
      // `analyzed` state of the LogicalPlan, we set the plan as analyzed here as well out of
      // paranoia.
      plan.setAnalyzed()
      plan
    }
  }

  private def assertOptimized(): Unit = optimizedPlan

  // 将逻辑计划转化为物理计划
  lazy val sparkPlan: SparkPlan = {
    // 我们需要在这里物化 optimizedPlan，因为 sparkPlan 也会在规划阶段进行跟踪。
    assertOptimized()
    executePhase(QueryPlanningTracker.PLANNING) {
      // Clone the logical plan here, in case the planner rules change the states of the logical
      // plan.
      QueryExecution.createSparkPlan(sparkSession, planner, optimizedPlan.clone())
    }
  }

  // executedPlan不应用于初始化任何SparkPlan。应该是
  // 仅用于执行。
  lazy val executedPlan: SparkPlan = {
    // 我们需要在跟踪规划阶段之前对 optimizedPlan 进行具体化，以确保优化时间不会被计入规划阶段的时间。
    assertOptimized()
    executePhase(QueryPlanningTracker.PLANNING) {
      // 克隆计划以避免在不同阶段（如分析、优化和规划）之间共享计划实例。
      QueryExecution.prepareForExecution(preparations, sparkPlan.clone())
    }
  }

  /**
   * Internal version of the RDD. Avoids copies and has no schema.
   * Note for callers: Spark may apply various optimization including reusing object: this means
   * the row is valid only for the iteration it is retrieved. You should avoid storing row and
   * accessing after iteration. (Calling `collect()` is one of known bad usage.)
   * If you want to store these rows into collection, please apply some converter or copy row
   * which produces new object per iteration.
   * Given QueryExecution is not a public class, end users are discouraged to use this: please
   * use `Dataset.rdd` instead where conversion will be applied.
   */
  lazy val toRdd: RDD[InternalRow] = new SQLExecutionRDD(
    executedPlan.execute(), sparkSession.sessionState.conf)

  /** Get the metrics observed during the execution of the query plan. */
  def observedMetrics: Map[String, Row] = CollectMetricsExec.collect(executedPlan)

  protected def preparations: Seq[Rule[SparkPlan]] = {
    QueryExecution.preparations(sparkSession,
      Option(InsertAdaptiveSparkPlan(AdaptiveExecutionContext(sparkSession, this))), false)
  }

  protected def executePhase[T](phase: String)(block: => T): T = sparkSession.withActive {
    QueryExecution.withInternalError(s"The Spark SQL phase $phase failed with an internal error.") {
      tracker.measurePhase(phase)(block)
    }
  }

  def simpleString: String = {
    val concat = new PlanStringConcat()
    simpleString(false, SQLConf.get.maxToStringFields, concat.append)
    withRedaction {
      concat.toString
    }
  }

  private def simpleString(
      formatted: Boolean,
      maxFields: Int,
      append: String => Unit): Unit = {
    append("== Physical Plan ==\n")
    if (formatted) {
      try {
        ExplainUtils.processPlan(executedPlan, append)
      } catch {
        case e: AnalysisException => append(e.toString)
        case e: IllegalArgumentException => append(e.toString)
      }
    } else {
      QueryPlan.append(executedPlan,
        append, verbose = false, addSuffix = false, maxFields = maxFields)
    }
    append("\n")
  }

  def explainString(mode: ExplainMode): String = {
    val concat = new PlanStringConcat()
    explainString(mode, SQLConf.get.maxToStringFields, concat.append)
    withRedaction {
      concat.toString
    }
  }

  private def explainString(mode: ExplainMode, maxFields: Int, append: String => Unit): Unit = {
    val queryExecution = if (logical.isStreaming) {
      // This is used only by explaining `Dataset/DataFrame` created by `spark.readStream`, so the
      // output mode does not matter since there is no `Sink`.
      new IncrementalExecution(
        sparkSession, logical, OutputMode.Append(), "<unknown>",
        UUID.randomUUID, UUID.randomUUID, 0, OffsetSeqMetadata(0, 0))
    } else {
      this
    }

    mode match {
      case SimpleMode =>
        queryExecution.simpleString(false, maxFields, append)
      case ExtendedMode =>
        queryExecution.toString(maxFields, append)
      case CodegenMode =>
        try {
          org.apache.spark.sql.execution.debug.writeCodegen(append, queryExecution.executedPlan)
        } catch {
          case e: AnalysisException => append(e.toString)
        }
      case CostMode =>
        queryExecution.stringWithStats(maxFields, append)
      case FormattedMode =>
        queryExecution.simpleString(formatted = true, maxFields = maxFields, append)
    }
  }

  private def writePlans(append: String => Unit, maxFields: Int): Unit = {
    val (verbose, addSuffix) = (true, false)
    append("== Parsed Logical Plan ==\n")
    QueryPlan.append(logical, append, verbose, addSuffix, maxFields)
    append("\n== Analyzed Logical Plan ==\n")
    try {
      if (analyzed.output.nonEmpty) {
        append(
          truncatedString(
            analyzed.output.map(o => s"${o.name}: ${o.dataType.simpleString}"), ", ", maxFields)
        )
        append("\n")
      }
      QueryPlan.append(analyzed, append, verbose, addSuffix, maxFields)
      append("\n== Optimized Logical Plan ==\n")
      QueryPlan.append(optimizedPlan, append, verbose, addSuffix, maxFields)
      append("\n== Physical Plan ==\n")
      QueryPlan.append(executedPlan, append, verbose, addSuffix, maxFields)
    } catch {
      case e: AnalysisException => append(e.toString)
    }
  }

  override def toString: String = withRedaction {
    val concat = new PlanStringConcat()
    toString(SQLConf.get.maxToStringFields, concat.append)
    withRedaction {
      concat.toString
    }
  }

  private def toString(maxFields: Int, append: String => Unit): Unit = {
    writePlans(append, maxFields)
  }

  def stringWithStats: String = {
    val concat = new PlanStringConcat()
    stringWithStats(SQLConf.get.maxToStringFields, concat.append)
    withRedaction {
      concat.toString
    }
  }

  private def stringWithStats(maxFields: Int, append: String => Unit): Unit = {
    // trigger to compute stats for logical plans
    try {
      // This will trigger to compute stats for all the nodes in the plan, including subqueries,
      // if the stats doesn't exist in the statsCache and update the statsCache corresponding
      // to the node.
      optimizedPlan.collectWithSubqueries {
        case plan => plan.stats
      }
    } catch {
      case e: AnalysisException => append(e.toString + "\n")
    }
    // only show optimized logical plan and physical plan
    append("== Optimized Logical Plan ==\n")
    QueryPlan.append(optimizedPlan, append, verbose = true, addSuffix = true, maxFields)
    append("\n== Physical Plan ==\n")
    QueryPlan.append(executedPlan, append, verbose = true, addSuffix = false, maxFields)
    append("\n")
  }

  /**
   * Redact the sensitive information in the given string.
   */
  private def withRedaction(message: String): String = {
    Utils.redact(sparkSession.sessionState.conf.stringRedactionPattern, message)
  }

  /** A special namespace for commands that can be used to debug query execution. */
  // scalastyle:off
  object debug {
  // scalastyle:on

    /**
     * Prints to stdout all the generated code found in this plan (i.e. the output of each
     * WholeStageCodegen subtree).
     */
    def codegen(): Unit = {
      // scalastyle:off println
      println(org.apache.spark.sql.execution.debug.codegenString(executedPlan))
      // scalastyle:on println
    }

    /**
     * Get WholeStageCodegenExec subtrees and the codegen in a query plan
     *
     * @return Sequence of WholeStageCodegen subtrees and corresponding codegen
     */
    def codegenToSeq(): Seq[(String, String, ByteCodeStats)] = {
      org.apache.spark.sql.execution.debug.codegenStringSeq(executedPlan)
    }

    /**
     * Dumps debug information about query execution into the specified file.
     *
     * @param path path of the file the debug info is written to.
     * @param maxFields maximum number of fields converted to string representation.
     * @param explainMode the explain mode to be used to generate the string
     *                    representation of the plan.
     */
    def toFile(
        path: String,
        maxFields: Int = Int.MaxValue,
        explainMode: Option[String] = None): Unit = {
      val filePath = new Path(path)
      val fs = filePath.getFileSystem(sparkSession.sessionState.newHadoopConf())
      val writer = new BufferedWriter(new OutputStreamWriter(fs.create(filePath)))
      try {
        val mode = explainMode.map(ExplainMode.fromString(_)).getOrElse(ExtendedMode)
        explainString(mode, maxFields, writer.write)
        if (mode != CodegenMode) {
          writer.write("\n== Whole Stage Codegen ==\n")
          org.apache.spark.sql.execution.debug.writeCodegen(writer.write, executedPlan)
        }
        log.info(s"Debug information was written at: $filePath")
      } finally {
        writer.close()
      }
    }
  }
}

/**
 * SPARK-35378: Commands should be executed eagerly so that something like `sql("INSERT ...")`
 * can trigger the table insertion immediately without a `.collect()`. To avoid end-less recursion
 * we should use `NON_ROOT` when recursively executing commands. Note that we can't execute
 * a query plan with leaf command nodes, because many commands return `GenericInternalRow`
 * and can't be put in a query plan directly, otherwise the query engine may cast
 * `GenericInternalRow` to `UnsafeRow` and fail. When running EXPLAIN, or commands inside other
 * command, we should use `SKIP` to not eagerly trigger the command execution.
 */
object CommandExecutionMode extends Enumeration {
  val SKIP, NON_ROOT, ALL = Value
}

object QueryExecution {
  private val _nextExecutionId = new AtomicLong(0)

  private def nextExecutionId: Long = _nextExecutionId.getAndIncrement

  /****
   * 构建一系列规则，用于准备执行计划的[[SparkPlan]]。
   * 这些规则将确保子查询是有计划的，利用数据分区和排序正确，插入整个阶段代码生成器，并尝试通过重用交换来减少所做的工作
   * 以及子查询。
   **/
  private[execution] def preparations(
      sparkSession: SparkSession,
      adaptiveExecutionRule: Option[InsertAdaptiveSparkPlan] = None,
      subquery: Boolean): Seq[Rule[SparkPlan]] = {
    // `AdaptiveSparkPlanExec` 是一个叶子节点。如果插入，它将使所有后续的规则变为无操作（no-op），
    // 因为原始计划被隐藏在 `AdaptiveSparkPlanExec` 后面。
    adaptiveExecutionRule.toSeq ++
    Seq(
      CoalesceBucketsInJoin,
      PlanDynamicPruningFilters(sparkSession),
      PlanSubqueries(sparkSession),
      RemoveRedundantProjects,
      EnsureRequirements(),
      // `ReplaceHashWithSortAgg` needs to be added after `EnsureRequirements` to guarantee the
      // sort order of each node is checked to be valid.
      ReplaceHashWithSortAgg,
      // `RemoveRedundantSorts` needs to be added after `EnsureRequirements` to guarantee the same
      // number of partitions when instantiating PartitioningCollection.
      RemoveRedundantSorts,
      DisableUnnecessaryBucketedScan,
      ApplyColumnarRulesAndInsertTransitions(
        sparkSession.sessionState.columnarRules, outputsColumnar = false),
      CollapseCodegenStages()) ++
      (if (subquery) {
        Nil
      } else {
        Seq(ReuseExchangeAndSubquery)
      })
  }

  /**
   * Prepares a planned [[SparkPlan]] for execution by inserting shuffle operations and internal
   * row format conversions as needed.
   * 通过插入shuffle操作和内部命令来准备执行计划的[[SparkPlan]] 根据需要进行行格式转换。
   */
  private[execution] def prepareForExecution(
      preparations: Seq[Rule[SparkPlan]],
      plan: SparkPlan): SparkPlan = {
    val planChangeLogger = new PlanChangeLogger[SparkPlan]()
    val preparedPlan = preparations.foldLeft(plan) { case (sp, rule) =>
      val result = rule.apply(sp)
      planChangeLogger.logRule(rule.ruleName, sp, result)
      result
    }
    planChangeLogger.logBatch("Preparations", plan, preparedPlan)
    preparedPlan
  }

  /**
   * 将 [[LogicalPlan]] 转换成 [[SparkPlan]].
   *
   * Note that the returned physical plan still needs to be prepared for execution
   * 请注意，返回的物理计划仍需准备才能执行.
   */
  def createSparkPlan(
      sparkSession: SparkSession,
      planner: SparkPlanner,
      plan: LogicalPlan): SparkPlan = {
    // TODO: We use next(), i.e. take the first plan returned by the planner, here for now,
    //       but we will implement to choose the best plan.
    planner.plan(ReturnAnswer(plan)).next()
  }

  /**
   * 为 [[SparkPlan]] 的执行做准备.
   */
  def prepareExecutedPlan(spark: SparkSession, plan: SparkPlan): SparkPlan = {
    prepareForExecution(preparations(spark, subquery = true), plan)
  }

  /**
   * Transform the subquery's [[LogicalPlan]] into a [[SparkPlan]] and prepare the resulting
   * [[SparkPlan]] for execution.
   */
  def prepareExecutedPlan(spark: SparkSession, plan: LogicalPlan): SparkPlan = {
    val sparkPlan = createSparkPlan(spark, spark.sessionState.planner, plan.clone())
    prepareExecutedPlan(spark, sparkPlan)
  }

  /**
   * Prepare the [[SparkPlan]] for execution using exists adaptive execution context.
   * This method is only called by [[PlanAdaptiveDynamicPruningFilters]].
   */
  def prepareExecutedPlan(
      session: SparkSession,
      plan: LogicalPlan,
      context: AdaptiveExecutionContext): SparkPlan = {
    val sparkPlan = createSparkPlan(session, session.sessionState.planner, plan.clone())
    val preparationRules = preparations(session, Option(InsertAdaptiveSparkPlan(context)), true)
    prepareForExecution(preparationRules, sparkPlan.clone())
  }

  /**
   * Converts asserts, null pointer exceptions to internal errors.
   */
  private[sql] def toInternalError(msg: String, e: Throwable): Throwable = e match {
    case e @ (_: java.lang.NullPointerException | _: java.lang.AssertionError) =>
      new SparkException(
        errorClass = "INTERNAL_ERROR",
        messageParameters = Array(msg +
          " Please, fill a bug report in, and provide the full stack trace."),
        cause = e)
    case e: Throwable =>
      e
  }

  /**
   * Catches asserts, null pointer exceptions, and converts them to internal errors.
   */
  private[sql] def withInternalError[T](msg: String)(block: => T): T = {
    try {
      block
    } catch {
      case e: Throwable => throw toInternalError(msg, e)
    }
  }
}
