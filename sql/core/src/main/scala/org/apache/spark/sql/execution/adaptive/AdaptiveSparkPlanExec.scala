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

import java.util
import java.util.concurrent.LinkedBlockingQueue

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.rules.{PlanChangeLogger, Rule}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec._
import org.apache.spark.sql.execution.bucketing.DisableUnnecessaryBucketedScan
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.ui.{SparkListenerSQLAdaptiveExecutionUpdate, SparkListenerSQLAdaptiveSQLMetricUpdates, SQLPlanMetric}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{SparkFatalException, ThreadUtils}

/**
 * 执行查询计划的自适应根节点。它将查询计划拆分为独立的阶段，并根据它们的依赖关系依次执行这些阶段。
 * 查询阶段在结束时将其输出物化。当一个阶段完成时，物化输出的数据统计将用于优化查询的其余部分。
 *
 * 为了创建查询阶段，我们自下而上遍历查询树。当我们遇到一个交换节点，并且该交换节点的所有子查询阶段都已物化时，
 * 我们为该交换节点创建一个新的查询阶段。新阶段一旦创建，就会异步地进行物化。
 *
 * 当一个查询阶段完成物化时，根据所有物化阶段提供的最新统计数据，重新优化并计划剩余的查询。
 * 然后我们再次遍历查询计划，并在可能的情况下创建更多阶段。所有阶段物化后，我们执行剩余的计划。
 *
 * LogicalPlan → Optimize → PhysicalPlan → Stage1 → 收集统计
 * ↑                                              ↓
 * └── 基于运行时统计重新优化 ← Stage2 ← 更新逻辑计划
 */
case class AdaptiveSparkPlanExec(
    inputPlan: SparkPlan,
    @transient context: AdaptiveExecutionContext,
    @transient preprocessingRules: Seq[Rule[SparkPlan]],
    @transient isSubquery: Boolean,
    @transient override val supportsColumnar: Boolean = false)
  extends LeafExecNode {

  @transient private val logOnLevel: ( => String) => Unit = conf.adaptiveExecutionLogLevel match {
    case "TRACE" => logTrace(_)
    case "DEBUG" => logDebug(_)
    case "INFO" => logInfo(_)
    case "WARN" => logWarning(_)
    case "ERROR" => logError(_)
    case _ => logDebug(_)
  }

  @transient private val planChangeLogger = new PlanChangeLogger[SparkPlan]()

  /////////  配置上下文   /////////////

  // 线程同步锁
  @transient private val lock = new Object()

  // AQE 专用优化器
  @transient private val optimizer = new AQEOptimizer(conf)

  // 成本评估器，用于比较计划优劣
  @transient private val costEvaluator = conf.getConf(SQLConf.ADAPTIVE_CUSTOM_COST_EVALUATOR_CLASS) match {
      case Some(className) => CostEvaluator.instantiate(className, session.sparkContext.getConf)
      case _ => SimpleCostEvaluator(conf.getConf(SQLConf.ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN))
    }

  /////////  执行计划状态 ////////////////

  // 初始物理计划（经过预处理）
  @transient val initialPlan = context.session.withActive {
    applyPhysicalRules(
      inputPlan, queryStagePreparationRules, Some((planChangeLogger, "AQE Preparations")))
  }

  // 当前执行计划
  @volatile private var currentPhysicalPlan = initialPlan

  // 是否为最终计划
  private var isFinalPlan = false

  // 当前阶段ID
  private var currentStageId = 0


  ////// 核心优化规则 ////////////////////

  // `EnsureRequirements` 可能会移除用户指定的重新分区，并假设查询计划不会更改其输出分区。
  // 但在 AQE 中，这一假设并不成立。这里我们检查尚未被 `EnsureRequirements` 处理的 `inputPlan`，
  // 以找出有效的用户指定的重新分区。稍后，AQE 框架将确保最终输出分区不会相对于有效的用户指定重新分区发生变化。
  @transient private val requiredDistribution: Option[Distribution] = if (isSubquery) {
    // Subquery output does not need a specific output partitioning.
    Some(UnspecifiedDistribution)
  } else {
    AQEUtils.getRequiredDistribution(inputPlan)
  }
  
  // 查询阶段准备规则
  @transient private val queryStagePreparationRules: Seq[Rule[SparkPlan]] = {
    // 对于像 `df.repartition(a, b).select(c)` 这样的情况，最终计划没有分区要求，但我们确实需要尊重用户指定的重新分区。
    // 在这里，我们要求 `EnsureRequirements` 不优化掉用户指定的按列重新分区，以解决这种情况。
    val ensureRequirements =
    EnsureRequirements(requiredDistribution.isDefined, requiredDistribution)

    Seq(
      RemoveRedundantProjects,                  // 移除冗余投影
      ensureRequirements,                       // 确保分布要求
      AdjustShuffleExchangePosition,            // 调整Shuffle位置
      ValidateSparkPlan,                        // 验证执行计划
      ReplaceHashWithSortAgg,                   // 替换Hash聚合为Sort聚合
      RemoveRedundantSorts,                     // 移除冗余排序
      DisableUnnecessaryBucketedScan,           // 禁用不必要的分桶扫描

      // 优化倾斜连接，避免数据倾斜导致的滞后任务
      // 将倾斜分区拆分为更小分区
      // 在连接另一侧复制匹配分区以并行执行
      OptimizeSkewedJoin(ensureRequirements)    // 优化倾斜Join
    ) ++ context.session.sessionState.queryStagePrepRules   // 自定义优化
  }


  // 查询阶段优化规则
  @transient private val queryStageOptimizerRules: Seq[Rule[SparkPlan]] = Seq(
    PlanAdaptiveDynamicPruningFilters(this),                //  动态分区裁剪
    ReuseAdaptiveSubquery(context.subqueryCache),           //  重用子查询
    OptimizeSkewInRebalancePartitions,                      //  优化重平衡分区中的倾斜

    // 基于映射输出统计信息合并 shuffle 分区
    // 避免许多小的归并任务，提高性能
    // 目标: 64MB 每个分区 (可配置)
    CoalesceShufflePartitions(context.session),             //  合并Shuffle分区

    // 当不需要 shuffle 分区时使用本地 shuffle reader
    // 例如: sort-merge join 转换为 broadcast-hash join 后
    OptimizeShuffleWithLocalRead                            // 优化 Shuffle 读取为本地读取
  )




  // This rule is stateful as it maintains the codegen stage ID. We can't create a fresh one every
  // time and need to keep it in a variable.
  @transient private val collapseCodegenStagesRule: Rule[SparkPlan] =
    CollapseCodegenStages()

  // A list of physical optimizer rules to be applied right after a new stage is created. The input
  // plan to these rules has exchange as its root node.
  private def postStageCreationRules(outputsColumnar: Boolean) = Seq(
    ApplyColumnarRulesAndInsertTransitions(
      context.session.sessionState.columnarRules, outputsColumnar),
    collapseCodegenStagesRule
  )

  private def optimizeQueryStage(plan: SparkPlan, isFinalStage: Boolean): SparkPlan = {
    val optimized = queryStageOptimizerRules.foldLeft(plan) { case (latestPlan, rule) =>
      val applied = rule.apply(latestPlan)
      val result = rule match {
        case _: AQEShuffleReadRule if !applied.fastEquals(latestPlan) =>
          val distribution = if (isFinalStage) {
            // If `requiredDistribution` is None, it means `EnsureRequirements` will not optimize
            // out the user-specified repartition, thus we don't have a distribution requirement
            // for the final plan.
            requiredDistribution.getOrElse(UnspecifiedDistribution)
          } else {
            UnspecifiedDistribution
          }
          if (ValidateRequirements.validate(applied, distribution)) {
            applied
          } else {
            logDebug(s"Rule ${rule.ruleName} is not applied as it breaks the " +
              "distribution requirement of the query plan.")
            latestPlan
          }
        case _ => applied
      }
      planChangeLogger.logRule(rule.ruleName, latestPlan, result)
      result
    }
    planChangeLogger.logBatch("AQE Query Stage Optimization", plan, optimized)
    optimized
  }

  /**
   * Return type for `createQueryStages`
   * @param newPlan 新的物理计划.
   * @param allChildStagesMaterialized 所有子阶段是否已物化.
   * @param newStages 新创建的查询阶段列表.
   */
  private case class CreateStageResult(
    // 如果没有 exchange 操作，  newPlan=oldPlan
    // 如果发生 exchage 操作, 则改 exchage 被替换为 QueryStageExec 操作(仅当所有子节点被物化)
    newPlan: SparkPlan,
    // 判断是否所有子节点都被物化
    allChildStagesMaterialized: Boolean,
    // 有没有新产生的 QueryStageExec 节点(需要被物化)
    newStages: Seq[QueryStageExec])

  def executedPlan: SparkPlan = currentPhysicalPlan

  override def conf: SQLConf = context.session.sessionState.conf

  override def output: Seq[Attribute] = inputPlan.output

  override def doCanonicalize(): SparkPlan = inputPlan.canonicalized

  override def resetMetrics(): Unit = {
    metrics.valuesIterator.foreach(_.reset())
    executedPlan.resetMetrics()
  }

  private def getExecutionId: Option[Long] = {
    // 如果 `QueryExecution` 与当前执行 ID 不匹配，则意味着执行 ID 属于另一个（父）查询，
    // 我们不应该在此查询中调用更新 UI 的操作。d
    Option(context.session.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
      .map(_.toLong).filter(SQLExecution.getQueryExecution(_) eq context.qe)
  }

  private def getFinalPhysicalPlan(): SparkPlan = lock.synchronized {

    // 判断是都是最终计划 -- ?????
    if (isFinalPlan) return currentPhysicalPlan

    // 如果自适应计划在 `withActive` 范围函数之外执行，例如 `plan.queryExecution.rdd`，
    // 我们需要在这里设置活动会话，因为在执行过程中可能会创建新的计划节点。

    context.session.withActive {       // 作用？ ？？？
      val executionId = getExecutionId   // 获取当前的 SQL ID
      // Use inputPlan logicalLink here in case some top level physical nodes may be removed
      // during `initialPlan`
      var currentLogicalPlan = inputPlan.logicalLink.get

      //  TODO - 1 : 搜索逻辑树中是否有 Exchange 节点
      //             如果存在 Exchange 节点则创建对应的 QueryStageExec
      var result = createQueryStages(currentPhysicalPlan)    // 创建查询阶段

      val events = new LinkedBlockingQueue[StageMaterializationEvent]()  // 阶段物化事件队列（成功/失败）
      val errors = new mutable.ArrayBuffer[Throwable]()                  // 错误收集器
      var stagesToReplace = Seq.empty[QueryStageExec]                    // 需要替换的查询阶段列表

      // 表示如果新的节点还没有开启物化
      while (!result.allChildStagesMaterialized) {

        // 拿到优化以后的新的 逻辑树
        currentPhysicalPlan = result.newPlan

        //  TODO - 2 : 表示有新的需要物化的结果树 - 要对所有需要物化的结果树进行物化处理
        if (result.newStages.nonEmpty) {

          stagesToReplace = result.newStages ++ stagesToReplace

          // 通知 Listener 物理计划被改变
          executionId.foreach(onUpdatePlan(_, result.newStages.map(_.plan)))

          // SPARK-33933: 优先提交广播阶段，避免广播超时
          val reorderedNewStages = result.newStages
            .sortWith {
              case (_: BroadcastQueryStageExec, _: BroadcastQueryStageExec) => false
              case (_: BroadcastQueryStageExec, _) => true
              case _ => false
            }

          // 异步启动所有新阶段的物化
          reorderedNewStages.foreach { stage =>
            try {
              stage.materialize().onComplete { res =>
                if (res.isSuccess) {
                  events.offer(StageSuccess(stage, res.get))
                } else {
                  events.offer(StageFailure(stage, res.failed.get))
                }
              }(AdaptiveSparkPlanExec.executionContext)
            } catch {
              case e: Throwable =>
                cleanUpAndThrowException(Seq(e), Some(stage.id))
            }
          }
        }

        // 等待物化完成
        val nextMsg = events.take()
        val rem = new util.ArrayList[StageMaterializationEvent]()
        events.drainTo(rem)
        (Seq(nextMsg) ++ rem.asScala).foreach {
          case StageSuccess(stage, res) =>
            stage.resultOption.set(Some(res))
          case StageFailure(stage, ex) =>
            errors.append(ex)
        }

        // 如果物化过程发生失败， 则终止并抛出异常
        if (errors.nonEmpty) {
          cleanUpAndThrowException(errors.toSeq, None)
        }


        // TODO - 3  : 重优化处理

        // 因为物理树发生了变更， 所以需要将当前的逻辑树做修改
        val logicalPlan = replaceWithQueryStagesInLogicalPlan(currentLogicalPlan, stagesToReplace)
        // 2. 基于更新后的逻辑计划重新优化
        val afterReOptimize = reOptimize(logicalPlan)

        // TODO - 4 基于成本比较器 : 挑选最优的计划树

        if (afterReOptimize.isDefined) {
          // 拿到优化后的逻辑计划树跟物理计划树
          val (newPhysicalPlan, newLogicalPlan) = afterReOptimize.get


          val origCost = costEvaluator.evaluateCost(currentPhysicalPlan)    // 计算当前的物理计划损失
          val newCost = costEvaluator.evaluateCost(newPhysicalPlan)         // 计算新的物理计划损失

          if (newCost < origCost ||
            (newCost == origCost && currentPhysicalPlan != newPhysicalPlan)) {

            logOnLevel(s"Plan changed from $currentPhysicalPlan to $newPhysicalPlan")
            cleanUpTempTags(newPhysicalPlan)
            currentPhysicalPlan = newPhysicalPlan
            currentLogicalPlan = newLogicalPlan          // 当前的逻辑计划树
            stagesToReplace = Seq.empty[QueryStageExec]
          }
        }
        // Now that some stages have finished, we can try creating new stages.
        result = createQueryStages(currentPhysicalPlan)
      }

      // Run the final plan when there's no more unfinished stages.
      currentPhysicalPlan = applyPhysicalRules(
        optimizeQueryStage(result.newPlan, isFinalStage = true),
        postStageCreationRules(supportsColumnar),
        Some((planChangeLogger, "AQE Post Stage Creation")))
      isFinalPlan = true

      // 通知WebUI 此计划被采纳
      executionId.foreach(onUpdatePlan(_, Seq(currentPhysicalPlan)))
      currentPhysicalPlan
    }
  }

  // Use a lazy val to avoid this being called more than once.
  @transient private lazy val finalPlanUpdate: Unit = {
    // Subqueries that don't belong to any query stage of the main query will execute after the
    // last UI update in `getFinalPhysicalPlan`, so we need to update UI here again to make sure
    // the newly generated nodes of those subqueries are updated.
    if (!isSubquery && currentPhysicalPlan.exists(_.subqueries.nonEmpty)) {
      getExecutionId.foreach(onUpdatePlan(_, Seq.empty))
    }
    logOnLevel(s"Final plan: $currentPhysicalPlan")
  }

  override def executeCollect(): Array[InternalRow] = {
    withFinalPlanUpdate(_.executeCollect())
  }

  override def executeTake(n: Int): Array[InternalRow] = {
    withFinalPlanUpdate(_.executeTake(n))
  }

  override def executeTail(n: Int): Array[InternalRow] = {
    withFinalPlanUpdate(_.executeTail(n))
  }

  override def doExecute(): RDD[InternalRow] = {
    withFinalPlanUpdate(_.execute())
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    withFinalPlanUpdate(_.executeColumnar())
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    withFinalPlanUpdate { finalPlan =>
      assert(finalPlan.isInstanceOf[BroadcastQueryStageExec])
      finalPlan.doExecuteBroadcast()
    }
  }

  private def withFinalPlanUpdate[T](fun: SparkPlan => T): T = {
    val plan = getFinalPhysicalPlan()
    val result = fun(plan)
    finalPlanUpdate
    result
  }

  protected override def stringArgs: Iterator[Any] = Iterator(s"isFinalPlan=$isFinalPlan")

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean,
      indent: Int = 0): Unit = {
    super.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      prefix,
      addSuffix,
      maxFields,
      printNodeId,
      indent)
    if (currentPhysicalPlan.fastEquals(initialPlan)) {
      currentPhysicalPlan.generateTreeString(
        depth + 1,
        lastChildren :+ true,
        append,
        verbose,
        prefix = "",
        addSuffix = false,
        maxFields,
        printNodeId,
        indent)
    } else {
      generateTreeStringWithHeader(
        if (isFinalPlan) "Final Plan" else "Current Plan",
        currentPhysicalPlan,
        depth,
        append,
        verbose,
        maxFields,
        printNodeId)
      generateTreeStringWithHeader(
        "Initial Plan",
        initialPlan,
        depth,
        append,
        verbose,
        maxFields,
        printNodeId)
    }
  }


  private def generateTreeStringWithHeader(
      header: String,
      plan: SparkPlan,
      depth: Int,
      append: String => Unit,
      verbose: Boolean,
      maxFields: Int,
      printNodeId: Boolean): Unit = {
    append("   " * depth)
    append(s"+- == $header ==\n")
    plan.generateTreeString(
      0,
      Nil,
      append,
      verbose,
      prefix = "",
      addSuffix = false,
      maxFields,
      printNodeId,
      indent = depth + 1)
  }

  override def hashCode(): Int = inputPlan.hashCode()

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[AdaptiveSparkPlanExec]) {
      return false
    }

    this.inputPlan == obj.asInstanceOf[AdaptiveSparkPlanExec].inputPlan
  }

  /**
   * 让我用一个具体的查询计划来说明：
   *
   * sql
   *
   * SELECT t1.id, t2.name
   * FROM table1 t1
   *    JOIN table2 t2 ON t1.id = t2.id
   * WHERE t1.status = 'active'
   *
   *
   * #### **初始物理计划**:
   * SortMergeJoin
   * ├── ShuffleExchange(HashPartitioning)  // Exchange1
   * │   └── Filter(status = 'active')
   * │       └── Scan(table1)
   * └── ShuffleExchange(HashPartitioning)  // Exchange2
   *      └── Scan(table2)
   *
   *
   * #### **第一次调用 createQueryStages(SortMergeJoin)**:
   *
   * 处理过程:
   * 1. SortMergeJoin 不是 Exchange，进入最后一个 case
   * 2. 递归处理两个子节点 (Exchange1 和 Exchange2)
   *
   * 对 Exchange1 的处理:
   * scala
   * // createQueryStages(Exchange1)
   * val result = createQueryStages(Filter(...))  // 递归处理子节点
   *
   * // Filter 节点的结果
   * CreateStageResult(
   *   newPlan = Filter(...),
   *   allChildStagesMaterialized = true,    // 叶子节点，已物化
   *   newStages = Seq.empty                 // 没有新阶段
   * )
   *
   * // 由于子节点已物化，可以创建 QueryStage
   * val newStage = ShuffleQueryStageExec(0, Exchange1, ...)
   *
   * // 返回结果
   * CreateStageResult(
   *   newPlan = ShuffleQueryStageExec(0, ...),  // Exchange1 被替换为 QueryStage
   *   allChildStagesMaterialized = false,       // 新创建的阶段未物化
   *   newStages = Seq(ShuffleQueryStageExec(0, ...))  // 新创建的阶段
   * )
   *
   *
   * 对 Exchange2 的处理:
   * scala
   * // 类似地创建 ShuffleQueryStageExec(1, ...)
   * CreateStageResult(
   *   newPlan = ShuffleQueryStageExec(1, ...),
   *   allChildStagesMaterialized = false,
   *   newStages = Seq(ShuffleQueryStageExec(1, ...))
   * )
   *
   *
   * SortMergeJoin 的最终结果:
   * scala
   * CreateStageResult(
   *   newPlan = SortMergeJoin(
   *   ShuffleQueryStageExec(0, ...),  // 左子树
   *   ShuffleQueryStageExec(1, ...)   // 右子树
   * ),
   * allChildStagesMaterialized = false,  // 因为子阶段都未物化
   * newStages = Seq(
   *   ShuffleQueryStageExec(0, ...),
   *   ShuffleQueryStageExec(1, ...)
   *  )
   * )
   *
   *
   * ### **4. 在 AQE 主循环中的使用**
   *
   * scala
   * // 在 getFinalPhysicalPlan 中
   * var result = createQueryStages(currentPhysicalPlan)
   *
   * while (!result.allChildStagesMaterialized) {
   *   currentPhysicalPlan = result.newPlan
   *
   *   if (result.newStages.nonEmpty) {
   *     // 提交新阶段进行物化
   *     result.newStages.foreach { stage =>
   *       stage.materialize().onComplete { ... }
   *     }
   *   }
   *
   *   // 等待阶段完成...
   *   // 重新优化...
   *
   *   // 再次创建阶段
   *   result = createQueryStages(currentPhysicalPlan)
   * }
   *
   *
   * 该方法递归调用以自下而上地遍历计划树，并创建新查询阶段，或者在当前节点是 [[Exchange]] 节点且其所有子阶段都已物化时尝试重用现有阶段。
   *
   * 从叶子节点开始向上遍历
   * 当遇到 Exchange 节点且其所有子阶段都已物化时，创建新的 QueryStageExec
   * 通过阶段缓存实现 Exchange 重用优化
   */
  private def createQueryStages(plan: SparkPlan): CreateStageResult = plan match {


    case e: Exchange =>
      // First have a quick check in the `stageCache` without having to traverse down the node.
      context.stageCache.get(e.canonicalized) match {   //  当前 exchanger 已经存在
        case Some(existingStage) if conf.exchangeReuseEnabled =>
          // // 找到可重用的阶段, 复用重用的阶段，
          val stage = reuseQueryStage(existingStage, e)
          val isMaterialized = stage.isMaterialized
          CreateStageResult(
            newPlan = stage,
            allChildStagesMaterialized = isMaterialized,
            newStages = if (isMaterialized) Seq.empty else Seq(stage))

        case _ =>
          // 递归处理 Exchage 子节点
          val result = createQueryStages(e.child)

          // 如果子节点中没有 exchange 节点，则不改变， 否则，子节点中的 exchage 被替换
          val newPlan = e.withNewChildren(Seq(result.newPlan)).asInstanceOf[Exchange]  // 生成新的执行节点



          // 所有子节点都被物化以后才创建
          if (result.allChildStagesMaterialized) {    // 所有子阶段都已物化，可以创建新的查询阶段

            // 在创建时并且进行优化
            var newStage = newQueryStage(newPlan)   // 对当前的 Exchange 节点创建 QueryStageExec(ShuffleQueryStageExec, BroadcastQueryStageExec)

            if (conf.exchangeReuseEnabled) {        // spark.sql.exchange.reuse
              // 再次检查 `stageCache` 以进行重用。如果找到匹配项，则放弃新阶段
              // 并重用在 `stageCache` 中找到的现有阶段，否则使用新阶段更新
              // `stageCache`。
              val queryStage = context.stageCache.getOrElseUpdate(
                newStage.plan.canonicalized, newStage)
              if (queryStage.ne(newStage)) {
                newStage = reuseQueryStage(queryStage, e)
              }
            }
            val isMaterialized = newStage.isMaterialized
            CreateStageResult(
              newPlan = newStage,
              allChildStagesMaterialized = isMaterialized,
              newStages = if (isMaterialized) Seq.empty else Seq(newStage))
          } else {
            CreateStageResult(newPlan = newPlan,
              allChildStagesMaterialized = false, newStages = result.newStages)
          }
      }

    case q: QueryStageExec =>
      CreateStageResult(newPlan = q,
        allChildStagesMaterialized = q.isMaterialized, newStages = Seq.empty)

    case _ =>
      if (plan.children.isEmpty) {
        CreateStageResult(newPlan = plan, allChildStagesMaterialized = true, newStages = Seq.empty)  // 叶子节点， 已经物化，没有新阶段
      } else {
        val results = plan.children.map(createQueryStages)    // 遍历子逻辑树类型
        CreateStageResult(
          newPlan = plan.withNewChildren(results.map(_.newPlan)),
          allChildStagesMaterialized = results.forall(_.allChildStagesMaterialized),
          newStages = results.flatMap(_.newStages))
      }
  }

  private def newQueryStage(e: Exchange): QueryStageExec = {
    // 对  Exchange 子节点进行查询优化
    val optimizedPlan = optimizeQueryStage(e.child, isFinalStage = false)

    val queryStage = e match {
      case s: ShuffleExchangeLike =>
        val newShuffle = applyPhysicalRules(
          s.withNewChildren(Seq(optimizedPlan)),
          postStageCreationRules(outputsColumnar = s.supportsColumnar),
          Some((planChangeLogger, "AQE Post Stage Creation")))
        if (!newShuffle.isInstanceOf[ShuffleExchangeLike]) {
          throw new IllegalStateException(
            "Custom columnar rules cannot transform shuffle node to something else.")
        }
        ShuffleQueryStageExec(currentStageId, newShuffle, s.canonicalized)
      case b: BroadcastExchangeLike =>
        val newBroadcast = applyPhysicalRules(
          b.withNewChildren(Seq(optimizedPlan)),
          postStageCreationRules(outputsColumnar = b.supportsColumnar),
          Some((planChangeLogger, "AQE Post Stage Creation")))
        if (!newBroadcast.isInstanceOf[BroadcastExchangeLike]) {
          throw new IllegalStateException(
            "Custom columnar rules cannot transform broadcast node to something else.")
        }
        BroadcastQueryStageExec(currentStageId, newBroadcast, b.canonicalized)
    }
    currentStageId += 1
    setLogicalLinkForNewQueryStage(queryStage, e)
    queryStage
  }

  private def reuseQueryStage(existing: QueryStageExec, exchange: Exchange): QueryStageExec = {
    val queryStage = existing.newReuseInstance(currentStageId, exchange.output)
    currentStageId += 1
    setLogicalLinkForNewQueryStage(queryStage, exchange)
    queryStage
  }

  /**
   * Set the logical node link of the `stage` as the corresponding logical node of the `plan` it
   * encloses. If an `plan` has been transformed from a `Repartition`, it should have `logicalLink`
   * available by itself; otherwise traverse down to find the first node that is not generated by
   * `EnsureRequirements`.
   */
  private def setLogicalLinkForNewQueryStage(stage: QueryStageExec, plan: SparkPlan): Unit = {
    val link = plan.getTagValue(TEMP_LOGICAL_PLAN_TAG).orElse(
      plan.logicalLink.orElse(plan.collectFirst {
        case p if p.getTagValue(TEMP_LOGICAL_PLAN_TAG).isDefined =>
          p.getTagValue(TEMP_LOGICAL_PLAN_TAG).get
        case p if p.logicalLink.isDefined => p.logicalLink.get
      }))
    assert(link.isDefined)
    stage.setLogicalLink(link.get)
  }

  /**
   * 对于 `stagesToReplace` 中的每个查询阶段，在 `logicalPlan` 中找到它们对应的逻辑节点，
   * 并用新的 [[LogicalQueryStage]] 节点替换它们。
   *
   * 1. 如果查询阶段可以映射到一个完整的逻辑子树，则用引用该查询阶段的叶子节点 [[LogicalQueryStage]]
   *    替换相应的逻辑子树。例如：
   *        Join                   SMJ                      SMJ
   *      /     \                /    \                   /    \
   *    r1      r2    =>    Xchg1     Xchg2    =>    Stage1     Stage2
   *                          |        |
   *                          r1       r2
   *    更新后的计划节点将是：
   *                               Join
   *                             /     \
   *    LogicalQueryStage1(Stage1)     LogicalQueryStage2(Stage2)
   *
   * 2. 否则（这意味着查询阶段只能映射到逻辑子树的一部分），
   *    用引用在物理规划期间该逻辑节点转换成的顶层物理节点的叶子节点 [[LogicalQueryStage]]
   *    替换相应的逻辑子树。例如：
   *     Agg           HashAgg          HashAgg
   *      |               |                |
   *    child    =>     Xchg      =>     Stage1
   *                      |
   *                   HashAgg
   *                      |
   *                    child
   *    更新后的计划节点将是：
   *    LogicalQueryStage(HashAgg - Stage1)
   */
  private def replaceWithQueryStagesInLogicalPlan(
      plan: LogicalPlan,
      stagesToReplace: Seq[QueryStageExec]): LogicalPlan = {

    var logicalPlan = plan

    stagesToReplace.foreach {
      case stage if currentPhysicalPlan.exists(_.eq(stage)) =>

        val logicalNodeOpt = stage.getTagValue(TEMP_LOGICAL_PLAN_TAG).orElse(stage.logicalLink)

        assert(logicalNodeOpt.isDefined)

        val logicalNode = logicalNodeOpt.get

        val physicalNode = currentPhysicalPlan.collectFirst {
          case p if p.eq(stage) ||
            p.getTagValue(TEMP_LOGICAL_PLAN_TAG).exists(logicalNode.eq) ||
            p.logicalLink.exists(logicalNode.eq) => p
        }
        assert(physicalNode.isDefined)
        // Set the temp link for those nodes that are wrapped inside a `LogicalQueryStage` node for
        // they will be shared and reused by different physical plans and their usual logical links
        // can be overwritten through re-planning processes.
        setTempTagRecursive(physicalNode.get, logicalNode)
        // Replace the corresponding logical node with LogicalQueryStage
        val newLogicalNode = LogicalQueryStage(logicalNode, physicalNode.get)
        val newLogicalPlan = logicalPlan.transformDown {
          case p if p.eq(logicalNode) => newLogicalNode
        }
        logicalPlan = newLogicalPlan

      case _ => // Ignore those earlier stages that have been wrapped in later stages.
    }
    logicalPlan
  }

  /**
   * 重新优化这个逻辑计划树，跟对应的物理计划树
   */
  private def reOptimize(logicalPlan: LogicalPlan): Option[(SparkPlan, LogicalPlan)] = {
    try {

      logicalPlan.invalidateStatsCache()  // 当前逻辑计划树的指标失效


      val optimized = optimizer.execute(logicalPlan)  // 使用AQE 优化器重新优化该算子

      // 生成物理计划
      val sparkPlan = context.session.sessionState.planner.plan(ReturnAnswer(optimized)).next()

      // 优化物理计划
      val newPlan = applyPhysicalRules(sparkPlan, preprocessingRules ++ queryStagePreparationRules, Some((planChangeLogger, "AQE Replanning")))


      // 当同时启用AQE（自适应查询执行）和DPP（动态分区裁剪）时，PlanAdaptiveDynamicPruningFilters规则会手动在DPP子查询中添加BroadcastExchangeExec节点，
      // 而不是通过EnsureRequirements规则。
      // 因此，当DPP子查询较为复杂且需要重新优化时，AQE也需要手动插入BroadcastExchangeExec节点，以防止DPP子查询中的BroadcastExchangeExec节点丢失。
      // 此外，在应用LogicalQueryStageStrategy规则后，如果新计划已经是BroadcastExchangeExec计划，我们也需要避免重复插入该节点。
      val finalPlan = inputPlan match {
        case b: BroadcastExchangeLike
          if (!newPlan.isInstanceOf[BroadcastExchangeLike]) => b.withNewChildren(Seq(newPlan))
        case _ => newPlan
      }

      Some((finalPlan, optimized))
    } catch {
      case e: InvalidAQEPlanException[_] =>
        logOnLevel(s"Re-optimize - ${e.getMessage()}:\n${e.plan}")
        None
    }
  }

  /**
   * Recursively set `TEMP_LOGICAL_PLAN_TAG` for the current `plan` node.
   */
  private def setTempTagRecursive(plan: SparkPlan, logicalPlan: LogicalPlan): Unit = {
    plan.setTagValue(TEMP_LOGICAL_PLAN_TAG, logicalPlan)
    plan.children.foreach(c => setTempTagRecursive(c, logicalPlan))
  }

  /**
   * Unset all `TEMP_LOGICAL_PLAN_TAG` tags.
   */
  private def cleanUpTempTags(plan: SparkPlan): Unit = {
    plan.foreach {
      case plan: SparkPlan if plan.getTagValue(TEMP_LOGICAL_PLAN_TAG).isDefined =>
        plan.unsetTagValue(TEMP_LOGICAL_PLAN_TAG)
      case _ =>
    }
  }

  /**
   * 通知 Listener 物理计划被改变
   */
  private def onUpdatePlan(executionId: Long, newSubPlans: Seq[SparkPlan]): Unit = {
    if (isSubquery) {
      // When executing subqueries, we can't update the query plan in the UI as the
      // UI doesn't support partial update yet. However, the subquery may have been
      // optimized into a different plan and we must let the UI know the SQL metrics
      // of the new plan nodes, so that it can track the valid accumulator updates later
      // and display SQL metrics correctly.
      val newMetrics = newSubPlans.flatMap { p =>
        p.flatMap(_.metrics.values.map(m => SQLPlanMetric(m.name.get, m.id, m.metricType)))
      }
      context.session.sparkContext.listenerBus.post(SparkListenerSQLAdaptiveSQLMetricUpdates(
        executionId, newMetrics))
    } else {
      val planDescriptionMode = ExplainMode.fromString(conf.uiExplainMode)
      context.session.sparkContext.listenerBus.post(SparkListenerSQLAdaptiveExecutionUpdate(
        executionId,
        context.qe.explainString(planDescriptionMode),
        SparkPlanInfo.fromSparkPlan(context.qe.executedPlan)))
    }
  }

  /**
   * Cancel all running stages with best effort and throw an Exception containing all stage
   * materialization errors and stage cancellation errors.
   */
  private def cleanUpAndThrowException(
       errors: Seq[Throwable],
       earlyFailedStage: Option[Int]): Unit = {
    currentPhysicalPlan.foreach {
      // earlyFailedStage is the stage which failed before calling doMaterialize,
      // so we should avoid calling cancel on it to re-trigger the failure again.
      case s: QueryStageExec if !earlyFailedStage.contains(s.id) =>
        try {
          s.cancel()
        } catch {
          case NonFatal(t) =>
            logError(s"Exception in cancelling query stage: ${s.treeString}", t)
        }
      case _ =>
    }
    // Respect SparkFatalException which can be thrown by BroadcastExchangeExec
    val originalErrors = errors.map {
      case fatal: SparkFatalException => fatal.throwable
      case other => other
    }
    val e = if (originalErrors.size == 1) {
      originalErrors.head
    } else {
      val se = QueryExecutionErrors.multiFailuresInStageMaterializationError(originalErrors.head)
      originalErrors.tail.foreach(se.addSuppressed)
      se
    }
    throw e
  }
}

object AdaptiveSparkPlanExec {
  private[adaptive] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("QueryStageCreator", 16))

  /**
   * The temporary [[LogicalPlan]] link for query stages.
   *
   * Physical nodes wrapped in a [[LogicalQueryStage]] can be shared among different physical plans
   * and thus their usual logical links can be overwritten during query planning, leading to
   * situations where those nodes point to a new logical plan and the rest point to the current
   * logical plan. In this case we use temp logical links to make sure we can always trace back to
   * the original logical links until a new physical plan is adopted, by which time we can clear up
   * the temp logical links.
   */
  val TEMP_LOGICAL_PLAN_TAG = TreeNodeTag[LogicalPlan]("temp_logical_plan")

  /**
   * Apply a list of physical operator rules on a [[SparkPlan]].
   */
  def applyPhysicalRules(
      plan: SparkPlan,
      rules: Seq[Rule[SparkPlan]],
      loggerAndBatchName: Option[(PlanChangeLogger[SparkPlan], String)] = None): SparkPlan = {
    if (loggerAndBatchName.isEmpty) {
      rules.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
    } else {
      val (logger, batchName) = loggerAndBatchName.get
      val newPlan = rules.foldLeft(plan) { case (sp, rule) =>
        val result = rule.apply(sp)
        logger.logRule(rule.ruleName, sp, result)
        result
      }
      logger.logBatch(batchName, plan, newPlan)
      newPlan
    }
  }
}

/**
 * The execution context shared between the main query and all sub-queries.
 * 复用指的是：当 AQE 发现两个或多个 Exchange 节点在逻辑上等价时，可以共享同一个已经物化的 QueryStage，避免重复计算。
 *
 * #### **例子1: 相同子查询的复用**
 *
 * sql
 * SELECT
 *   (SELECT COUNT(*) FROM orders WHERE customer_id = c.id) as order_count,
 *   (SELECT COUNT(*) FROM orders WHERE customer_id = c.id) as order_count_copy
 * FROM customers c
 *
 *
 * 物理计划:
 * Project
 * ├── Subquery1: Aggregate(COUNT)
 * │   └── ShuffleExchange(HashPartitioning(customer_id))  ← Exchange1
 * │       └── Filter(customer_id = c.id)
 * │           └── Scan(orders)
 * └── Subquery2: Aggregate(COUNT)
 *       └── ShuffleExchange(HashPartitioning(customer_id))  ← Exchange2 (与Exchange1相同!)
 *           └── Filter(customer_id = c.id)
 *               └── Scan(orders)
 *
 *
 * 复用结果:
 * Project
 * ├── Subquery1: Aggregate(COUNT)
 * │   └── ShuffleQueryStageExec(0)  ← 原始阶段
 * └── Subquery2: Aggregate(COUNT)
 *      └── ShuffleQueryStageExec(1)  ← 复用阶段，但有新的ID和输出
 *           └── ReusedExchangeExec ← 指向同一个物化结果
 */
case class AdaptiveExecutionContext(session: SparkSession, qe: QueryExecution) {

  /**
   * The subquery-reuse map shared across the entire query.
   */
  val subqueryCache: TrieMap[SparkPlan, BaseSubqueryExec] =
    new TrieMap[SparkPlan, BaseSubqueryExec]()

  /**
   * The exchange-reuse map shared across the entire query, including sub-queries.
   */
  val stageCache: TrieMap[SparkPlan, QueryStageExec] =
    new TrieMap[SparkPlan, QueryStageExec]()
}

/**
 * The event type for stage materialization.
 */
sealed trait StageMaterializationEvent

/**
 * The materialization of a query stage completed with success.
 */
case class StageSuccess(stage: QueryStageExec, result: Any) extends StageMaterializationEvent

/**
 * The materialization of a query stage hit an error and failed.
 */
case class StageFailure(stage: QueryStageExec, error: Throwable) extends StageMaterializationEvent
