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

package org.apache.spark.sql.hive.execution

import java.util.Locale

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.ErrorMsg
import org.apache.hadoop.hive.ql.plan.TableDesc

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.CommandUtils
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.hive.HiveShim.{ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.hive.client.hive._


/**
 * 写出数据到一个 Hive 表的命令。
 *
 * 由于历史原因，这个类主要是一团乱麻（因为它是有机地演变发展的，必须紧密遵循 Hive 内部的实现，而 Hive 本身也是一团乱麻）。
 * 请不要责怪 Reynold！他只是在移动代码！
 *
 * 在未来，我们应该将 Hive 的写入路径与普通数据源写入路径合并，
 * 如 org.apache.spark.sql.execution.datasources.FileFormatWriter 中所定义的那样。
 *
 * @param table        表的元数据。
 * @param partition    一个从分区键到分区值的映射（可选）。如果分区值是可选的，将执行动态分区插入。
 *
 *                  INSERT INTO tbl PARTITION (a=1, b=2) AS ...
 *                  {{{
 *                  Map('a' -> Some('1'), 'b' -> Some('2'))
 *                  }}}
 *
 *                   `INSERT INTO tbl PARTITION (a=1, b) AS ...`
 *                  {{{
 *                  Map('a' -> Some('1'), 'b' -> None)
 *                  }}}.
 * @param query 表示要写入数据的逻辑计划。
 * @param overwrite 覆盖现有表或分区。
 * @param ifPartitionNotExists 如果为真，则仅在分区不存在时进行写入。仅对静态分区有效。
 */
case class InsertIntoHiveTable(
    table: CatalogTable,
    partition: Map[String, Option[String]],
    query: LogicalPlan,
    overwrite: Boolean,
    ifPartitionNotExists: Boolean,
    outputColumnNames: Seq[String]) extends SaveAsHiveFile {

  /**
   * 将表中的所有行插入到 Hive 中。
   * 行对象会使用表定义提供的 org.apache.hadoop.hive.serde2.SerDe
   * org.apache.hadoop.mapred.OutputFormat 正确地序列化。
   */
  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    val externalCatalog = sparkSession.sharedState.externalCatalog
    val hadoopConf = sparkSession.sessionState.newHadoopConf()

    val hiveQlTable = HiveClientImpl.toHiveTable(table)

    // 必须将 TableDesc 对象传递给 RDD.mapPartitions，
    // 然后在闭包内实例化新的序列化器实例，因为 Serializer 不可序列化，而 TableDesc 可序列化。
    val tableDesc = new TableDesc(
      hiveQlTable.getInputFormatClass,
      // 表的类应该是 org.apache.hadoop.hive.ql.metadata.Table，
      // 因为 getOutputFormatClass 将使用 HiveFileFormatUtils.getOutputFormatSubstitute 来替换一些输出格式，
      // 例如将 SequenceFileOutputFormat 替换为HiveSequenceFileOutputFormat。
      hiveQlTable.getOutputFormatClass,
      hiveQlTable.getMetadata
    )
    val tableLocation = hiveQlTable.getDataLocation

    // 获取表的临时目录:  位置 hive.exec.stagingdir
    // 默认 ： ${TablePath}/.hive-staging
    val tmpLocation = getExternalTmpPath(sparkSession, hadoopConf, tableLocation)

    try {
      processInsert(sparkSession, externalCatalog, hadoopConf, tableDesc, tmpLocation, child)
    } finally {
      // 尝试删除临时目录及其中包含的文件。
      // 如果失败，则文件应在 VM 正常终止时被删除，因为使用了 deleteOnExit。
      deleteExternalTmpPath(hadoopConf)
    }

    // 刷新当前表的缓存信息
    CommandUtils.uncacheTableOrView(sparkSession, table.identifier.quotedString)
    sparkSession.sessionState.catalog.refreshTable(table.identifier)

    // 更新表的统计信息
    CommandUtils.updateTableStats(sparkSession, table)

    // It would be nice to just return the childRdd unchanged so insert operations could be chained,
    // however for now we return an empty list to simplify compatibility checks with hive, which
    // does not return anything for insert operations.
    // TODO: implement hive compatibility as rules.
    Seq.empty[Row]
  }

  /***
   * 数据写入表操作
   * @param sparkSession
   * @param externalCatalog  catalog操作接口
   * @param hadoopConf
   * @param tableDesc    要写入的表信息
   * @param tmpLocation  当前写入的临时目录
   * @param child        计算子树节点
   */
  private def processInsert(
      sparkSession: SparkSession,
      externalCatalog: ExternalCatalog,
      hadoopConf: Configuration,
      tableDesc: TableDesc,
      tmpLocation: Path,
      child: SparkPlan): Unit = {
    val fileSinkConf = new FileSinkDesc(tmpLocation.toString, tableDesc, false)

    val numDynamicPartitions = partition.values.count(_.isEmpty)   // 动态分区数量
    val numStaticPartitions = partition.values.count(_.nonEmpty)   // 静态分区数量

    val partitionSpec = partition.map {
      case (key, Some(null)) => key -> ExternalCatalogUtils.DEFAULT_PARTITION_NAME
      case (key, Some(value)) => key -> value
      case (key, None) => key -> ""
    }

    // 从表元信息中获取到表的分区属性
    val partitionColumns = fileSinkConf.getTableInfo.getProperties.getProperty("partition_columns")
    val partitionColumnNames = Option(partitionColumns).map(_.split("/")).getOrElse(Array.empty)

    // 校验SQL中的分区映射，必须跟表元信息中的分区映射相匹配
    if (partitionColumnNames.toSet != partition.keySet) {
      throw QueryExecutionErrors.requestedPartitionsMismatchTablePartitionsError(table, partition)
    }

    // 如果存在任何动态分区，则验证分区规范。
    if (numDynamicPartitions > 0) {
      // 如果未启用动态分区，则报告错误。
      if (!hadoopConf.get("hive.exec.dynamic.partition", "true").toBoolean) {
        throw new SparkException(ErrorMsg.DYNAMIC_PARTITION_DISABLED.getMsg)
      }

      // 如果动态分区strict mode 开启但未找到静态分区，则报告错误。
      if (numStaticPartitions == 0 &&
        hadoopConf.get("hive.exec.dynamic.partition.mode", "strict").equalsIgnoreCase("strict")) {
        throw new SparkException(ErrorMsg.DYNAMIC_PARTITION_STRICT_MODE.getMsg)
      }

      // 如果在动态分区后出现任何静态分区，则报告错误。
      val isDynamic = partitionColumnNames.map(partitionSpec(_).isEmpty)
      if (isDynamic.init.zip(isDynamic.tail).contains((true, false))) {
        throw new AnalysisException(ErrorMsg.PARTITION_DYN_STA_ORDER.getMsg)
      }
    }

    val partitionAttributes = partitionColumnNames.takeRight(numDynamicPartitions).map { name =>
      val attr = query.resolve(name :: Nil, sparkSession.sessionState.analyzer.resolver).getOrElse {
        throw QueryCompilationErrors.cannotResolveAttributeError(
          name, query.output.map(_.name).mkString(", "))
      }.asInstanceOf[Attribute]
      // SPARK-28054: Hive metastore 不区分大小写并保留小写名称的分区列。
      // Hive 将在 loadDynamicPartitions 过程中验证分区目录中的列名。
      // 为了使 loadDynamicPartitions 正常工作，Spark 需要使用小写的列名来写入分区目录。
      attr.withName(name.toLowerCase(Locale.ROOT))
    }

    val writtenParts = saveAsHiveFile(
      sparkSession = sparkSession,
      plan = child,
      hadoopConf = hadoopConf,
      fileSinkConf = fileSinkConf,
      outputLocation = tmpLocation.toString,
      partitionAttributes = partitionAttributes,
      bucketSpec = table.bucketSpec)

    if (partition.nonEmpty) {
      if (numDynamicPartitions > 0) {
        if (overwrite && table.tableType == CatalogTableType.EXTERNAL) {
          val numWrittenParts = writtenParts.size
          val maxDynamicPartitionsKey = HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS.varname
          val maxDynamicPartitions = hadoopConf.getInt(maxDynamicPartitionsKey,
            HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS.defaultIntVal)
          if (numWrittenParts > maxDynamicPartitions) {
            throw QueryExecutionErrors.writePartitionExceedConfigSizeWhenDynamicPartitionError(
              numWrittenParts, maxDynamicPartitions, maxDynamicPartitionsKey)
          }
          // SPARK-29295: When insert overwrite to a Hive external table partition, if the
          // partition does not exist, Hive will not check if the external partition directory
          // exists or not before copying files. So if users drop the partition, and then do
          // insert overwrite to the same partition, the partition will have both old and new
          // data. We construct partition path. If the path exists, we delete it manually.
          writtenParts.foreach { partPath =>
            val dpMap = partPath.split("/").map { part =>
              val splitPart = part.split("=")
              assert(splitPart.size == 2, s"Invalid written partition path: $part")
              ExternalCatalogUtils.unescapePathName(splitPart(0)) ->
                ExternalCatalogUtils.unescapePathName(splitPart(1))
            }.toMap

            val caseInsensitiveDpMap = CaseInsensitiveMap(dpMap)

            val updatedPartitionSpec = partition.map {
              case (key, Some(null)) => key -> ExternalCatalogUtils.DEFAULT_PARTITION_NAME
              case (key, Some(value)) => key -> value
              case (key, None) if caseInsensitiveDpMap.contains(key) =>
                key -> caseInsensitiveDpMap(key)
              case (key, _) =>
                throw QueryExecutionErrors.dynamicPartitionKeyNotAmongWrittenPartitionPathsError(
                  key)
            }
            val partitionColumnNames = table.partitionColumnNames
            val tablePath = new Path(table.location)
            val partitionPath = ExternalCatalogUtils.generatePartitionPath(updatedPartitionSpec,
              partitionColumnNames, tablePath)

            val fs = partitionPath.getFileSystem(hadoopConf)
            if (fs.exists(partitionPath)) {
              if (!fs.delete(partitionPath, true)) {
                throw QueryExecutionErrors.cannotRemovePartitionDirError(partitionPath)
              }
            }
          }
        }

        externalCatalog.loadDynamicPartitions(
          db = table.database,
          table = table.identifier.table,
          tmpLocation.toString,
          partitionSpec,
          overwrite,
          numDynamicPartitions)
      } else {
        // scalastyle:off
        // ifNotExists is only valid with static partition, refer to
        // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-InsertingdataintoHiveTablesfromqueries
        // scalastyle:on
        val oldPart =
          externalCatalog.getPartitionOption(
            table.database,
            table.identifier.table,
            partitionSpec)

        var doHiveOverwrite = overwrite

        if (oldPart.isEmpty || !ifPartitionNotExists) {
          // SPARK-29295: When insert overwrite to a Hive external table partition, if the
          // partition does not exist, Hive will not check if the external partition directory
          // exists or not before copying files. So if users drop the partition, and then do
          // insert overwrite to the same partition, the partition will have both old and new
          // data. We construct partition path. If the path exists, we delete it manually.
          val partitionPath = if (oldPart.isEmpty && overwrite
              && table.tableType == CatalogTableType.EXTERNAL) {
            val partitionColumnNames = table.partitionColumnNames
            val tablePath = new Path(table.location)
            Some(ExternalCatalogUtils.generatePartitionPath(partitionSpec,
              partitionColumnNames, tablePath))
          } else {
            oldPart.flatMap(_.storage.locationUri.map(uri => new Path(uri)))
          }

          // SPARK-18107: Insert overwrite runs much slower than hive-client.
          // Newer Hive largely improves insert overwrite performance. As Spark uses older Hive
          // version and we may not want to catch up new Hive version every time. We delete the
          // Hive partition first and then load data file into the Hive partition.
          val hiveVersion = externalCatalog.asInstanceOf[ExternalCatalogWithListener]
            .unwrapped.asInstanceOf[HiveExternalCatalog]
            .client
            .version
          // SPARK-31684:
          // For Hive 2.0.0 and onwards, as https://issues.apache.org/jira/browse/HIVE-11940
          // has been fixed, and there is no performance issue anymore. We should leave the
          // overwrite logic to hive to avoid failure in `FileSystem#checkPath` when the table
          // and partition locations do not belong to the same `FileSystem`
          // TODO(SPARK-31675): For Hive 2.2.0 and earlier, if the table and partition locations
          // do not belong together, we will still get the same error thrown by hive encryption
          // check. see https://issues.apache.org/jira/browse/HIVE-14380.
          // So we still disable for Hive overwrite for Hive 1.x for better performance because
          // the partition and table are on the same cluster in most cases.
          if (partitionPath.nonEmpty && overwrite && hiveVersion < v2_0) {
            partitionPath.foreach { path =>
              val fs = path.getFileSystem(hadoopConf)
              if (fs.exists(path)) {
                if (!fs.delete(path, true)) {
                  throw QueryExecutionErrors.cannotRemovePartitionDirError(path)
                }
                // Don't let Hive do overwrite operation since it is slower.
                doHiveOverwrite = false
              }
            }
          }

          // inheritTableSpecs is set to true. It should be set to false for an IMPORT query
          // which is currently considered as a Hive native command.
          val inheritTableSpecs = true
          externalCatalog.loadPartition(
            table.database,
            table.identifier.table,
            tmpLocation.toString,
            partitionSpec,
            isOverwrite = doHiveOverwrite,
            inheritTableSpecs = inheritTableSpecs,
            isSrcLocal = false)
        }
      }
    } else {
      externalCatalog.loadTable(
        table.database,
        table.identifier.table,
        tmpLocation.toString, // TODO: URI
        overwrite,
        isSrcLocal = false)
    }
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): InsertIntoHiveTable =
    copy(query = newChild)
}
