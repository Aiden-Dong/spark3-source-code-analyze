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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.execution.FileRelation
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister}
import org.apache.spark.sql.types.{StructField, StructType}


/**
 * 充当从数据源读取所需的所有元数据的容器。
 * 所有关于模式和分区的发现、解析和合并逻辑都已移除。
 *
 * @param location 一个 [[FileIndex]]，它可以列举出组成这个关系的所有文件的位置。  [[CatalogFileIndex]]
 * @param partitionSchema 用于分区关系的列（如果有的话）的Schema。
 * @param dataSchema 任何剩余列的schema。请注意，如果实际数据文件中存在任何分区列，它们也会被保留。
 * @param bucketSpec 描述了桶分配（根据某些列值对文件进行哈希分区）。
 * @param fileFormat 一个可以用来读取和写入文件中数据的文件格式。
 * @param options 读取/写入数据时使用的配置。
 */
case class HadoopFsRelation(
    location: FileIndex,
    partitionSchema: StructType,
    // dataSchema 中的顶级列应该与实际的物理文件模式匹配，否则 ORC 数据源可能无法使用按序号模式。
    dataSchema: StructType,
    bucketSpec: Option[BucketSpec],
    fileFormat: FileFormat,
    options: Map[String, String])(val sparkSession: SparkSession)
  extends BaseRelation with FileRelation {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  // When data and partition schemas have overlapping columns, the output
  // schema respects the order of the data schema for the overlapping columns, and it
  // respects the data types of the partition schema.
  val (schema: StructType, overlappedPartCols: Map[String, StructField]) =
    PartitioningUtils.mergeDataAndPartitionSchema(dataSchema,
      partitionSchema, sparkSession.sessionState.conf.caseSensitiveAnalysis)

  override def toString: String = {
    fileFormat match {
      case source: DataSourceRegister => source.shortName()
      case _ => "HadoopFiles"
    }
  }

  override def sizeInBytes: Long = {
    val compressionFactor = sqlContext.conf.fileCompressionFactor
    (location.sizeInBytes * compressionFactor).toLong
  }


  override def inputFiles: Array[String] = location.inputFiles
}
