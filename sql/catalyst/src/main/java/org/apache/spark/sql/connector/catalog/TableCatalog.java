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

package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.errors.QueryCompilationErrors;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

/**
 * 用于操作表的 catalog 方法。
 *
 * TableCatalog 的实现可以是区分大小写的，也可以是不区分大小写的。
 * Spark 在传递 {@link Identifier 表标识符} 时不会进行修改。
 * 当 Catalyst 分析阶段为不区分大小写时，传递给 {@link #alterTable(Identifier, TableChange...)} 的字段名会被标准化，
 * 以匹配表模式中使用的大小写，用于更新、重命名或删除已有列。
 * @since 3.0.0
 */
@Evolving
public interface TableCatalog extends CatalogPlugin {

  /**
   * 用于指定表位置的保留属性。表的数据文件应位于该位置之下。
   * CREATE TABLE my_table (...)
   * USING parquet
   * LOCATION 'hdfs://path/to/table/data';
   */
  String PROP_LOCATION = "location";

  /**
   * 一个保留属性，用于指示表的位置是由系统管理的，而不是用户指定的。
   * 如果该属性为 "true"，则 SHOW CREATE TABLE 不会生成 LOCATION 子句。
   */
  String PROP_IS_MANAGED_LOCATION = "is_managed_location";

  /**
   * 一个保留属性，用于指定该表是通过 EXTERNAL 创建的。
   */
  String PROP_EXTERNAL = "external";

  /**
   * 一个保留属性，用于指定该表的描述信息。
   */
  String PROP_COMMENT = "comment";

  /**
   * 一个保留属性，用于指定该表的提供者（provider）。
   */
  String PROP_PROVIDER = "provider";

  /**
   * 一个保留属性，用于指定该表的所有者。
   */
  String PROP_OWNER = "owner";

  /**
   * 用于在表属性中传递 OPTIONS 的前缀。
   */
  String OPTION_PREFIX = "option.";

  /**
   * 从目录中列出某个命名空间下的所有表。
   * <p>
   * 如果目录支持视图，则此方法必须仅返回表的标识符，而不包含视图。
   *
   * @param namespace 多级命名空间
   * @return 表的 Identifier 数组
   * @throws NoSuchNamespaceException 如果命名空间不存在（可选抛出）。
   */
  Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException;

  /**
   * 通过 {@link Identifier} 从目录中加载表的元数据。
   * <p>
   * 如果目录支持视图，并且对于给定标识符存在的是视图而不是表，则必须抛出 {@link NoSuchTableException}。
   *
   * @param ident 表的标识符
   * @return 该表的元数据
   * @throws NoSuchTableException 如果该表不存在，或该标识符对应的是视图
   */
  Table loadTable(Identifier ident) throws NoSuchTableException;

  /**
   * 通过 {@link Identifier} 从目录中加载指定版本的表元数据。
   * <p>
   * 如果目录支持视图，并且给定标识符对应的是视图而非表，则必须抛出 {@link NoSuchTableException}。
   *
   * @param ident 表的标识符
   * @param version 表的版本号
   * @return 该版本表的元数据
   * @throws NoSuchTableException 如果该表不存在或对应的是视图
   */
  default Table loadTable(Identifier ident, String version) throws NoSuchTableException {
    throw QueryCompilationErrors.tableNotSupportTimeTravelError(ident);
  }

  /**
   * 通过 {@link Identifier} 从目录中加载指定时间点的表元数据。
   * <p>
   * 如果目录支持视图，并且给定标识符对应的是视图而非表，则必须抛出 {@link NoSuchTableException}。
   *
   * @param ident 表的标识符
   * @param timestamp 表的时间戳，表示自 UTC 1970-01-01 00:00:00 以来的微秒数
   * @return 该时间点对应的表的元数据
   * @throws NoSuchTableException 如果该表不存在或对应的是视图
   */
  default Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
    throw QueryCompilationErrors.tableNotSupportTimeTravelError(ident);
  }

  /**
   * 使指定 {@link Identifier} 表的缓存元数据失效。
   * <p>
   * 如果该表已被加载或缓存，则清除其缓存数据。如果该表不存在或未被缓存，则不执行任何操作。
   * 调用此方法时不应访问远程服务。
   *
   * @param ident 表的标识符
   */
  default void invalidateTable(Identifier ident) {
  }

  /**
   * 使用目录中的 {@link Identifier} 来测试某个表是否存在。
   * <p>
   * 如果目录支持视图，并且给定的标识符对应的是视图而不是表，则必须返回 false。
   *
   * @param ident 表的标识符
   * @return 如果表存在则返回 true，否则返回 false
   */
  default boolean tableExists(Identifier ident) {
    try {
      return loadTable(ident) != null;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  /**
   * Create a table in the catalog.
   *
   * @param ident a table identifier
   * @param schema the schema of the new table, as a struct type
   * @param partitions transforms to use for partitioning data in the table
   * @param properties a string map of table properties
   * @return metadata for the new table
   * @throws TableAlreadyExistsException If a table or view already exists for the identifier
   * @throws UnsupportedOperationException If a requested partition transform is not supported
   * @throws NoSuchNamespaceException If the identifier namespace does not exist (optional)
   */
  Table createTable(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException;

  /**
   * 对目录中的表应用一组 {@link TableChange} 变更。
   * <p>
   * 实现可能会拒绝某些请求的变更。如果有任何一个变更被拒绝，则不应对表应用任何变更。
   * <p>
   * 请求的变更必须按照给定的顺序依次应用。
   * <p>
   * 如果目录支持视图，并且给定的标识符对应的是视图而不是表，则必须抛出 {@link NoSuchTableException}。
   *
   * @param ident 表的标识符
   * @param changes 要应用于该表的变更集合
   * @return 更新后的表的元数据
   * @throws NoSuchTableException 如果该表不存在或对应的是视图
   * @throws IllegalArgumentException 如果实现拒绝了任何一个变更
   */
  Table alterTable(
      Identifier ident,
      TableChange... changes) throws NoSuchTableException;

  /**
   * 删除目录中的一个表。
   * <p>
   * 如果目录支持视图，并且给定的标识符对应的是视图而不是表，则此方法**不得**删除该视图，并且必须返回 false。
   *
   * @param ident 表的标识符
   * @return 如果删除了表则返回 true；如果该标识符没有对应的表，则返回 false
   */
  boolean dropTable(Identifier ident);

  /**
   * 从目录中删除一个表，并彻底移除其数据，即使支持回收站（trash），也要跳过它。
   * <p>
   * 如果目录支持视图，并且标识符对应的是视图而不是表，则不得删除该视图，并且必须返回 false。
   * <p>
   * 如果目录支持彻底清除（purge）表，则应该重写此方法。
   * 默认实现会抛出 {@link UnsupportedOperationException}。
   *
   * @param ident 表的标识符
   * @return 如果成功删除了表，则返回 true；如果该标识符没有对应的表，则返回 false
   * @throws UnsupportedOperationException 如果不支持表清除操作
   *
   * @since 3.1.0
   */
  default boolean purgeTable(Identifier ident) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Purge table is not supported.");
  }

  /**
   * 重命名目录中的一个表。
   * <p>
   * 如果目录支持视图，并且旧标识符对应的是视图而不是表，则抛出 {@link NoSuchTableException}。
   * 此外，如果新标识符已经存在（无论是表还是视图），则抛出 {@link TableAlreadyExistsException}。
   * <p>
   * 如果目录不支持跨命名空间重命名表，则抛出 {@link UnsupportedOperationException}。
   *
   * @param oldIdent 要重命名的现有表的标识符
   * @param newIdent 重命名后的新表标识符
   * @throws NoSuchTableException 如果要重命名的表不存在或是一个视图
   * @throws TableAlreadyExistsException 如果新表名已经存在或是一个视图
   * @throws UnsupportedOperationException 如果旧标识符和新标识符的命名空间不一致（可选）
   */
  void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException;
}
