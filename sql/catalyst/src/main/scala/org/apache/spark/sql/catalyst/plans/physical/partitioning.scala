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

package org.apache.spark.sql.catalyst.plans.physical

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, IntegerType}

/**
 * 指定在查询在多台机器上并行执行时，共享相同表达式的元组将如何分布。
 * 这里的分布是指 数据的节点间分区。
 * 也就是说，它描述了元组在集群中的物理机器上如何分区。
 * 了解此属性使一些操作符（例如，聚合）能够执行分区本地操作而不是全局操作。
 */
sealed trait Distribution {
  /**
   * 此分布所需的分区数量。如果为 None，则此分布允许任意数量的分区。
   */
  def requiredNumPartitions: Option[Int]

  /**
   * 创建此分布的默认分区方案，该方案可以满足此分布并匹配给定数量的分区。
   */
  def createPartitioning(numPartitions: Int): Partitioning
}

// 表示一个分布，关于数据的共同位置没有做出任何承诺。
case object UnspecifiedDistribution extends Distribution {
  override def requiredNumPartitions: Option[Int] = None

  override def createPartitioning(numPartitions: Int): Partitioning = {
    throw new IllegalStateException("UnspecifiedDistribution does not have default partitioning.")
  }
}

// 表示仅具有单个分区且数据集的所有元组均共同位于其中的分布。
case object AllTuples extends Distribution {
  override def requiredNumPartitions: Option[Int] = Some(1)

  override def createPartitioning(numPartitions: Int): Partitioning = {
    assert(numPartitions == 1, "The default partitioning of AllTuples can only have 1 partition.")
    SinglePartition
  }
}

/**
 *
 * 表示共享相同clustering值的元组的数据
 * [[Expression Expressions]] 将共享相同clustering值的元组放置在同一分区中
 *
 * @param requireAllClusterKeys 当为 true 时，
 *                              满足此分布的 Partitioning 必须按照相同的顺序匹配所有的 clustering 表达式。
 */
case class ClusteredDistribution(
    clustering: Seq[Expression],
    requireAllClusterKeys: Boolean = SQLConf.get.getConf(
      SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_DISTRIBUTION),
    requiredNumPartitions: Option[Int] = None) extends Distribution {
  require(
    clustering != Nil,
    "The clustering expressions of a ClusteredDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")

  override def createPartitioning(numPartitions: Int): Partitioning = {
    assert(requiredNumPartitions.isEmpty || requiredNumPartitions.get == numPartitions,
      s"This ClusteredDistribution requires ${requiredNumPartitions.get} partitions, but " +
        s"the actual number of partitions is $numPartitions.")
    HashPartitioning(clustering, numPartitions)
  }

  /**
   * 检查 expressions 是否按照相同的顺序匹配所有的 clustering 表达式。
   * 当 requireAllClusterKeys 设置为 true 时，Partitioning 应该调用此方法来检查其表达式。
   */
  def areAllClusterKeysMatched(expressions: Seq[Expression]): Boolean = {
    expressions.length == clustering.length &&
      expressions.zip(clustering).forall {
        case (l, r) => l.semanticEquals(r)
      }
  }
}

/**
 * 表示对结构化流式处理中有状态操作符的分布要求。
 * 每个有状态操作符中的分区都初始化状态存储（state store），这些状态存储与其他分区中的状态存储是独立的。
 * 由于无法对状态存储中的数据进行重新分区，因此 Spark 应该确保有状态操作符的物理分区在不同的 Spark 版本中保持不变。
 * 违反此要求可能会带来潜在的正确性问题。
 *
 * 由于此分布依赖于对有状态操作符的物理分区进行的 [[HashPartitioning]]，
 * 因此只有 [[HashPartitioning]]（以及 [[PartitioningCollection]] 中的 HashPartitioning）可以满足此分布。
 * 当 _requiredNumPartitions 为 1 时，[[SinglePartition]] 本质上与 [[HashPartitioning]] 相同，因此它也可以满足此分布。
 *
 * 注意：目前此分布仅适用于流-流连接。
 * 对于其他有状态操作符，我们一直使用 ClusteredDistribution，
 * 该分布可以以不同的方式构造状态的物理分区（ClusteredDistribution 需要放松条件，多个分区方案可以满足要求）。
 * 我们需要构建一种方法来修复此问题，并尽量减少破坏现有检查点的可能性。
 *
 * TODO(SPARK-38204): address the issue explained in above note.
 */
case class StatefulOpClusteredDistribution(
    expressions: Seq[Expression],
    _requiredNumPartitions: Int) extends Distribution {
  require(
    expressions != Nil,
    "The expressions for hash of a StatefulOpClusteredDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")

  override val requiredNumPartitions: Option[Int] = Some(_requiredNumPartitions)

  override def createPartitioning(numPartitions: Int): Partitioning = {
    assert(_requiredNumPartitions == numPartitions,
      s"This StatefulOpClusteredDistribution requires ${_requiredNumPartitions} " +
        s"partitions, but the actual number of partitions is $numPartitions.")
    HashPartitioning(expressions, numPartitions)
  }
}

/**
 * 表示数据已根据ordering[[Expression表达式]]排序的情况。
 * 其要求定义如下：
 *   - 给定任意两个相邻的分区，第二个分区的所有行必须根据ordering表达式大于或等于第一个分区中的任何行。
 * 换句话说，此分布要求在分区之间对行进行排序，但不一定在分区内部排序。
 */
case class OrderedDistribution(ordering: Seq[SortOrder]) extends Distribution {
  require(
    ordering != Nil,
    "The ordering expressions of an OrderedDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")

  override def requiredNumPartitions: Option[Int] = None

  override def createPartitioning(numPartitions: Int): Partitioning = {
    RangePartitioning(ordering, numPartitions)
  }
}

/**
 * 表示数据被广播到每个节点。通常整个元组集合被转换成不同的数据结构。
 */
case class BroadcastDistribution(mode: BroadcastMode) extends Distribution {
  override def requiredNumPartitions: Option[Int] = Some(1)

  override def createPartitioning(numPartitions: Int): Partitioning = {
    assert(numPartitions == 1,
      "The default partitioning of BroadcastDistribution can only have 1 partition.")
    BroadcastPartitioning(mode)
  }
}

/**
 * 描述了操作符输出在分区间如何分割的方式。它有两个主要属性：
 * 1. 分区的数量。
 * 2. 是否能够满足给定的分布。
 */
trait Partitioning {
  // 分区数量
  val numPartitions: Int

  /**
   * 当且仅当此[[Partitioning]]所提供的保证足以满足required [[Distribution]]所规定的分区方案时返回true，
   * 即当前数据集不需要重新分区以满足 required Distribution（可能需要对分区内的元组进行重新组织）。
   *
   * 如果numPartitions不匹配[[Distribution.requiredNumPartitions]]，
   * 则[[Partitioning]]永远无法满足[[Distribution]]。
   */
  final def satisfies(required: Distribution): Boolean = {
    required.requiredNumPartitions.forall(_ == numPartitions) && satisfies0(required)
  }

  /**
   * 为此分区和其所需的分布创建一个洗牌规范。
   *   该规范用于以下场景：操作符有多个子操作符（例如，join），
   *                    并用于决定此子操作符是否与其他操作符共同分区，因此是否需要引入额外的洗牌。
   *
   * @param distribution 此分区所需的聚集分布
   */
  def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec =
    throw new IllegalStateException(s"Unexpected partitioning: ${getClass.getSimpleName}")

  /**
   * 在numPartitions检查之后，实际定义此[[Partitioning]]是否可以满足给定的[[Distribution]]的方法。
   *
   * 默认情况下，如果[[Partitioning]]只有一个分区，则可以满足[[UnspecifiedDistribution]]和[[AllTuples]]。
   * 实现还可以使用特殊逻辑覆盖此方法。
   */
  protected def satisfies0(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true
    case AllTuples => numPartitions == 1
    case _ => false
  }
}

case class UnknownPartitioning(numPartitions: Int) extends Partitioning

/**
 * 表示一种分区方式，其中行通过从随机目标分区号开始并以轮询方式分配行，均匀分布在输出分区上。
 * 在实现DataFrame.repartition()操作符时使用此分区方式。
 */
case class RoundRobinPartitioning(numPartitions: Int) extends Partitioning

case object SinglePartition extends Partitioning {
  val numPartitions = 1

  override def satisfies0(required: Distribution): Boolean = required match {
    case _: BroadcastDistribution => false
    case _ => true
  }

  override def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec =
    SinglePartitionShuffleSpec
}

/**
 * 表示一种分区方式，其中行根据expressions的哈希值分割到不同的分区中。
 * 所有expressions计算结果相同的行保证在同一个分区中。
 *
 * 由于[[StatefulOpClusteredDistribution]]依赖于此分区方式，并且 Spark 要求有状态的操作符在查询的生命周期内（包括重新启动）保持相同的物理分区，
 * 因此对partitionIdExpression进行评估的结果在不同的 Spark 版本中必须保持不变。
 * 违反此要求可能会引发潜在的正确性问题。
 */
case class HashPartitioning(expressions: Seq[Expression], numPartitions: Int)
  extends Expression with Partitioning with Unevaluable {

  override def children: Seq[Expression] = expressions
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case h: StatefulOpClusteredDistribution =>
          expressions.length == h.expressions.length && expressions.zip(h.expressions).forall {
            case (l, r) => l.semanticEquals(r)
          }
        case c @ ClusteredDistribution(requiredClustering, requireAllClusterKeys, _) =>
          if (requireAllClusterKeys) {
            // 检查 HashPartitioning 是否精确分区在与 ClusteredDistribution 的完全相同的聚集键上。
            c.areAllClusterKeysMatched(expressions)
          } else {
            expressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
          }
        case _ => false
      }
    }
  }

  override def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec =
    HashShuffleSpec(this, distribution)

  /**
   * 返回一个表达式，根据哈希表达式生成一个有效的分区ID（即非负且小于numPartitions）。
   */
  def partitionIdExpression: Expression = Pmod(new Murmur3Hash(expressions), Literal(numPartitions))

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): HashPartitioning = copy(expressions = newChildren)
}

/**
 * 表示一种分区方式，其中行根据expressions中定义的转换进行分割到不同的分区中。
 * 如果定义了partitionValuesOpt，则应包含每个输入分区中的分区键值（经过expressions中的转换评估后）以升序排列的值。
 * 此外，其长度必须与输入分区的数量相同（因此是一一映射），并且partitionValuesOpt中的每一行必须是唯一的。
 *
 * 例如，如果expressions是[years(ts_col)]，那么partitionValuesOpt的一个有效值是[0, 1, 2]，它表示具有不同分区值的3个输入分区。
 * 每个分区中的所有行在应用years转换后对列 ts_col（时间戳类型）具有相同的值。
 *
 * 另一方面，[0, 0, 1]不是partitionValuesOpt的有效值，因为0重复了两次。.
 *
 * @param expressions 分区的分区表达式.
 * @param numPartitions 分区的数量
 * @param partitionValuesOpt  如果设置了，分布的聚集键的值，必须以升序排.
 */
case class KeyGroupedPartitioning(
    expressions: Seq[Expression],
    numPartitions: Int,
    partitionValuesOpt: Option[Seq[InternalRow]] = None) extends Partitioning {

  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case c @ ClusteredDistribution(requiredClustering, requireAllClusterKeys, _) =>
          if (requireAllClusterKeys) {
            // 检查此分区是否精确地在 ClusteredDistribution 的完全相同的聚集键上分区。
            c.areAllClusterKeysMatched(expressions)
          } else {
            // We'll need to find leaf attributes from the partition expressions first.
            val attributes = expressions.flatMap(_.collectLeaves())
            attributes.forall(x => requiredClustering.exists(_.semanticEquals(x)))
          }

        case _ =>
          false
      }
    }
  }

  override def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec =
    KeyGroupedShuffleSpec(this, distribution)
}

object KeyGroupedPartitioning {
  def apply(
      expressions: Seq[Expression],
      partitionValues: Seq[InternalRow]): KeyGroupedPartitioning = {
    KeyGroupedPartitioning(expressions, partitionValues.size, Some(partitionValues))
  }
}

/**
 * 表示一种分区方式，其中行根据在ordering中指定的表达式的某种总排序而分割到不同的分区中。
 * 当数据以这种方式分区时，它保证：
 * - 给定任意两个相邻的分区，第二个分区的所有行必须根据ordering表达式大于第一个分区中的任何行。
 * - 这是比OrderedDistribution(ordering)要求的要强的保证，因为分区之间不存在重叠。
 * 此类主要扩展了表达式，以便表达式上的转换会下降到其子节点。
 */
case class RangePartitioning(ordering: Seq[SortOrder], numPartitions: Int)
  extends Expression with Partitioning with Unevaluable {

  override def children: Seq[SortOrder] = ordering
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case OrderedDistribution(requiredOrdering) =>
          // 如果ordering是requiredOrdering的前缀：
          // 假设ordering是[a, b]，requiredOrdering是[a, b, c]。
          // 根据RangePartitioning的定义，任何前一个分区中的[a, b]必须小于后一个分区中的任何[a, b]。
          // 这也意味着任何前一个分区中的[a, b, c]必须小于后一个分区中的任何[a, b, c]。
          // 因此，RangePartitioning(a, b)满足OrderedDistribution(a, b, c)。

          // 如果requiredOrdering是ordering的前缀：
          // 假设ordering是[a, b, c]，requiredOrdering是[a, b]。
          // 根据RangePartitioning的定义，任何前一个分区中的[a, b, c]必须小于后一个分区中的任何[a, b, c]。
          // 如果有一个前一个分区中的[a1, b1]比后一个分区中的[a2, b2]大，那么一定有一个[a1, b1 c1]比[a2, b2, c2]大，这违反了RangePartitioning的定义。
          // 因此，保证了任何前一个分区中的[a, b]不得大于（即小于或等于）后一个分区中的任何[a, b]。
          // 因此，RangePartitioning(a, b, c)满足OrderedDistribution(a, b)。
          val minSize = Seq(requiredOrdering.size, ordering.size).min
          requiredOrdering.take(minSize) == ordering.take(minSize)
        case c @ ClusteredDistribution(requiredClustering, requireAllClusterKeys, _) =>
          val expressions = ordering.map(_.child)
          if (requireAllClusterKeys) {
            // Checks `RangePartitioning` is partitioned on exactly same clustering keys of
            // `ClusteredDistribution`.
            c.areAllClusterKeysMatched(expressions)
          } else {
            expressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
          }
        case _ => false
      }
    }
  }

  override def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec =
    RangeShuffleSpec(this.numPartitions, distribution)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): RangePartitioning =
    copy(ordering = newChildren.asInstanceOf[Seq[SortOrder]])
}

/**
 * 一个包含[[Partitioning]]的集合，可用于描述物理操作符输出的分区方案。
 * 通常用于具有多个子操作符的操作符。
 * 在这种情况下，此集合中的一个[[Partitioning]]描述了此操作符输出如何根据来自子操作符的表达式进行分区。
 *
 * 例如，对于在两个表A和B上的连接操作符，假设使用哈希分区方案并且连接条件为 A.key1 = B.key2，那么有两个[[Partitioning]]可以用于描述此连接操作符的输出如何分区，
 * 即HashPartitioning(A.key1)和HashPartitioning(B.key2)。
 *
 * 值得注意的是，此集合中的partitionings不需要是等价的，这对于外连接操作符很有用。
 */
case class PartitioningCollection(partitionings: Seq[Partitioning])
  extends Expression with Partitioning with Unevaluable {

  require(
    partitionings.map(_.numPartitions).distinct.length == 1,
    s"PartitioningCollection requires all of its partitionings have the same numPartitions.")

  override def children: Seq[Expression] = partitionings.collect {
    case expr: Expression => expr
  }

  override def nullable: Boolean = false

  override def dataType: DataType = IntegerType

  override val numPartitions = partitionings.map(_.numPartitions).distinct.head

  /**
   * Returns true if any `partitioning` of this collection satisfies the given
   * [[Distribution]].
   */
  override def satisfies0(required: Distribution): Boolean =
    partitionings.exists(_.satisfies(required))

  override def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec = {
    val filtered = partitionings.filter(_.satisfies(distribution))
    ShuffleSpecCollection(filtered.map(_.createShuffleSpec(distribution)))
  }

  override def toString: String = {
    partitionings.map(_.toString).mkString("(", " or ", ")")
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): PartitioningCollection =
    super.legacyWithNewChildren(newChildren).asInstanceOf[PartitioningCollection]
}

/**
 * 表示一种分区方式，其中行被收集、转换并广播到集群中的每个节点。
 */
case class BroadcastPartitioning(mode: BroadcastMode) extends Partitioning {
  override val numPartitions: Int = 1

  override def satisfies0(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true
    case BroadcastDistribution(m) if m == mode => true
    case _ => false
  }
}

/**
 * 这在操作符具有多个子节点（例如，连接操作）的情况下使用，其中一个或多个子节点对其数据是否可以被视为与其他数据共同分区具有自己的要求。
 * 它提供了以下API：
 *  - 与操作符的其他子节点的规范进行比较，并检查它们是否兼容。
 *  当两个规范兼容时，我们可以说它们的数据是共同分区的，并且如果必要，Spark可能能够消除Shuffle。
 * - 创建一个可用于重新分区另一个子节点的分区，以使其具有与此节点兼容的分区。
 */
trait ShuffleSpec {
  /**
   * 返回此洗牌规范的分区数。
   */
  def numPartitions: Int

  /**
   * 当前规范与提供的洗牌规范兼容时返回true。
   * 返回true意味着来自此规范的数据分区可以被视为与“other”共同分区，因此在连接两个侧时不需要洗牌。
   * 注意，Spark假定此操作是自反的、对称的和传递的。
   */
  def isCompatibleWith(other: ShuffleSpec): Boolean

  /**
   * 此洗牌规范是否可以用于为其他子操作符创建分区方案。
   */
  def canCreatePartitioning: Boolean

  /**
   * 创建一个可以用于使用给定的聚类表达式重新分区另一侧的分区方案。
   * 只有在以下情况下才会调用此方法：
   * 当包含clustering的一侧的[[isCompatibleWith]]返回false时。
   */
  def createPartitioning(clustering: Seq[Expression]): Partitioning =
    throw new UnsupportedOperationException("Operation unsupported for " +
        s"${getClass.getCanonicalName}")
}

case object SinglePartitionShuffleSpec extends ShuffleSpec {
  override def isCompatibleWith(other: ShuffleSpec): Boolean = {
    other.numPartitions == 1
  }

  override def canCreatePartitioning: Boolean = false

  override def createPartitioning(clustering: Seq[Expression]): Partitioning =
    SinglePartition

  override def numPartitions: Int = 1
}

case class RangeShuffleSpec(
    numPartitions: Int,
    distribution: ClusteredDistribution) extends ShuffleSpec {

  // RangePartitioning与任何其他分区方式都不兼容，因为它无法保证所有子节点的数据都是共同分区的，因为范围边界是随机抽样的。
  // 我们不能让RangeShuffleSpec创建一个分区方案。
  override def canCreatePartitioning: Boolean = false

  override def isCompatibleWith(other: ShuffleSpec): Boolean = other match {
    case SinglePartitionShuffleSpec => numPartitions == 1
    case ShuffleSpecCollection(specs) => specs.exists(isCompatibleWith)
    // `RangePartitioning` is not compatible with any other partitioning since it can't guarantee
    // data are co-partitioned for all the children, as range boundaries are randomly sampled.
    case _ => false
  }
}

case class HashShuffleSpec(
    partitioning: HashPartitioning,
    distribution: ClusteredDistribution) extends ShuffleSpec {

  /**
   *
   * 一个序列，其中每个元素是哈希分区键到集群键的位置集合。
   * 例如，如果集群键是 [a, b, b]，哈希分区键是 [a, b]，结果将是 [(0), (1, 2)]。
   *
   * 这对于检查两个HashShuffleSpec之间的兼容性很有用。
   * 如果两个连接子节点的集群键分别为 [a, b, b] 和 [x, y, z]，哈希分区键为 [a, b] 和 [x, z]，它们是兼容的。
   * 通过位置，我们可以通过查看两侧的哈希分区键的位置是否重叠来进行兼容性检查。
   */
  lazy val hashKeyPositions: Seq[mutable.BitSet] = {
    val distKeyToPos = mutable.Map.empty[Expression, mutable.BitSet]
    distribution.clustering.zipWithIndex.foreach { case (distKey, distKeyPos) =>
      distKeyToPos.getOrElseUpdate(distKey.canonicalized, mutable.BitSet.empty).add(distKeyPos)
    }
    partitioning.expressions.map(k => distKeyToPos.getOrElse(k.canonicalized, mutable.BitSet.empty))
  }

  override def isCompatibleWith(other: ShuffleSpec): Boolean = other match {
    case SinglePartitionShuffleSpec =>
      partitioning.numPartitions == 1
    case otherHashSpec @ HashShuffleSpec(otherPartitioning, otherDistribution) =>
      // 我们需要检查：
      // 1. 两个分布具有相同数量的聚类表达式
      // 2. 两个分区具有相同数量的分区
      // 3. 两个分区具有相同数量的表达式
      // 4. 两侧的每一对分区表达式在其相应的分布中具有重叠的位置。
      distribution.clustering.length == otherDistribution.clustering.length &&
      partitioning.numPartitions == otherPartitioning.numPartitions &&
      partitioning.expressions.length == otherPartitioning.expressions.length && {
        val otherHashKeyPositions = otherHashSpec.hashKeyPositions
        hashKeyPositions.zip(otherHashKeyPositions).forall { case (left, right) =>
          left.intersect(right).nonEmpty
        }
      }
    case ShuffleSpecCollection(specs) =>
      specs.exists(isCompatibleWith)
    case _ =>
      false
  }

  override def canCreatePartitioning: Boolean = {

    //为了避免潜在的数据倾斜，如果哈希分区键不是完整的连接键（集群键），我们不允许HashShuffleSpec创建分区。
    // 然后，规划器将使用ClusteredDistribution的默认分区添加洗牌，该分区使用所有连接键。
    if (SQLConf.get.getConf(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION)) {
      distribution.areAllClusterKeysMatched(partitioning.expressions)
    } else {
      true
    }
  }

  override def createPartitioning(clustering: Seq[Expression]): Partitioning = {
    val exprs = hashKeyPositions.map(v => clustering(v.head))
    HashPartitioning(exprs, partitioning.numPartitions)
  }

  override def numPartitions: Int = partitioning.numPartitions
}

case class KeyGroupedShuffleSpec(
    partitioning: KeyGroupedPartitioning,
    distribution: ClusteredDistribution) extends ShuffleSpec {

  /**
   * 一个序列，其中每个元素是分区表达式到聚类键的位置集合。例如，如果聚类键是[a, b, b]，分区表达式是[bucket(4, a), years(b)]，则结果将是[(0), (1, 2)]。
   * 请注意，我们只允许每个分区表达式包含一个单独的分区键。因此，这里的映射与HashShuffleSpec中的映射非常相似。
   */
  lazy val keyPositions: Seq[mutable.BitSet] = {
    val distKeyToPos = mutable.Map.empty[Expression, mutable.BitSet]
    distribution.clustering.zipWithIndex.foreach { case (distKey, distKeyPos) =>
      distKeyToPos.getOrElseUpdate(distKey.canonicalized, mutable.BitSet.empty).add(distKeyPos)
    }
    partitioning.expressions.map { e =>
      val leaves = e.collectLeaves()
      assert(leaves.size == 1, s"Expected exactly one child from $e, but found ${leaves.size}")
      distKeyToPos.getOrElse(leaves.head.canonicalized, mutable.BitSet.empty)
    }
  }

  private lazy val ordering: Ordering[InternalRow] =
    RowOrdering.createNaturalAscendingOrdering(partitioning.expressions.map(_.dataType))

  override def numPartitions: Int = partitioning.numPartitions

  override def isCompatibleWith(other: ShuffleSpec): Boolean = other match {
    // 这里我们检查：
    // 1. 两个分布具有相同数量的聚类键
    // 2. 两个分区具有相同数量的分区
    // 3. 两边的分区表达式是兼容的，这意味着：
    // 3.1 两边具有相同数量的分区表达式
    // 3.2 在相同索引处的每对分区表达式，相应的分区键必须在各自的聚类键中共享重叠的位置。
    // 3.3 在相同索引处的每对分区表达式必须共享兼容的转换函数。
    // 4. 分区值（如果两边都存在）按相同顺序进行。
    case otherSpec @ KeyGroupedShuffleSpec(otherPartitioning, otherDistribution) =>
      val expressions = partitioning.expressions
      val otherExpressions = otherPartitioning.expressions

      distribution.clustering.length == otherDistribution.clustering.length &&
        numPartitions == other.numPartitions &&
          expressions.length == otherExpressions.length && {
            val otherKeyPositions = otherSpec.keyPositions
            keyPositions.zip(otherKeyPositions).forall { case (left, right) =>
              left.intersect(right).nonEmpty
            }
          } && expressions.zip(otherExpressions).forall {
            case (l, r) => isExpressionCompatible(l, r)
          } && partitioning.partitionValuesOpt.zip(otherPartitioning.partitionValuesOpt).forall {
            case (left, right) => left.zip(right).forall { case (l, r) =>
              ordering.compare(l, r) == 0
            }
         }
    case ShuffleSpecCollection(specs) =>
      specs.exists(isCompatibleWith)
    case _ => false
  }

  private def isExpressionCompatible(left: Expression, right: Expression): Boolean =
    (left, right) match {
      case (_: LeafExpression, _: LeafExpression) => true
      case (left: TransformExpression, right: TransformExpression) =>
        left.isSameFunction(right)
      case _ => false
    }

  override def canCreatePartitioning: Boolean = false
}

case class ShuffleSpecCollection(specs: Seq[ShuffleSpec]) extends ShuffleSpec {
  override def isCompatibleWith(other: ShuffleSpec): Boolean = {
    specs.exists(_.isCompatibleWith(other))
  }

  override def canCreatePartitioning: Boolean =
    specs.forall(_.canCreatePartitioning)

  override def createPartitioning(clustering: Seq[Expression]): Partitioning = {
    // as we only consider # of partitions as the cost now, it doesn't matter which one we choose
    // since they should all have the same # of partitions.
    require(specs.map(_.numPartitions).toSet.size == 1, "expected all specs in the collection " +
      "to have the same number of partitions")
    specs.head.createPartitioning(clustering)
  }

  override def numPartitions: Int = {
    require(specs.nonEmpty, "expected specs to be non-empty")
    specs.head.numPartitions
  }
}
