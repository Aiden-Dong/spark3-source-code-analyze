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

package org.apache.spark.sql.catalyst.encoders

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{typeTag, TypeTag}

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.{InternalRow, JavaTypeInference, ScalaReflection}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, GetColumnByOrdinal, SimpleAnalyzer, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder.{Deserializer, Serializer}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.expressions.objects.{AssertNotNull, InitializeJavaBean, Invoke, NewInstance}
import org.apache.spark.sql.catalyst.optimizer.{ReassignLambdaVariableID, SimplifyCasts}
import org.apache.spark.sql.catalyst.plans.logical.{CatalystSerde, DeserializeToObject, LeafNode, LocalRelation}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{ObjectType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * 用于构造编码器的工厂类，这些Encoder使用 Catalyst 表达式和Codegen将对象和基本类型与内部行格式相互转换。
 * 默认情况下，从输入行中检索值以生成对象时使用的表达式将按以下方式创建：
 *  - 类将使用 [[UnresolvedAttribute]] 表达式和 [[UnresolvedExtractValue]] 表达式按名称提取其子字段。
 *  - 元组将使用 [[BoundReference]] 表达式按位置提取其子字段。
 *  - 基本类型将从第一个序号中提取其值，默认 schema 的名称为 `value`。
 */
object ExpressionEncoder {

  def apply[T : TypeTag](): ExpressionEncoder[T] = {
    val mirror = ScalaReflection.mirror
    val tpe = typeTag[T].in(mirror).tpe

    val cls = mirror.runtimeClass(tpe)
    val serializer = ScalaReflection.serializerForType(tpe)
    val deserializer = ScalaReflection.deserializerForType(tpe)

    new ExpressionEncoder[T](
      serializer,
      deserializer,
      ClassTag[T](cls))
  }

  // TODO: 改进 Java Bean 编码器的错误消息。
  def javaBean[T](beanClass: Class[T]): ExpressionEncoder[T] = {
    val schema = JavaTypeInference.inferDataType(beanClass)._1
    assert(schema.isInstanceOf[StructType])

    val objSerializer = JavaTypeInference.serializerFor(beanClass)
    val objDeserializer = JavaTypeInference.deserializerFor(beanClass)

    new ExpressionEncoder[T](
      objSerializer,
      objDeserializer,
      ClassTag[T](beanClass))
  }

  /**
   * 给定一组 N 个编码器，构造一个新的编码器，该编码器将对象作为 N 元组中的项生成。
   * 注意，这些编码器应该是[未解析]的，以便保留有关名称/位置绑定的信息。
   */
  def tuple(encoders: Seq[ExpressionEncoder[_]]): ExpressionEncoder[_] = {
    if (encoders.length > 22) {
      throw QueryExecutionErrors.elementsOfTupleExceedLimitError()
    }

    encoders.foreach(_.assertUnresolved())

    val cls = Utils.getContextOrSparkClassLoader.loadClass(s"scala.Tuple${encoders.size}")

    val newSerializerInput = BoundReference(0, ObjectType(cls), nullable = true)
    val serializers = encoders.zipWithIndex.map { case (enc, index) =>
      val boundRefs = enc.objSerializer.collect { case b: BoundReference => b }.distinct
      assert(boundRefs.size == 1, "object serializer should have only one bound reference but " +
        s"there are ${boundRefs.size}")

      val originalInputObject = boundRefs.head
      val newInputObject = Invoke(
        newSerializerInput,
        s"_${index + 1}",
        originalInputObject.dataType,
        returnNullable = originalInputObject.nullable)

      val newSerializer = enc.objSerializer.transformUp {
        case BoundReference(0, _, _) => newInputObject
      }

      Alias(newSerializer, s"_${index + 1}")()
    }
    val newSerializer = CreateStruct(serializers)

    val newDeserializerInput = GetColumnByOrdinal(0, newSerializer.dataType)
    val deserializers = encoders.zipWithIndex.map { case (enc, index) =>
      val getColExprs = enc.objDeserializer.collect { case c: GetColumnByOrdinal => c }.distinct
      assert(getColExprs.size == 1, "object deserializer should have only one " +
        s"`GetColumnByOrdinal`, but there are ${getColExprs.size}")

      val input = GetStructField(newDeserializerInput, index)
      enc.objDeserializer.transformUp {
        case GetColumnByOrdinal(0, _) => input
      }
    }
    val newDeserializer = NewInstance(cls, deserializers, ObjectType(cls), propagateNull = false)

    def nullSafe(input: Expression, result: Expression): Expression = {
      If(IsNull(input), Literal.create(null, result.dataType), result)
    }

    new ExpressionEncoder[Any](
      nullSafe(newSerializerInput, newSerializer),
      nullSafe(newDeserializerInput, newDeserializer),
      ClassTag(cls))
  }

  def tuple[T](e: ExpressionEncoder[T]): ExpressionEncoder[Tuple1[T]] =
    tuple(Seq(e)).asInstanceOf[ExpressionEncoder[Tuple1[T]]]

  def tuple[T1, T2](
      e1: ExpressionEncoder[T1],
      e2: ExpressionEncoder[T2]): ExpressionEncoder[(T1, T2)] =
    tuple(Seq(e1, e2)).asInstanceOf[ExpressionEncoder[(T1, T2)]]

  def tuple[T1, T2, T3](
      e1: ExpressionEncoder[T1],
      e2: ExpressionEncoder[T2],
      e3: ExpressionEncoder[T3]): ExpressionEncoder[(T1, T2, T3)] =
    tuple(Seq(e1, e2, e3)).asInstanceOf[ExpressionEncoder[(T1, T2, T3)]]

  def tuple[T1, T2, T3, T4](
      e1: ExpressionEncoder[T1],
      e2: ExpressionEncoder[T2],
      e3: ExpressionEncoder[T3],
      e4: ExpressionEncoder[T4]): ExpressionEncoder[(T1, T2, T3, T4)] =
    tuple(Seq(e1, e2, e3, e4)).asInstanceOf[ExpressionEncoder[(T1, T2, T3, T4)]]

  def tuple[T1, T2, T3, T4, T5](
      e1: ExpressionEncoder[T1],
      e2: ExpressionEncoder[T2],
      e3: ExpressionEncoder[T3],
      e4: ExpressionEncoder[T4],
      e5: ExpressionEncoder[T5]): ExpressionEncoder[(T1, T2, T3, T4, T5)] =
    tuple(Seq(e1, e2, e3, e4, e5)).asInstanceOf[ExpressionEncoder[(T1, T2, T3, T4, T5)]]

  private val anyObjectType = ObjectType(classOf[Any])

  /**
   * 将 [[InternalRow]] 反序列化为类型 `T` 的对象的函数。此类不是线程安全的。
   */
  class Deserializer[T](private val expressions: Seq[Expression])
    extends (InternalRow => T) with Serializable {
    @transient
    private[this] var constructProjection: Projection = _

    override def apply(row: InternalRow): T = try {
      if (constructProjection == null) {
        constructProjection = SafeProjection.create(expressions)
      }
      constructProjection(row).get(0, anyObjectType).asInstanceOf[T]
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.expressionDecodingError(e, expressions)
    }
  }

  /**
   * 将类型 `T` 的对象序列化为 [[InternalRow]] 的函数。此类不是线程安全的。
   * 注意，多次调用 `apply(..)` 会返回相同的实际 [[InternalRow]] 对象。
   * 因此，如果需要，调用者应在进行另一次调用之前复制结果。
   */
  class Serializer[T](private val expressions: Seq[Expression])
    extends (T => InternalRow) with Serializable {

    @transient
    private[this] var inputRow: GenericInternalRow = _
    @transient
    private[this] var extractProjection: UnsafeProjection = _

    override def apply(t: T): InternalRow = try {
      if (extractProjection == null) {
        inputRow = new GenericInternalRow(1)                                 // 创建输入行容器
        extractProjection = GenerateUnsafeProjection.generate(expressions)   // 生成代码优化的投影
      }
      inputRow(0) = t               // 将对象放入输入行
      extractProjection(inputRow)   // 执行投影转换
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.expressionEncodingError(e, expressions)
    }
  }
}


/**
 * JVM 对象的通用编码器，使用 Catalyst 表达式作为 `serializer` 和 `deserializer`。
 *
 * @param objSerializer 可用于将原始对象编码为相应 Spark SQL 表示形式的表达式， 该表示形式可以是基本列、数组、映射或结构体。
 *                      这表示 Spark SQL 通常如何序列化类型 `T` 的对象。
 * @param objDeserializer 给定 Spark SQL 表示形式将构造对象的表达式。这表示 Spark SQL 通常如何将 Spark SQL 表示形式中的序列化值
 *                        反序列化回类型 `T` 的对象。
 * @param clsTag `T` 的类标签。
 */
case class ExpressionEncoder[T](
    objSerializer: Expression,        // 对象序列化表达式 : object -> InternalRow
    objDeserializer: Expression,      // 对象反序列化表达式 : InternalRow -> object
    clsTag: ClassTag[T])              // 当前对象的类型标签
  extends Encoder[T] {

  /**
   * 表达式序列，每个顶级字段一个，可用于将原始对象的值提取到 [[InternalRow]] 中：
   * 1. 如果 `serializer` 将原始对象编码为结构体，则剥离外部 If-IsNull 并获取 `CreateNamedStruct`。
   * 2. 对于其他情况，用 `CreateNamedStruct` 包装单个序列化器。
   */
  val serializer: Seq[NamedExpression] = {
    val clsName = Utils.getSimpleName(clsTag.runtimeClass)

    if (isSerializedAsStructForTopLevel) {
      val nullSafeSerializer = objSerializer.transformUp {
        case r: BoundReference =>
          // 对于 Product 类型的输入对象，如果它为 null，我们无法将其编码为行，
          // 因为 Spark SQL 不允许顶级行为 null，只有其列可以为 null。
          AssertNotNull(r, Seq("top level Product or row object"))
      }
      nullSafeSerializer match {
        case If(_: IsNull, _, s: CreateNamedStruct) => s
        case s: CreateNamedStruct => s
        case _ =>
          throw QueryExecutionErrors.classHasUnexpectedSerializerError(clsName, objSerializer)
      }
    } else {
      // 对于其他输入对象（如基本类型、数组、映射等），我们构造一个结构体来包装序列化器，
      // 该序列化器是行的一列。
      //
      // 注意：因为 Spark SQL 不允许顶级行为 null，为了编码顶级 Option[Product] 类型，
      // 我们将其作为顶级结构体列。
      CreateNamedStruct(Literal("value") :: objSerializer :: Nil)
    }
  }.flatten


  /**
   * 返回一个表达式，该表达式可用于将输入行反序列化为具有兼容 schema 的类型 `T` 的对象。
   * 行的字段将使用与构造函数参数同名的 `UnresolvedAttribute` 提取。
   *
   * 对于编码为结构体的复杂对象，结构体的字段将使用相应序号的 `GetColumnByOrdinal` 提取。
   */
  val deserializer: Expression = {
    if (isSerializedAsStructForTopLevel) {
      // 我们将这种对象序列化为根级行。通用反序列化器的输入是一个 `GetColumnByOrdinal(0)`表达式，用于提取行的第一列。
      // 我们需要转换属性访问器。
      objDeserializer.transform {
        case UnresolvedExtractValue(GetColumnByOrdinal(0, _),
            Literal(part: UTF8String, StringType)) =>
          UnresolvedAttribute.quoted(part.toString)
        case GetStructField(GetColumnByOrdinal(0, dt), ordinal, _) =>
          GetColumnByOrdinal(ordinal, dt)
        case If(IsNull(GetColumnByOrdinal(0, _)), _, n: NewInstance) => n
        case If(IsNull(GetColumnByOrdinal(0, _)), _, i: InitializeJavaBean) => i
      }
    } else {
      // 对于其他输入对象（如基本类型、数组、映射等），我们将行的第一列反序列化为对象。
      objDeserializer
    }
  }

  // 将 `T` 转换为 Spark SQL 行后的 schema。此 schema 依赖于给定的序列化器。
  val schema: StructType = StructType(serializer.map { s =>
    StructField(s.name, s.dataType, s.nullable)
  })

  /**
   * 如果类型 `T` 被 `objSerializer` 序列化为结构体，则返回 true。
   */
  def isSerializedAsStruct: Boolean = objSerializer.dataType.isInstanceOf[StructType]

  /**
   * 如果类型 `T` 被序列化为结构体，当它被编码为 Spark SQL 行时，结构体中的字段自然映射到行中的顶级列。换句话说，序列化的结构体被展平为行。
   * 但如果 `T` 也是 `Option` 类型，则无法展平为顶级行，因为在 Spark SQL 中顶级行不能为 null。
   * 如果 `T` 被序列化为结构体且不是 `Option` 类型，则此方法返回 true。
   */
  def isSerializedAsStructForTopLevel: Boolean = {
    isSerializedAsStruct && !classOf[Option[_]].isAssignableFrom(clsTag.runtimeClass)
  }

  // 序列化器表达式用于将对象编码为行，而对象通常是在算子内部产生的中间值， 而不是来自子算子的输出。
  // 这与普通表达式有很大不同，`AttributeReference` 在这里不起作用 （中间值不是属性）。
  // 我们假设所有序列化器表达式使用相同的 `BoundReference` 来引用对象， 如果不是，则抛出异常。
  assert(serializer.forall(_.references.isEmpty), "serializer cannot reference any attributes.")
  assert(serializer.flatMap { ser =>
    val boundRefs = ser.collect { case b: BoundReference => b }
    assert(boundRefs.nonEmpty,
      "each serializer expression should contain at least one `BoundReference`")
    boundRefs
  }.distinct.length <= 1, "all serializer expressions must use the same BoundReference.")

  /**
   * 返回此编码器的新副本，其中 `deserializer` 已解析并绑定到给定的 schema。
   *
   * 注意，理想情况下，编码器用作序列化/反序列化表达式的容器，解析和绑定工作应该
   * 在查询框架内部进行。但是，在某些情况下，我们需要将编码器用作函数来直接进行序列化
   * （例如 Dataset.collect），那么我们可以使用此方法在查询框架外部进行解析和绑定。
   */
  def resolveAndBind(
      attrs: Seq[Attribute] = schema.toAttributes,
      analyzer: Analyzer = SimpleAnalyzer): ExpressionEncoder[T] = {
    val dummyPlan = CatalystSerde.deserialize(LocalRelation(attrs))(this)
    val analyzedPlan = analyzer.execute(dummyPlan)
    analyzer.checkAnalysis(analyzedPlan)
    val resolved = SimplifyCasts(analyzedPlan).asInstanceOf[DeserializeToObject].deserializer
    val bound = BindReferences.bindReference(resolved, attrs)
    copy(objDeserializer = bound)
  }

  @transient
  private lazy val optimizedDeserializer: Seq[Expression] = {
    // 当直接使用 `ExpressionEncoder` 时，我们将跳过正常的查询处理步骤（分析器、优化器等）。
    // 这里我们应用 ReassignLambdaVariableID 规则， 因为它对代码生成性能很重要。
    val optimizedPlan = ReassignLambdaVariableID.apply(DummyExpressionHolder(Seq(deserializer)))
    optimizedPlan.asInstanceOf[DummyExpressionHolder].exprs
  }

  @transient
  private lazy val optimizedSerializer = {
    // 当直接使用 `ExpressionEncoder` 时，我们将跳过正常的查询处理步骤 （分析器、优化器等）。
    // 这里我们应用 ReassignLambdaVariableID 规则， 因为它对代码生成性能很重要。
    val optimizedPlan = ReassignLambdaVariableID.apply(DummyExpressionHolder(serializer))
    optimizedPlan.asInstanceOf[DummyExpressionHolder].exprs
  }

  /**
   * 返回一组新的（具有唯一 ID）[[NamedExpression]]，表示此对象的序列化形式。
   */
  def namedExpressions: Seq[NamedExpression] = schema.map(_.name).zip(serializer).map {
    case (_, ne: NamedExpression) => ne.newInstance()
    case (name, e) => Alias(e, name)()
  }

  /**
   * 创建一个可以将类型 `T` 的对象转换为 Spark SQL Row 的序列化器。
   *
   * 注意，返回的 [[Serializer]] 不是线程安全的。允许多次调用 `serializer.apply(..)`
   * 返回相同的实际 [[InternalRow]] 对象。因此，如果需要，调用者应在进行另一次调用之前复制结果。
   */
  def createSerializer(): Serializer[T] = new Serializer[T](optimizedSerializer)

  /**
   * 创建一个可以将 Spark SQL Row 转换为类型 `T` 的对象的反序列化器。
   *
   * 注意，在创建反序列化器之前，必须将编码器 `resolveAndBind` 到特定的 schema。
   */
  def createDeserializer(): Deserializer[T] = new Deserializer[T](optimizedDeserializer)

  /**
   * 解析到给定 schema 的过程会丢失有关给定字段按序号而不是按名称绑定的位置的信息。
   * 此方法检查以确保在我们计划稍后组合编码器的地方尚未完成此过程。
   */
  def assertUnresolved(): Unit = {
    (deserializer +:  serializer).foreach(_.foreach {
      case a: AttributeReference if a.name != "loopVar" =>
        throw QueryExecutionErrors.notExpectedUnresolvedEncoderError(a)
      case _ =>
    })
  }

  protected val attrs = serializer.flatMap(_.collect {
    case _: UnresolvedAttribute => ""
    case a: Attribute => s"#${a.exprId}"
    case b: BoundReference => s"[${b.ordinal}]"
  })

  protected val schemaString =
    schema
      .zip(attrs)
      .map { case(f, a) => s"${f.name}$a: ${f.dataType.simpleString}"}.mkString(", ")

  override def toString: String = s"class[$schemaString]"
}

// 一个虚拟逻辑计划，可以保存表达式并通过优化器规则。
case class DummyExpressionHolder(exprs: Seq[Expression]) extends LeafNode {
  override lazy val resolved = true
  override def output: Seq[Attribute] = Nil
}
