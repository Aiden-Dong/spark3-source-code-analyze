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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.CatalystTypeConverters.{createToCatalystConverter, createToScalaConverter => catalystCreateToScalaConverter, isPrimitive}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.TreePattern.{SCALA_UDF, TreePattern}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, DataType}
import org.apache.spark.util.Utils

/**
 * 用户自定义函数。
 * @param function  要运行的用户定义的 scala 函数。
 *                  注意，如果使用原始类型参数，则无法检查它是否为 null， 如果原始输入为 null，UDF 将为您返回 null。
 *                  如果您想自己处理 null，请使用包装类型或 [[Option]]。
 * @param dataType  函数的返回类型。
 * @param children  此 UDF 的输入表达式。
 * @param inputEncoders 每个输入参数的 ExpressionEncoder。对于序列化为结构体的输入参数，将使用编码器而不是 CatalystTypeConverters 将内部值转换为 Scala 值。
 * @param outputEncoder 函数返回类型的 ExpressionEncoder。仅在这是类型化 Scala UDF 时定义。
 * @param udfName  此 UDF 的用户指定名称。
 * @param nullable  如果 UDF 可以返回 null 值，则为 true。
 * @param udfDeterministic  如果 UDF 是确定性的，则为 true。确定性 UDF 在每次使用特定输入调用时都会返回相同的结果。
 */
case class ScalaUDF(
    function: AnyRef,
    dataType: DataType,
    children: Seq[Expression],
    inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Nil,
    outputEncoder: Option[ExpressionEncoder[_]] = None,
    udfName: Option[String] = None,
    nullable: Boolean = true,
    udfDeterministic: Boolean = true)
  extends Expression with NonSQLExpression with UserDefinedExpression {

  override lazy val deterministic: Boolean = udfDeterministic && children.forall(_.deterministic)

  final override val nodePatterns: Seq[TreePattern] = Seq(SCALA_UDF)

  override def toString: String = s"$name(${children.mkString(", ")})"

  override def name: String = udfName.getOrElse("UDF")

  override lazy val canonicalized: Expression = {
    // SPARK-32307: `ExpressionEncoder` 无法被规范化，从技术上讲，
    // 我们不需要它来识别 `ScalaUDF`。
    copy(children = children.map(_.canonicalized), inputEncoders = Nil, outputEncoder = None)
  }

  /**
   * 分析器应该了解 Scala 原始类型，以便在这些类型的任何输入值为 null 时使 UDF 返回 null。
   * 另一方面，Java UDF 只能有包装类型， 因此这将返回 Nil（与全部为 false 具有相同效果），分析器将跳过对它们的 null 处理。
   */
  lazy val inputPrimitives: Seq[Boolean] = {       // 判断是不是原始类型
    inputEncoders.map { encoderOpt =>
      // 某些输入可能没有特定的编码器（例如 `Any`）
      if (encoderOpt.isDefined) {
        val encoder = encoderOpt.get
        if (encoder.isSerializedAsStruct) {     // 结构体类型不是原始类型
          false
        } else {                                // `nullable` 为 false 当且仅当类型是原始类型
          !encoder.schema.head.nullable
        }
      } else {                                  // // Any 类型不是原始类型
        false
      }
    }
  }

  /**
   * 此 UDF 的预期输入类型，用于执行类型强制转换。
   * 如果我们不想执行强制转换， 只需使用 "Nil"。
   * 请注意，使用 Option of Seq[DataType] 会更好， 这样我们可以使用 "None" 作为不进行类型强制转换的情况。
   * 但是，这需要对代码库进行更多重构。
   */
  def inputTypes: Seq[AbstractDataType] = {     // 获取输入数据类型
    inputEncoders.map { encoderOpt =>
      if (encoderOpt.isDefined) {
        val encoder = encoderOpt.get
        if (encoder.isSerializedAsStruct) {   // 表示如果是结构体类型
          encoder.schema
        } else {
          encoder.schema.head.dataType        // 非结构体类型
        }
      } else {
        AnyDataType
      }
    }
  }

  /**
   * 输出数据处理 : 把 scala 类型的输出变量转义成 InternalRow
   *
   * 创建转换器，将 scala 数据类型转换为 catalyst 数据类型， 用于 udf 函数的返回数据类型。
   * 我们仅对类型化 ScalaUDF 使用 `ExpressionEncoder`来创建转换器，因为这是我们知道 udf 函数返回数据类型的类型标签的唯一情况。
   */
  private def catalystConverter: Any => Any = outputEncoder.map { enc =>

    val toRow = enc.createSerializer().asInstanceOf[Any => Any]  // 获取输出类型序列化器

    if (enc.isSerializedAsStructForTopLevel) {    // 是否是结构体并可以展平
      value: Any =>
        if (value == null) null else toRow(value).asInstanceOf[InternalRow]
    } else {
      value: Any =>
        if (value == null) null else toRow(value).asInstanceOf[InternalRow].get(0, dataType)
    }
  }.getOrElse(createToCatalystConverter(dataType))

  /**
   * 输入数据处理
   *
   * 创建转换器，将 catalyst 数据类型转换为 scala 数据类型。
   * 我们使用 `CatalystTypeConverters` 为以下情况创建转换器：
   *   - 不提供 inputEncoders 的 UDF，例如无类型 Scala UDF 和 Java UDF
   *   - `ExpressionEncoder` 不支持的类型，例如 Any
   *   - 原始类型，为了使用 `identity` 以获得更好的性能
   * 对于其他情况，如 case class、Option[T]，我们使用 `ExpressionEncoder`，
   * 因为 `CatalystTypeConverters` 不支持这些数据类型。
   *
   * @param i 子节点的索引
   * @param dataType 第 i 个子节点的输出数据类型
   * @return 转换器和一个布尔值，指示转换器是否是使用 `ExpressionEncoder` 创建的。
   */
  private def scalaConverter(i: Int, dataType: DataType): (Any => Any, Boolean) = {

    val useEncoder =
      !(inputEncoders.isEmpty ||   // 对于无类型 Scala UDF 和 Java UDF
      inputEncoders(i).isEmpty ||  // 对于编码器不支持的类型，例如 Any
      inputPrimitives(i))          // 对于原始类型

    if (useEncoder) {
      val enc = inputEncoders(i).get                  // 获取对应列的反序列化类型
      val fromRow = enc.createDeserializer()          // 创建反序列化方法 InternalRow -> object

      // 反序列化方法
      val converter = if (enc.isSerializedAsStructForTopLevel) {
        row: Any => fromRow(row.asInstanceOf[InternalRow])
      } else {
        val inputRow = new GenericInternalRow(1)
        value: Any => inputRow.update(0, value); fromRow(inputRow)
      }
      (converter, true)
    } else { // 使用 CatalystTypeConverters
      (catalystCreateToScalaConverter(dataType), false)
    }
  }

  private def createToScalaConverter(i: Int, dataType: DataType): Any => Any =
    scalaConverter(i, dataType)._1

  // scalastyle:off line.size.limit

  /** 此方法由以下脚本生成

   (1 to 22).map { x =>
   val anys = (1 to x).map(x => "Any").reduce(_ + ", " + _)
   val childs = (0 to x - 1).map(x => s"val child$x = children($x)").reduce(_ + "\n  " + _)
   val converters = (0 to x - 1).map(x => s"lazy val converter$x = createToScalaConverter($x, child$x.dataType)").reduce(_ + "\n  " + _)
   val evals = (0 to x - 1).map(x => s"converter$x(child$x.eval(input))").reduce(_ + ",\n      " + _)

   s"""case $x =>
   val func = function.asInstanceOf[($anys) => Any]
      $childs
      $converters
      (input: InternalRow) => {
   func(
          $evals)
   }
   """
   }.foreach(println)

   */
  private[this] val f = children.size match {
    case 0 =>
      val func = function.asInstanceOf[() => Any]
      (input: InternalRow) => {
        func()
      }

    case 1 =>
      val func = function.asInstanceOf[(Any) => Any]
      val child0 = children(0)   // 获取子列表达式

      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      (input: InternalRow) => {
        func(

          converter0(child0.eval(input)))
      }

    case 2 =>
      val func = function.asInstanceOf[(Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)))
      }

    case 3 =>
      val func = function.asInstanceOf[(Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)))
      }

    case 4 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)))
      }

    case 5 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)))
      }

    case 6 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)))
      }

    case 7 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)))
      }

    case 8 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)))
      }

    case 9 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)))
      }

    case 10 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)))
      }

    case 11 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)))
      }

    case 12 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      val child11 = children(11)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      lazy val converter11 = createToScalaConverter(11, child11.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)),
          converter11(child11.eval(input)))
      }

    case 13 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      val child11 = children(11)
      val child12 = children(12)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      lazy val converter11 = createToScalaConverter(11, child11.dataType)
      lazy val converter12 = createToScalaConverter(12, child12.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)),
          converter11(child11.eval(input)),
          converter12(child12.eval(input)))
      }

    case 14 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      val child11 = children(11)
      val child12 = children(12)
      val child13 = children(13)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      lazy val converter11 = createToScalaConverter(11, child11.dataType)
      lazy val converter12 = createToScalaConverter(12, child12.dataType)
      lazy val converter13 = createToScalaConverter(13, child13.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)),
          converter11(child11.eval(input)),
          converter12(child12.eval(input)),
          converter13(child13.eval(input)))
      }

    case 15 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      val child11 = children(11)
      val child12 = children(12)
      val child13 = children(13)
      val child14 = children(14)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      lazy val converter11 = createToScalaConverter(11, child11.dataType)
      lazy val converter12 = createToScalaConverter(12, child12.dataType)
      lazy val converter13 = createToScalaConverter(13, child13.dataType)
      lazy val converter14 = createToScalaConverter(14, child14.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)),
          converter11(child11.eval(input)),
          converter12(child12.eval(input)),
          converter13(child13.eval(input)),
          converter14(child14.eval(input)))
      }

    case 16 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      val child11 = children(11)
      val child12 = children(12)
      val child13 = children(13)
      val child14 = children(14)
      val child15 = children(15)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      lazy val converter11 = createToScalaConverter(11, child11.dataType)
      lazy val converter12 = createToScalaConverter(12, child12.dataType)
      lazy val converter13 = createToScalaConverter(13, child13.dataType)
      lazy val converter14 = createToScalaConverter(14, child14.dataType)
      lazy val converter15 = createToScalaConverter(15, child15.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)),
          converter11(child11.eval(input)),
          converter12(child12.eval(input)),
          converter13(child13.eval(input)),
          converter14(child14.eval(input)),
          converter15(child15.eval(input)))
      }

    case 17 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      val child11 = children(11)
      val child12 = children(12)
      val child13 = children(13)
      val child14 = children(14)
      val child15 = children(15)
      val child16 = children(16)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      lazy val converter11 = createToScalaConverter(11, child11.dataType)
      lazy val converter12 = createToScalaConverter(12, child12.dataType)
      lazy val converter13 = createToScalaConverter(13, child13.dataType)
      lazy val converter14 = createToScalaConverter(14, child14.dataType)
      lazy val converter15 = createToScalaConverter(15, child15.dataType)
      lazy val converter16 = createToScalaConverter(16, child16.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)),
          converter11(child11.eval(input)),
          converter12(child12.eval(input)),
          converter13(child13.eval(input)),
          converter14(child14.eval(input)),
          converter15(child15.eval(input)),
          converter16(child16.eval(input)))
      }

    case 18 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      val child11 = children(11)
      val child12 = children(12)
      val child13 = children(13)
      val child14 = children(14)
      val child15 = children(15)
      val child16 = children(16)
      val child17 = children(17)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      lazy val converter11 = createToScalaConverter(11, child11.dataType)
      lazy val converter12 = createToScalaConverter(12, child12.dataType)
      lazy val converter13 = createToScalaConverter(13, child13.dataType)
      lazy val converter14 = createToScalaConverter(14, child14.dataType)
      lazy val converter15 = createToScalaConverter(15, child15.dataType)
      lazy val converter16 = createToScalaConverter(16, child16.dataType)
      lazy val converter17 = createToScalaConverter(17, child17.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)),
          converter11(child11.eval(input)),
          converter12(child12.eval(input)),
          converter13(child13.eval(input)),
          converter14(child14.eval(input)),
          converter15(child15.eval(input)),
          converter16(child16.eval(input)),
          converter17(child17.eval(input)))
      }

    case 19 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      val child11 = children(11)
      val child12 = children(12)
      val child13 = children(13)
      val child14 = children(14)
      val child15 = children(15)
      val child16 = children(16)
      val child17 = children(17)
      val child18 = children(18)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      lazy val converter11 = createToScalaConverter(11, child11.dataType)
      lazy val converter12 = createToScalaConverter(12, child12.dataType)
      lazy val converter13 = createToScalaConverter(13, child13.dataType)
      lazy val converter14 = createToScalaConverter(14, child14.dataType)
      lazy val converter15 = createToScalaConverter(15, child15.dataType)
      lazy val converter16 = createToScalaConverter(16, child16.dataType)
      lazy val converter17 = createToScalaConverter(17, child17.dataType)
      lazy val converter18 = createToScalaConverter(18, child18.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)),
          converter11(child11.eval(input)),
          converter12(child12.eval(input)),
          converter13(child13.eval(input)),
          converter14(child14.eval(input)),
          converter15(child15.eval(input)),
          converter16(child16.eval(input)),
          converter17(child17.eval(input)),
          converter18(child18.eval(input)))
      }

    case 20 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      val child11 = children(11)
      val child12 = children(12)
      val child13 = children(13)
      val child14 = children(14)
      val child15 = children(15)
      val child16 = children(16)
      val child17 = children(17)
      val child18 = children(18)
      val child19 = children(19)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      lazy val converter11 = createToScalaConverter(11, child11.dataType)
      lazy val converter12 = createToScalaConverter(12, child12.dataType)
      lazy val converter13 = createToScalaConverter(13, child13.dataType)
      lazy val converter14 = createToScalaConverter(14, child14.dataType)
      lazy val converter15 = createToScalaConverter(15, child15.dataType)
      lazy val converter16 = createToScalaConverter(16, child16.dataType)
      lazy val converter17 = createToScalaConverter(17, child17.dataType)
      lazy val converter18 = createToScalaConverter(18, child18.dataType)
      lazy val converter19 = createToScalaConverter(19, child19.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)),
          converter11(child11.eval(input)),
          converter12(child12.eval(input)),
          converter13(child13.eval(input)),
          converter14(child14.eval(input)),
          converter15(child15.eval(input)),
          converter16(child16.eval(input)),
          converter17(child17.eval(input)),
          converter18(child18.eval(input)),
          converter19(child19.eval(input)))
      }

    case 21 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      val child11 = children(11)
      val child12 = children(12)
      val child13 = children(13)
      val child14 = children(14)
      val child15 = children(15)
      val child16 = children(16)
      val child17 = children(17)
      val child18 = children(18)
      val child19 = children(19)
      val child20 = children(20)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      lazy val converter11 = createToScalaConverter(11, child11.dataType)
      lazy val converter12 = createToScalaConverter(12, child12.dataType)
      lazy val converter13 = createToScalaConverter(13, child13.dataType)
      lazy val converter14 = createToScalaConverter(14, child14.dataType)
      lazy val converter15 = createToScalaConverter(15, child15.dataType)
      lazy val converter16 = createToScalaConverter(16, child16.dataType)
      lazy val converter17 = createToScalaConverter(17, child17.dataType)
      lazy val converter18 = createToScalaConverter(18, child18.dataType)
      lazy val converter19 = createToScalaConverter(19, child19.dataType)
      lazy val converter20 = createToScalaConverter(20, child20.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)),
          converter11(child11.eval(input)),
          converter12(child12.eval(input)),
          converter13(child13.eval(input)),
          converter14(child14.eval(input)),
          converter15(child15.eval(input)),
          converter16(child16.eval(input)),
          converter17(child17.eval(input)),
          converter18(child18.eval(input)),
          converter19(child19.eval(input)),
          converter20(child20.eval(input)))
      }

    case 22 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      val child11 = children(11)
      val child12 = children(12)
      val child13 = children(13)
      val child14 = children(14)
      val child15 = children(15)
      val child16 = children(16)
      val child17 = children(17)
      val child18 = children(18)
      val child19 = children(19)
      val child20 = children(20)
      val child21 = children(21)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      lazy val converter11 = createToScalaConverter(11, child11.dataType)
      lazy val converter12 = createToScalaConverter(12, child12.dataType)
      lazy val converter13 = createToScalaConverter(13, child13.dataType)
      lazy val converter14 = createToScalaConverter(14, child14.dataType)
      lazy val converter15 = createToScalaConverter(15, child15.dataType)
      lazy val converter16 = createToScalaConverter(16, child16.dataType)
      lazy val converter17 = createToScalaConverter(17, child17.dataType)
      lazy val converter18 = createToScalaConverter(18, child18.dataType)
      lazy val converter19 = createToScalaConverter(19, child19.dataType)
      lazy val converter20 = createToScalaConverter(20, child20.dataType)
      lazy val converter21 = createToScalaConverter(21, child21.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)),
          converter11(child11.eval(input)),
          converter12(child12.eval(input)),
          converter13(child13.eval(input)),
          converter14(child14.eval(input)),
          converter15(child15.eval(input)),
          converter16(child16.eval(input)),
          converter17(child17.eval(input)),
          converter18(child18.eval(input)),
          converter19(child19.eval(input)),
          converter20(child20.eval(input)),
          converter21(child21.eval(input)))
      }
  }

  // scalastyle:on line.size.limit
  override def doGenCode(
      ctx: CodegenContext,
      ev: ExprCode): ExprCode = {
    val converterClassName = classOf[Any => Any].getName

    // 输入和结果的类型转换器
    val (converters, useEncoders): (Array[Any => Any], Array[Boolean]) =
      (children.zipWithIndex.map { case (c, i) =>
        scalaConverter(i, c.dataType)
      }.toArray :+ (catalystConverter, false)).unzip
    val convertersTerm = ctx.addReferenceObj("converters", converters, s"$converterClassName[]")
    val resultTerm = ctx.freshName("result")

    // 为子表达式生成代码
    val evals = children.map(_.genCode(ctx))

    // 生成表达式和调用用户定义函数的代码
    // 我们需要在这里获取 dataType 的 javaType 的 boxedType。因为对于像
    // IntegerType 这样的 dataType，它的 javaType 是 `int`，而用户定义
    // 函数的返回类型是 Object。尝试将 Object 转换为 `int` 会导致转换异常。
    val evalCode = evals.map(_.code).mkString("\n")
    val (funcArgs, initArgs) = evals.zipWithIndex.zip(children.map(_.dataType)).map {
      case ((eval, i), dt) =>
        val argTerm = ctx.freshName("arg")
        // 检查 `inputPrimitives` 当它不为空时，以便找出 Option
        // 类型作为非原始类型，例如 Option[Int]。当 `inputPrimitives` 为空时
        // 回退到 `isPrimitive`，用于其他情况，例如 Java UDF、无类型 Scala UDF
        val primitive = (inputPrimitives.isEmpty && isPrimitive(dt)) ||
          (inputPrimitives.nonEmpty && inputPrimitives(i))
        val initArg = if (primitive) {
          val convertedTerm = ctx.freshName("conv")
          s"""
             |${CodeGenerator.boxedType(dt)} $convertedTerm = ${eval.value};
             |Object $argTerm = ${eval.isNull} ? null : $convertedTerm;
           """.stripMargin
        } else if (useEncoders(i)) {
          s"""
             |Object $argTerm = null;
             |if (${eval.isNull}) {
             |  $argTerm = $convertersTerm[$i].apply(null);
             |} else {
             |  $argTerm = $convertersTerm[$i].apply(${eval.value});
             |}
          """.stripMargin
        } else {
          s"Object $argTerm = ${eval.isNull} ? null : $convertersTerm[$i].apply(${eval.value});"
        }
        (argTerm, initArg)
    }.unzip

    val udf = ctx.addReferenceObj("udf", function, s"scala.Function${children.length}")
    val getFuncResult = s"$udf.apply(${funcArgs.mkString(", ")})"
    val resultConverter = s"$convertersTerm[${children.length}]"
    val boxedType = CodeGenerator.boxedType(dataType)

    val funcInvocation = if (isPrimitive(dataType)
      // 如果输出可为空，则返回值必须从 Option 中解包
        && !nullable) {
      s"$resultTerm = ($boxedType)$getFuncResult"
    } else {
      s"$resultTerm = ($boxedType)$resultConverter.apply($getFuncResult)"
    }
    val callFunc =
      s"""
         |$boxedType $resultTerm = null;
         |try {
         |  $funcInvocation;
         |} catch (Throwable e) {
         |  throw QueryExecutionErrors.failedExecuteUserDefinedFunctionError(
         |    "$funcCls", "$inputTypesString", "$outputType", e);
         |}
       """.stripMargin

    ev.copy(code =
      code"""
         |$evalCode
         |${initArgs.mkString("\n")}
         |$callFunc
         |
         |boolean ${ev.isNull} = $resultTerm == null;
         |${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
         |if (!${ev.isNull}) {
         |  ${ev.value} = $resultTerm;
         |}
       """.stripMargin)
  }

  private[this] val resultConverter = catalystConverter

  lazy val funcCls = Utils.getSimpleName(function.getClass)
  lazy val inputTypesString = children.map(_.dataType.catalogString).mkString(", ")
  lazy val outputType = dataType.catalogString

  override def eval(input: InternalRow): Any = {
    val result = try {
      f(input)
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.failedExecuteUserDefinedFunctionError(
          funcCls, inputTypesString, outputType, e)
    }

    resultConverter(result)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): ScalaUDF =
    copy(children = newChildren)
}
