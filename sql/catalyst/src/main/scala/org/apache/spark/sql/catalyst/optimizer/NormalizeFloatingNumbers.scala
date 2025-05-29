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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{Alias, And, ArrayTransform, CaseWhen, Coalesce, CreateArray, CreateMap, CreateNamedStruct, EqualTo, ExpectsInputTypes, Expression, GetStructField, If, IsNull, KnownFloatingPointNormalized, LambdaFunction, Literal, NamedLambdaVariable, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Window}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.types._

/**
 * 我们需要在多个地方处理特殊的浮点数（NaN 和 -0.0）：
 *  - 当比较值时，不同的 NaN 应被视为相同，-0.0 和 0.0 也应被视为相同。
 *  - 在聚合分组键中，不同的 NaN 应属于同一组，-0.0 和 0.0 应属于同一组。
 *  - 在连接键中，不同的 NaN 应被视为相同，-0.0 和 0.0 也应被视为相同。
 *  - 在窗口分区键中，不同的 NaN 应属于同一个分区，-0.0 和 0.0 也应属于同一个分区。
 *
 * 情况 1 没有问题，因为我们在比较时已经很好地处理了 NaN 和 -0.0。对于复杂类型，我们递归地比较字段/元素，所以也没有问题。
 *
 * 情况 2、3 和 4 存在问题，因为 Spark SQL 会将分组/连接/窗口分区键转换为二进制 UnsafeRow 并直接比较二进制数据。
 * 不同的 NaN 具有不同的二进制表示，-0.0 和 0.0 同样如此。
 *
 * 本规则对窗口分区键、连接键和聚合分组键中的 NaN 和 -0.0 进行标准化处理。
 *
 * 理想情况下，我们应该在物理运算符中对二进制 UnsafeRow 的直接比较进行标准化处理。
 * 如果 Spark SQL 执行引擎没有针对二进制数据进行优化，就不需要这种标准化。
 * 创建此规则是为了简化实现，使我们可以在一个统一的地方进行标准化，从而更易于维护。
 *
 * 请注意，此规则必须在优化器的最后阶段执行，因为优化器可能会创建新的连接（子查询重写）和新的连接条件（连接重排序）。
 */
object NormalizeFloatingNumbers extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case _ => plan.transformWithPruning( _.containsAnyPattern(WINDOW, JOIN)) {
      case w: Window if w.partitionSpec.exists(p => needNormalize(p)) =>
        // Although the `windowExpressions` may refer to `partitionSpec` expressions, we don't need
        // to normalize the `windowExpressions`, as they are executed per input row and should take
        // the input row as it is.
        w.copy(partitionSpec = w.partitionSpec.map(normalize))

      // Only hash join and sort merge join need the normalization. Here we catch all Joins with
      // join keys, assuming Joins with join keys are always planned as hash join or sort merge
      // join. It's very unlikely that we will break this assumption in the near future.
      case j @ ExtractEquiJoinKeys(_, leftKeys, rightKeys, condition, _, _, _, _)
          // The analyzer guarantees left and right joins keys are of the same data type. Here we
          // only need to check join keys of one side.
          if leftKeys.exists(k => needNormalize(k)) =>
        val newLeftJoinKeys = leftKeys.map(normalize)
        val newRightJoinKeys = rightKeys.map(normalize)
        val newConditions = newLeftJoinKeys.zip(newRightJoinKeys).map {
          case (l, r) => EqualTo(l, r)
        } ++ condition
        j.copy(condition = Some(newConditions.reduce(And)))

      // TODO: ideally Aggregate should also be handled here, but its grouping expressions are
      // mixed in its aggregate expressions. It's unreliable to change the grouping expressions
      // here. For now we normalize grouping expressions in `AggUtils` during planning.
    }
  }

  /**
   * Short circuit if the underlying expression is already normalized
   */
  private def needNormalize(expr: Expression): Boolean = expr match {
    case KnownFloatingPointNormalized(_) => false
    case _ => needNormalize(expr.dataType)
  }

  private def needNormalize(dt: DataType): Boolean = dt match {
    case FloatType | DoubleType => true
    case StructType(fields) => fields.exists(f => needNormalize(f.dataType))
    case ArrayType(et, _) => needNormalize(et)
    // Currently MapType is not comparable and analyzer should fail earlier if this case happens.
    case _: MapType =>
      throw new IllegalStateException("grouping/join/window partition keys cannot be map type.")
    case _ => false
  }

  private[sql] def normalize(expr: Expression): Expression = expr match {
    case _ if !needNormalize(expr) => expr

    case a: Alias =>
      a.withNewChildren(Seq(normalize(a.child)))

    case CreateNamedStruct(children) =>
      CreateNamedStruct(children.map(normalize))

    case CreateArray(children, useStringTypeWhenEmpty) =>
      CreateArray(children.map(normalize), useStringTypeWhenEmpty)

    case CreateMap(children, useStringTypeWhenEmpty) =>
      CreateMap(children.map(normalize), useStringTypeWhenEmpty)

    case _ if expr.dataType == FloatType || expr.dataType == DoubleType =>
      KnownFloatingPointNormalized(NormalizeNaNAndZero(expr))

    case If(cond, trueValue, falseValue) =>
      If(cond, normalize(trueValue), normalize(falseValue))

    case CaseWhen(branches, elseVale) =>
      CaseWhen(branches.map(br => (br._1, normalize(br._2))), elseVale.map(normalize))

    case Coalesce(children) =>
      Coalesce(children.map(normalize))

    case _ if expr.dataType.isInstanceOf[StructType] =>
      val fields = expr.dataType.asInstanceOf[StructType].fieldNames.zipWithIndex.map {
        case (name, i) => Seq(Literal(name), normalize(GetStructField(expr, i)))
      }
      val struct = CreateNamedStruct(fields.flatten.toSeq)
      KnownFloatingPointNormalized(If(IsNull(expr), Literal(null, struct.dataType), struct))

    case _ if expr.dataType.isInstanceOf[ArrayType] =>
      val ArrayType(et, containsNull) = expr.dataType
      val lv = NamedLambdaVariable("arg", et, containsNull)
      val function = normalize(lv)
      KnownFloatingPointNormalized(ArrayTransform(expr, LambdaFunction(function, Seq(lv))))

    case _ => throw new IllegalStateException(s"fail to normalize $expr")
  }

  val FLOAT_NORMALIZER: Any => Any = (input: Any) => {
    val f = input.asInstanceOf[Float]
    if (f.isNaN) {
      Float.NaN
    } else if (f == -0.0f) {
      0.0f
    } else {
      f
    }
  }

  val DOUBLE_NORMALIZER: Any => Any = (input: Any) => {
    val d = input.asInstanceOf[Double]
    if (d.isNaN) {
      Double.NaN
    } else if (d == -0.0d) {
      0.0d
    } else {
      d
    }
  }
}

case class NormalizeNaNAndZero(child: Expression) extends UnaryExpression with ExpectsInputTypes {

  override def dataType: DataType = child.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(FloatType, DoubleType))

  private lazy val normalizer: Any => Any = child.dataType match {
    case FloatType => NormalizeFloatingNumbers.FLOAT_NORMALIZER
    case DoubleType => NormalizeFloatingNumbers.DOUBLE_NORMALIZER
  }

  override def nullSafeEval(input: Any): Any = {
    normalizer(input)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val codeToNormalize = child.dataType match {
      case FloatType => (f: String) => {
        s"""
           |if (Float.isNaN($f)) {
           |  ${ev.value} = Float.NaN;
           |} else if ($f == -0.0f) {
           |  ${ev.value} = 0.0f;
           |} else {
           |  ${ev.value} = $f;
           |}
         """.stripMargin
      }

      case DoubleType => (d: String) => {
        s"""
           |if (Double.isNaN($d)) {
           |  ${ev.value} = Double.NaN;
           |} else if ($d == -0.0d) {
           |  ${ev.value} = 0.0d;
           |} else {
           |  ${ev.value} = $d;
           |}
         """.stripMargin
      }
    }

    nullSafeCodeGen(ctx, ev, codeToNormalize)
  }

  override protected def withNewChildInternal(newChild: Expression): NormalizeNaNAndZero =
    copy(child = newChild)
}
