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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.analysis.TypeCoercion.numericPrecedence
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._

/**
 * 在 Spark 的 ANSI 模式下，类型强制转换规则基于输入数据类型的“类型优先级列表”。
 * 根据 ISO/IEC 9075-2:2011《信息技术-数据库语言-SQL-第2部分：基础（SQL/Foundation）》中“类型优先级列表的确定”一节，原始数据类型的优先级列表如下：
 *   * Byte: Byte, Short, Int, Long, Decimal, Float, Double
 *   * Short: Short, Int, Long, Decimal, Float, Double
 *   * Int: Int, Long, Decimal, Float, Double
 *   * Long: Long, Decimal, Float, Double
 *   * Decimal: Float, Double, or any wider Numeric type
 *   * Float: Float, Double
 *   * Double: Double
 *   * String: String
 *   * Date: Date, Timestamp
 *   * Timestamp: Timestamp
 *   * Binary: Binary
 *   * Boolean: Boolean
 *   * Interval: Interval
 * 对于复杂类型（如 struct、array、map），Spark 会根据它们的子类型及可空性递归地确定优先级列表。
 *
 * 有了类型优先级列表的定义，通用的类型强制转换规则如下：
 *  * 如果类型 T 在类型 S 的优先级列表中，则允许将数据类型 S 隐式转换为 T；
 *  * 若左右两边的数据类型优先级列表中至少有一个共同元素，则允许进行比较操作；
 *
 * 在执行比较时，Spark 会将两边都转换为它们优先级列表中的最紧凑的共同数据类型；
 *
 * 对于如下操作符，所有子表达式的优先级列表中必须至少有一个共同的数据类型，最终操作符的类型为最紧凑的共同优先类型：
 *       * Except
 *       * Intersect
 *       * Greatest
 *       * Least
 *       * Union
 *       * If
 *       * CaseWhen
 *       * CreateArray
 *       * Array Concat
 *       * Sequence
 *       * MapConcat
 *       * CreateMap
 *   * 对于复杂类型（struct、array、map），Spark 会递归查找元素类型并应用上述规则。
 *
 *
 * 注意：为了避免破坏大量现有 Spark SQL 查询，这个新的类型强制系统允许将 String 类型隐式转换为其他原始类型。
 * 这是 Spark 的特殊规则，并非来源于 ANSI SQL 标准。
 */
object AnsiTypeCoercion extends TypeCoercionBase {
  override def typeCoercionRules: List[Rule[LogicalPlan]] =
    WidenSetOperationTypes ::
    new AnsiCombinedTypeCoercionRule(
      InConversion ::
      PromoteStrings ::
      DecimalPrecision ::
      FunctionArgumentConversion ::
      ConcatCoercion ::
      MapZipWithCoercion ::
      EltCoercion ::
      CaseWhenCoercion ::
      IfCoercion ::
      StackCoercion ::
      Division ::
      IntegralDivision ::
      ImplicitTypeCasts ::
      DateTimeOperations ::
      WindowFrameCoercion ::
      GetDateFieldOperations:: Nil) :: Nil

  val findTightestCommonType: (DataType, DataType) => Option[DataType] = {
    case (t1, t2) if t1 == t2 => Some(t1)
    case (NullType, t1) => Some(t1)
    case (t1, NullType) => Some(t1)

    case (t1: IntegralType, t2: DecimalType) if t2.isWiderThan(t1) =>
      Some(t2)
    case (t1: DecimalType, t2: IntegralType) if t1.isWiderThan(t2) =>
      Some(t1)

    case (t1: NumericType, t2: NumericType)
        if !t1.isInstanceOf[DecimalType] && !t2.isInstanceOf[DecimalType] =>
      val index = numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
      val widerType = numericPrecedence(index)
      if (widerType == FloatType) {
        // If the input type is an Integral type and a Float type, simply return Double type as
        // the tightest common type to avoid potential precision loss on converting the Integral
        // type as Float type.
        Some(DoubleType)
      } else {
        Some(widerType)
      }

    case (_: TimestampType, _: DateType) | (_: DateType, _: TimestampType) =>
      Some(TimestampType)

    case (t1: DayTimeIntervalType, t2: DayTimeIntervalType) =>
      Some(DayTimeIntervalType(t1.startField.min(t2.startField), t1.endField.max(t2.endField)))
    case (t1: YearMonthIntervalType, t2: YearMonthIntervalType) =>
      Some(YearMonthIntervalType(t1.startField.min(t2.startField), t1.endField.max(t2.endField)))

    case (t1, t2) => findTypeForComplex(t1, t2, findTightestCommonType)
  }

  override def findWiderTypeForTwo(t1: DataType, t2: DataType): Option[DataType] = {
    findTightestCommonType(t1, t2)
      .orElse(findWiderTypeForDecimal(t1, t2))
      .orElse(findWiderTypeForString(t1, t2))
      .orElse(findTypeForComplex(t1, t2, findWiderTypeForTwo))
  }

  /** Promotes StringType to other data types. */
  @scala.annotation.tailrec
  private def findWiderTypeForString(dt1: DataType, dt2: DataType): Option[DataType] = {
    (dt1, dt2) match {
      case (StringType, _: IntegralType) => Some(LongType)
      case (StringType, _: FractionalType) => Some(DoubleType)
      case (StringType, NullType) => Some(StringType)
      // If a binary operation contains interval type and string, we can't decide which
      // interval type the string should be promoted as. There are many possible interval
      // types, such as year interval, month interval, day interval, hour interval, etc.
      case (StringType, _: AnsiIntervalType) => None
      case (StringType, a: AtomicType) => Some(a)
      case (other, StringType) if other != StringType => findWiderTypeForString(StringType, other)
      case _ => None
    }
  }

  override def findWiderCommonType(types: Seq[DataType]): Option[DataType] = {
    types.foldLeft[Option[DataType]](Some(NullType))((r, c) =>
      r match {
        case Some(d) => findWiderTypeForTwo(d, c)
        case _ => None
      })
  }

  override def implicitCast(e: Expression, expectedType: AbstractDataType): Option[Expression] = {
    implicitCast(e.dataType, expectedType).map { dt =>
      if (dt == e.dataType) e else Cast(e, dt)
    }
  }

  /**
   * In Ansi mode, the implicit cast is only allow when `expectedType` is in the type precedent
   * list of `inType`.
   */
  private def implicitCast(
      inType: DataType,
      expectedType: AbstractDataType): Option[DataType] = {
    (inType, expectedType) match {
      // If the expected type equals the input type, no need to cast.
      case _ if expectedType.acceptsType(inType) => Some(inType)

      // If input is a numeric type but not decimal, and we expect a decimal type,
      // cast the input to decimal.
      case (n: NumericType, DecimalType) => Some(DecimalType.forType(n))

      // Cast null type (usually from null literals) into target types
      // By default, the result type is `target.defaultConcreteType`. When the target type is
      // `TypeCollection`, there is another branch to find the "closet convertible data type" below.
      case (NullType, target) if !target.isInstanceOf[TypeCollection] =>
        Some(target.defaultConcreteType)

      // This type coercion system will allow implicit converting String type as other
      // primitive types, in case of breaking too many existing Spark SQL queries.
      case (StringType, a: AtomicType) =>
        Some(a)

      // If the target type is any Numeric type, convert the String type as Double type.
      case (StringType, NumericType) =>
        Some(DoubleType)

      // If the target type is any Decimal type, convert the String type as the default
      // Decimal type.
      case (StringType, DecimalType) =>
        Some(DecimalType.SYSTEM_DEFAULT)

      // If the target type is any timestamp type, convert the String type as the default
      // Timestamp type.
      case (StringType, AnyTimestampType) =>
        Some(AnyTimestampType.defaultConcreteType)

      case (DateType, AnyTimestampType) =>
        Some(AnyTimestampType.defaultConcreteType)

      case (_, target: DataType) =>
        if (Cast.canANSIStoreAssign(inType, target)) {
          Some(target)
        } else {
          None
        }

      // When we reach here, input type is not acceptable for any types in this type collection,
      // try to find the first one we can implicitly cast.
      case (_, TypeCollection(types)) =>
        types.flatMap(implicitCast(inType, _)).headOption

      case _ => None
    }
  }

  override def canCast(from: DataType, to: DataType): Boolean = AnsiCast.canCast(from, to)

  object PromoteStrings extends TypeCoercionRule {
    private def castExpr(expr: Expression, targetType: DataType): Expression = {
      expr.dataType match {
        case NullType => Literal.create(null, targetType)
        case l if l != targetType => Cast(expr, targetType)
        case _ => expr
      }
    }

    override def transform: PartialFunction[Expression, Expression] = {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case b @ BinaryOperator(left, right)
        if findWiderTypeForString(left.dataType, right.dataType).isDefined =>
        val promoteType = findWiderTypeForString(left.dataType, right.dataType).get
        b.withNewChildren(Seq(castExpr(left, promoteType), castExpr(right, promoteType)))

      case Abs(e @ StringType(), failOnError) => Abs(Cast(e, DoubleType), failOnError)
      case m @ UnaryMinus(e @ StringType(), _) => m.withNewChildren(Seq(Cast(e, DoubleType)))
      case UnaryPositive(e @ StringType()) => UnaryPositive(Cast(e, DoubleType))

      case d @ DateAdd(left @ StringType(), _) =>
        d.copy(startDate = Cast(d.startDate, DateType))
      case d @ DateAdd(_, right @ StringType()) =>
        d.copy(days = Cast(right, IntegerType))
      case d @ DateSub(left @ StringType(), _) =>
        d.copy(startDate = Cast(d.startDate, DateType))
      case d @ DateSub(_, right @ StringType()) =>
        d.copy(days = Cast(right, IntegerType))

      case s @ SubtractDates(left @ StringType(), _, _) =>
        s.copy(left = Cast(s.left, DateType))
      case s @ SubtractDates(_, right @ StringType(), _) =>
        s.copy(right = Cast(s.right, DateType))
      case t @ TimeAdd(left @ StringType(), _, _) =>
        t.copy(start = Cast(t.start, TimestampType))
      case t @ SubtractTimestamps(left @ StringType(), _, _, _) =>
        t.copy(left = Cast(t.left, t.right.dataType))
      case t @ SubtractTimestamps(_, right @ StringType(), _, _) =>
        t.copy(right = Cast(right, t.left.dataType))
    }
  }

  /**
   * When getting a date field from a Timestamp column, cast the column as date type.
   *
   * This is Spark's hack to make the implementation simple. In the default type coercion rules,
   * the implicit cast rule does the work. However, The ANSI implicit cast rule doesn't allow
   * converting Timestamp type as Date type, so we need to have this additional rule
   * to make sure the date field extraction from Timestamp columns works.
   */
  object GetDateFieldOperations extends TypeCoercionRule {
    override def transform: PartialFunction[Expression, Expression] = {
      // Skip nodes who's children have not been resolved yet.
      case g if !g.childrenResolved => g

      case g: GetDateField if AnyTimestampType.unapply(g.child) =>
        g.withNewChildren(Seq(Cast(g.child, DateType)))
    }
  }

  object DateTimeOperations extends TypeCoercionRule {
    override val transform: PartialFunction[Expression, Expression] = {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case d @ DateAdd(AnyTimestampType(), _) => d.copy(startDate = Cast(d.startDate, DateType))
      case d @ DateSub(AnyTimestampType(), _) => d.copy(startDate = Cast(d.startDate, DateType))

      case s @ SubtractTimestamps(DateType(), AnyTimestampType(), _, _) =>
        s.copy(left = Cast(s.left, s.right.dataType))
      case s @ SubtractTimestamps(AnyTimestampType(), DateType(), _, _) =>
        s.copy(right = Cast(s.right, s.left.dataType))
      case s @ SubtractTimestamps(AnyTimestampType(), AnyTimestampType(), _, _)
        if s.left.dataType != s.right.dataType =>
        val newLeft = castIfNotSameType(s.left, TimestampNTZType)
        val newRight = castIfNotSameType(s.right, TimestampNTZType)
        s.copy(left = newLeft, right = newRight)
    }
  }

  // This is for generating a new rule id, so that we can run both default and Ansi
  // type coercion rules against one logical plan.
  class AnsiCombinedTypeCoercionRule(rules: Seq[TypeCoercionRule]) extends
    CombinedTypeCoercionRule(rules)
}
