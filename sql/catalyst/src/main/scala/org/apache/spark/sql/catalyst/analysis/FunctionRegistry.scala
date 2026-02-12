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

import java.util.Locale
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.SparkThrowable
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.xml._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Range}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


/**
 * 用于查找用户定义函数的目录，由 [[Analyzer]] 使用。
 * 注意：
 * 1）实现应该是线程安全的，以允许并发访问。
 * 2）这里的数据库名称始终区分大小写，调用方负责根据区分大小写的配置格式化数据库名称。
 */
trait FunctionRegistryBase[T] {

  type FunctionBuilder = Seq[Expression] => T

  // 注册函数
  final def registerFunction(
      name: FunctionIdentifier, builder: FunctionBuilder, source: String): Unit = {
    val info = new ExpressionInfo(
      builder.getClass.getCanonicalName,
      name.database.orNull,
      name.funcName,
      null,
      "",
      "",
      "",
      "",
      "",
      "",
      source)
    registerFunction(name, info, builder)
  }

  def registerFunction(
    name: FunctionIdentifier,
    info: ExpressionInfo,
    builder: FunctionBuilder): Unit

  /* 创建或替换临时函数。 */
  final def createOrReplaceTempFunction(
      name: String, builder: FunctionBuilder, source: String): Unit = {
    registerFunction(
      FunctionIdentifier(name),
      builder,
      source)
  }

  @throws[AnalysisException]("If function does not exist")
  def lookupFunction(name: FunctionIdentifier, children: Seq[Expression]): T

  /* 列出所有已注册的函数名称。 */
  def listFunction(): Seq[FunctionIdentifier]

  /* 根据指定名称获取已注册函数的类。 */
  def lookupFunction(name: FunctionIdentifier): Option[ExpressionInfo]

  /* 根据指定名称获取已注册函数的构建器。 */
  def lookupFunctionBuilder(name: FunctionIdentifier): Option[FunctionBuilder]

  /** 删除函数并返回该函数是否存在。 */
  def dropFunction(name: FunctionIdentifier): Boolean

  /** 检查给定名称的函数是否存在。 */
  def functionExists(name: FunctionIdentifier): Boolean = lookupFunction(name).isDefined

  /** 清除所有已注册的函数。 */
  def clear(): Unit
}

object FunctionRegistryBase {

  /** 返回由 T 定义的函数的表达式信息和函数构建器，使用给定的名称。 */
  def build[T : ClassTag](name: String, since: Option[String]): (ExpressionInfo, Seq[Expression] => T) = {

    // 通过反射获取类型 T 的运行时 Class 对象。
    val runtimeClass = scala.reflect.classTag[T].runtimeClass

    // 检查 T 是否是 InheritAnalysisRules 的子类。
    val isRuntime = classOf[InheritAnalysisRules].isAssignableFrom(runtimeClass)

    val constructors = if (isRuntime) {
      val all = runtimeClass.getConstructors            // 获取构造函数
      val maxNumArgs = all.map(_.getParameterCount).max
      all.filterNot(_.getParameterCount == maxNumArgs) // 过滤掉参数最多的主构造函数（因为它包含 replacement 参数，不应该用作函数构建器）
    } else {
      runtimeClass.getConstructors                     // 获取构造函数
    }
    // 查看是否可以找到接受 Seq[Expression] 的构造函数
    // 因为类型擦除， 并不能知道类型具体是否是 Seq[Expression]
    val varargCtor = constructors.find(_.getParameterTypes.toSeq == Seq(classOf[Seq[_]]))

    // 定义构造器
    val builder = (expressions: Seq[Expression]) => {
      if (varargCtor.isDefined) {           // builder : (seq[expression]) => T
        // 如果有接受 Seq[Expression] 的 apply 方法，使用它。
        try {
          varargCtor.get.newInstance(expressions).asInstanceOf[T]
        } catch {
          // 异常是调用异常。要获得有意义的消息，我们需要原因。
          case e: Exception =>
            throw e.getCause match {
              case ae: SparkThrowable => ae
              case _ => new AnalysisException(e.getCause.getMessage)
            }
        }
      } else {                              // builder : (expression, expresion, ...) => T
        // 否则，找到与参数数量匹配的构造函数方法，并使用它。
        val params = Seq.fill(expressions.size)(classOf[Expression])                // 创建一个包含n个Expression.class 的序列
        val f = constructors.find(_.getParameterTypes.toSeq == params).getOrElse {  // 表示他是一个 (Expresion, Expresion,... ) 类型
          val validParametersCount = constructors
            .filter(_.getParameterTypes.forall(_ == classOf[Expression]))           // 找到所有的只包含 Expression 类型的参数
            .map(_.getParameterCount).distinct.sorted                               // 按照数量进行排序
          throw QueryCompilationErrors.invalidFunctionArgumentNumberError(validParametersCount, name, params.length)  // 报错提示
        }
        try {
          f.newInstance(expressions : _*).asInstanceOf[T]
        } catch {
          // 异常是调用异常。要获得有意义的消息，我们需要原因。
          case e: Exception => throw new AnalysisException(e.getCause.getMessage)
        }
      }
    }

    (expressionInfo(name, since), builder)
  }


  /**
   * Creates an [[ExpressionInfo]] for the function as defined by 使用给定的名称。
   */
  def expressionInfo[T : ClassTag](name: String, since: Option[String]): ExpressionInfo = {
    val clazz = scala.reflect.classTag[T].runtimeClass              // 获取当前类的运行时实例
    val df = clazz.getAnnotation(classOf[ExpressionDescription])    // 获取当前类注解
    if (df != null) {
      if (df.extended().isEmpty) {
        new ExpressionInfo(
          clazz.getCanonicalName.stripSuffix("$"),    // 类名
          null,
          name,
          df.usage(),
          df.arguments(),
          df.examples(),
          df.note(),
          df.group(),
          since.getOrElse(df.since()),
          df.deprecated(),
          df.source())
      } else {
        // 这是为了向后兼容旧的 ExpressionDescription，它们在
        // extended() 中定义扩展描述。
        new ExpressionInfo(
          clazz.getCanonicalName.stripSuffix("$"), null, name, df.usage(), df.extended())
      }
    } else {
      new ExpressionInfo(clazz.getCanonicalName.stripSuffix("$"), name)
    }
  }
}

trait SimpleFunctionRegistryBase[T] extends FunctionRegistryBase[T] with Logging {

  @GuardedBy("this")
  protected val functionBuilders = new mutable.HashMap[FunctionIdentifier, (ExpressionInfo, FunctionBuilder)]

  // 函数名称的解析始终不区分大小写，但数据库名称
  // 取决于调用者
  private def normalizeFuncName(name: FunctionIdentifier): FunctionIdentifier = {
    FunctionIdentifier(name.funcName.toLowerCase(Locale.ROOT), name.database)
  }

  override def registerFunction(name: FunctionIdentifier, info: ExpressionInfo, builder: FunctionBuilder): Unit = {
    val normalizedName = normalizeFuncName(name)
    internalRegisterFunction(normalizedName, info, builder)
  }

  /**
   * 执行函数注册而不进行任何预处理。
   * 这在注册内置函数和执行 FunctionRegistry.clone() 时使用
   */
  def internalRegisterFunction(name: FunctionIdentifier, info: ExpressionInfo, builder: FunctionBuilder): Unit = synchronized {
    val newFunction = (info, builder)
    functionBuilders.put(name, newFunction) match {
      case Some(previousFunction) if previousFunction != newFunction =>
        logWarning(s"The function $name replaced a previously registered function.")
      case _ =>
    }
  }

  override def lookupFunction(name: FunctionIdentifier, children: Seq[Expression]): T = {
    val func = synchronized {
      functionBuilders.get(normalizeFuncName(name)).map(_._2).getOrElse {
        throw QueryCompilationErrors.functionUndefinedError(name)
      }
    }
    func(children)
  }

  override def listFunction(): Seq[FunctionIdentifier] = synchronized {
    functionBuilders.iterator.map(_._1).toList
  }

  override def lookupFunction(name: FunctionIdentifier): Option[ExpressionInfo] = synchronized {
    functionBuilders.get(normalizeFuncName(name)).map(_._1)
  }

  override def lookupFunctionBuilder(
      name: FunctionIdentifier): Option[FunctionBuilder] = synchronized {
    functionBuilders.get(normalizeFuncName(name)).map(_._2)
  }

  override def dropFunction(name: FunctionIdentifier): Boolean = synchronized {
    functionBuilders.remove(normalizeFuncName(name)).isDefined
  }

  override def clear(): Unit = synchronized {
    functionBuilders.clear()
  }
}

/**
 * 一个简单的目录，当请求函数时返回错误。用于测试，当所有
 * 函数都已填充，分析器只需要解析属性引用时使用。
 */
trait EmptyFunctionRegistryBase[T] extends FunctionRegistryBase[T] {
  override def registerFunction(
      name: FunctionIdentifier, info: ExpressionInfo, builder: FunctionBuilder): Unit = {
    throw new UnsupportedOperationException
  }

  override def lookupFunction(name: FunctionIdentifier, children: Seq[Expression]): T = {
    throw new UnsupportedOperationException
  }

  override def listFunction(): Seq[FunctionIdentifier] = {
    throw new UnsupportedOperationException
  }

  override def lookupFunction(name: FunctionIdentifier): Option[ExpressionInfo] = {
    throw new UnsupportedOperationException
  }

  override def lookupFunctionBuilder(name: FunctionIdentifier): Option[FunctionBuilder] = {
    throw new UnsupportedOperationException
  }

  override def dropFunction(name: FunctionIdentifier): Boolean = {
    throw new UnsupportedOperationException
  }

  override def clear(): Unit = {
    throw new UnsupportedOperationException
  }
}

trait FunctionRegistry extends FunctionRegistryBase[Expression] {

  /** 创建此注册表的副本，其中包含与此注册表相同的函数。 */
  override def clone(): FunctionRegistry = throw new CloneNotSupportedException()
}

class SimpleFunctionRegistry
    extends SimpleFunctionRegistryBase[Expression]
    with FunctionRegistry {

  override def clone(): SimpleFunctionRegistry = synchronized {
    val registry = new SimpleFunctionRegistry
    functionBuilders.iterator.foreach { case (name, (info, builder)) =>
      registry.internalRegisterFunction(name, info, builder)
    }
    registry
  }
}

object EmptyFunctionRegistry
    extends EmptyFunctionRegistryBase[Expression]
    with FunctionRegistry {

  override def clone(): FunctionRegistry = this
}

object FunctionRegistry {

  type FunctionBuilder = Seq[Expression] => Expression

  val FUNC_ALIAS = TreeNodeTag[String]("functionAliasName")

  // ==============================================================================================
  //                          添加 SQL 函数的指南
  // ==============================================================================================
  // 要添加 SQL 函数，我们通常需要为该函数创建一个新的 Expression， 并在 Expression 的解释代码路径和代码生成路径中实现函数逻辑。
  // 我们还需要通过扩展 ImplicitCastInputTypes 或直接更新类型强制转换规则来定义函数输入的类型强制转换行为。
  //
  // 如果 SQL 函数可以用现有表达式实现，则会简单得多。有几种情况：
  //
  //   - 函数只是另一个函数的别名。我们可以用不同的函数名称注册相同的表达式， 例如 expression[Rand]("random", true)。
  //   - 函数与另一个函数基本相同，但参数列表不同。
  //     我们可以使用 RuntimeReplaceable 创建新表达式，它可以自定义参数列表和分析行为（类型强制转换）。
  //     RuntimeReplaceable 表达式将在分析结束时被实际表达式替换。参见 Left 作为示例。
  //   - 函数可以通过组合一些现有表达式来实现。我们可以使用 RuntimeReplaceable 来定义组合。
  //     参见 ParseToDate 作为示例。 要从替换表达式继承分析行为，请将 InheritAnalysisRules 与 RuntimeReplaceable 混合使用。
  //     参见 TryAdd 作为示例。
  //   - 对于 AggregateFunction，应该混合使用 RuntimeReplaceableAggregate。 参见 CountIf 作为示例。
  //
  // 有时，多个函数共享相同/相似的表达式替换逻辑，创建许多相似的 RuntimeReplaceable 表达式会很繁琐。
  // 我们可以使用 ExpressionBuilder 来共享替换逻辑。 参见 ParseToTimestampLTZExpressionBuilder 作为示例。
  //
  // 使用这些工具，我们甚至可以用 Java（静态）方法实现新的 SQL 函数，
  // 然后创建一个 RuntimeReplaceable 表达式，使用 Invoke 或 StaticInvoke 表达式 调用 Java 方法。
  // 这样我们就不需要再为新函数实现代码生成了。 参见 AesEncrypt/AesDecrypt 作为示例。
  val expressionsForTimestampNTZSupport: Map[String, (ExpressionInfo, FunctionBuilder)] =
    // SPARK-38813：在 Spark 3.3 中以最小的代码更改删除 TimestampNTZ 类型支持。
    if (Utils.isTesting) {
      Map(
        expression[LocalTimestamp]("localtimestamp"),
        expression[ConvertTimezone]("convert_timezone"),
        // 我们保留下面的 2 个表达式构建器以具有不同的函数文档。
        expressionBuilder(
          "to_timestamp_ntz", ParseToTimestampNTZExpressionBuilder, setAlias = true),
        expressionBuilder(
          "to_timestamp_ltz", ParseToTimestampLTZExpressionBuilder, setAlias = true),
        // 我们保留下面的 2 个表达式构建器以具有不同的函数文档。
        expressionBuilder("make_timestamp_ntz", MakeTimestampNTZExpressionBuilder, setAlias = true),
        expressionBuilder("make_timestamp_ltz", MakeTimestampLTZExpressionBuilder, setAlias = true)
      )
    } else {
      Map.empty
    }

  // 注意： Whenever we add a new entry here, make sure we also update ExpressionToSQLSuite
  val expressions: Map[String, (ExpressionInfo, FunctionBuilder)] = Map(
    // 杂项非聚合函数
    expression[Abs]("abs"),
    expression[Coalesce]("coalesce"),
    expression[Explode]("explode"),
    expressionGeneratorOuter[Explode]("explode_outer"),
    expression[Greatest]("greatest"),
    expression[If]("if"),
    expression[Inline]("inline"),
    expressionGeneratorOuter[Inline]("inline_outer"),
    expression[IsNaN]("isnan"),
    expression[Nvl]("ifnull", setAlias = true),
    expression[IsNull]("isnull"),
    expression[IsNotNull]("isnotnull"),
    expression[Least]("least"),
    expression[NaNvl]("nanvl"),
    expression[NullIf]("nullif"),
    expression[Nvl]("nvl"),
    expression[Nvl2]("nvl2"),
    expression[PosExplode]("posexplode"),
    expressionGeneratorOuter[PosExplode]("posexplode_outer"),
    expression[Rand]("rand"),
    expression[Rand]("random", true),
    expression[Randn]("randn"),
    expression[Stack]("stack"),
    expression[CaseWhen]("when"),

    // 数学函数
    expression[Acos]("acos"),
    expression[Acosh]("acosh"),
    expression[Asin]("asin"),
    expression[Asinh]("asinh"),
    expression[Atan]("atan"),
    expression[Atan2]("atan2"),
    expression[Atanh]("atanh"),
    expression[Bin]("bin"),
    expression[BRound]("bround"),
    expression[Cbrt]("cbrt"),
    expressionBuilder("ceil", CeilExpressionBuilder),
    expressionBuilder("ceiling", CeilExpressionBuilder, true),
    expression[Cos]("cos"),
    expression[Sec]("sec"),
    expression[Cosh]("cosh"),
    expression[Conv]("conv"),
    expression[ToDegrees]("degrees"),
    expression[EulerNumber]("e"),
    expression[Exp]("exp"),
    expression[Expm1]("expm1"),
    expressionBuilder("floor", FloorExpressionBuilder),
    expression[Factorial]("factorial"),
    expression[Hex]("hex"),
    expression[Hypot]("hypot"),
    expression[Logarithm]("log"),
    expression[Log10]("log10"),
    expression[Log1p]("log1p"),
    expression[Log2]("log2"),
    expression[Log]("ln"),
    expression[Remainder]("mod", true),
    expression[UnaryMinus]("negative", true),
    expression[Pi]("pi"),
    expression[Pmod]("pmod"),
    expression[UnaryPositive]("positive"),
    expression[Pow]("pow", true),
    expression[Pow]("power"),
    expression[ToRadians]("radians"),
    expression[Rint]("rint"),
    expression[Round]("round"),
    expression[ShiftLeft]("shiftleft"),
    expression[ShiftRight]("shiftright"),
    expression[ShiftRightUnsigned]("shiftrightunsigned"),
    expression[Signum]("sign", true),
    expression[Signum]("signum"),
    expression[Sin]("sin"),
    expression[Csc]("csc"),
    expression[Sinh]("sinh"),
    expression[StringToMap]("str_to_map"),
    expression[Sqrt]("sqrt"),
    expression[Tan]("tan"),
    expression[Cot]("cot"),
    expression[Tanh]("tanh"),
    expression[WidthBucket]("width_bucket"),

    expression[Add]("+"),
    expression[Subtract]("-"),
    expression[Multiply]("*"),
    expression[Divide]("/"),
    expression[IntegralDivide]("div"),
    expression[Remainder]("%"),

    // "try_*" 函数，总是返回 Null 而不是运行时错误。
    expression[TryAdd]("try_add"),
    expression[TryDivide]("try_divide"),
    expression[TrySubtract]("try_subtract"),
    expression[TryMultiply]("try_multiply"),
    expression[TryElementAt]("try_element_at"),
    expression[TryAverage]("try_avg"),
    expression[TrySum]("try_sum"),
    expression[TryToBinary]("try_to_binary"),

    // 聚合函数
    expression[HyperLogLogPlusPlus]("approx_count_distinct"),
    expression[Average]("avg"),
    expression[Corr]("corr"),
    expression[Count]("count"),
    expression[CountIf]("count_if"),
    expression[CovPopulation]("covar_pop"),
    expression[CovSample]("covar_samp"),
    expression[First]("first"),
    expression[First]("first_value", true),
    expression[Kurtosis]("kurtosis"),
    expression[Last]("last"),
    expression[Last]("last_value", true),
    expression[Max]("max"),
    expression[MaxBy]("max_by"),
    expression[Average]("mean", true),
    expression[Min]("min"),
    expression[MinBy]("min_by"),
    expression[Percentile]("percentile"),
    expression[Skewness]("skewness"),
    expression[ApproximatePercentile]("percentile_approx"),
    expression[ApproximatePercentile]("approx_percentile", true),
    expression[HistogramNumeric]("histogram_numeric"),
    expression[StddevSamp]("std", true),
    expression[StddevSamp]("stddev", true),
    expression[StddevPop]("stddev_pop"),
    expression[StddevSamp]("stddev_samp"),
    expression[Sum]("sum"),
    expression[VarianceSamp]("variance", true),
    expression[VariancePop]("var_pop"),
    expression[VarianceSamp]("var_samp"),
    expression[CollectList]("collect_list"),
    expression[CollectList]("array_agg", true),
    expression[CollectSet]("collect_set"),
    expression[CountMinSketchAgg]("count_min_sketch"),
    expression[BoolAnd]("every", true),
    expression[BoolAnd]("bool_and"),
    expression[BoolOr]("any", true),
    expression[BoolOr]("some", true),
    expression[BoolOr]("bool_or"),
    expression[RegrCount]("regr_count"),
    expression[RegrAvgX]("regr_avgx"),
    expression[RegrAvgY]("regr_avgy"),
    expression[RegrR2]("regr_r2"),

    // 字符串函数
    expression[Ascii]("ascii"),
    expression[Chr]("char", true),
    expression[Chr]("chr"),
    expressionBuilder("contains", ContainsExpressionBuilder),
    expressionBuilder("startswith", StartsWithExpressionBuilder),
    expressionBuilder("endswith", EndsWithExpressionBuilder),
    expression[Base64]("base64"),
    expression[BitLength]("bit_length"),
    expression[Length]("char_length", true),
    expression[Length]("character_length", true),
    expression[ConcatWs]("concat_ws"),
    expression[Decode]("decode"),
    expression[Elt]("elt"),
    expression[Encode]("encode"),
    expression[FindInSet]("find_in_set"),
    expression[FormatNumber]("format_number"),
    expression[FormatString]("format_string"),
    expression[ToNumber]("to_number"),
    expression[TryToNumber]("try_to_number"),
    expression[GetJsonObject]("get_json_object"),
    expression[InitCap]("initcap"),
    expression[StringInstr]("instr"),
    expression[Lower]("lcase", true),
    expression[Length]("length"),
    expression[Levenshtein]("levenshtein"),
    expression[Like]("like"),
    expression[ILike]("ilike"),
    expression[Lower]("lower"),
    expression[OctetLength]("octet_length"),
    expression[StringLocate]("locate"),
    expressionBuilder("lpad", LPadExpressionBuilder),
    expression[StringTrimLeft]("ltrim"),
    expression[JsonTuple]("json_tuple"),
    expression[ParseUrl]("parse_url"),
    expression[StringLocate]("position", true),
    expression[FormatString]("printf", true),
    expression[RegExpExtract]("regexp_extract"),
    expression[RegExpExtractAll]("regexp_extract_all"),
    expression[RegExpReplace]("regexp_replace"),
    expression[StringRepeat]("repeat"),
    expression[StringReplace]("replace"),
    expression[Overlay]("overlay"),
    expression[RLike]("rlike"),
    expression[RLike]("regexp_like", true, Some("3.2.0")),
    expression[RLike]("regexp", true, Some("3.2.0")),
    expressionBuilder("rpad", RPadExpressionBuilder),
    expression[StringTrimRight]("rtrim"),
    expression[Sentences]("sentences"),
    expression[SoundEx]("soundex"),
    expression[StringSpace]("space"),
    expression[StringSplit]("split"),
    expression[SplitPart]("split_part"),
    expression[Substring]("substr", true),
    expression[Substring]("substring"),
    expression[Left]("left"),
    expression[Right]("right"),
    expression[SubstringIndex]("substring_index"),
    expression[StringTranslate]("translate"),
    expression[StringTrim]("trim"),
    expression[StringTrimBoth]("btrim"),
    expression[Upper]("ucase", true),
    expression[UnBase64]("unbase64"),
    expression[Unhex]("unhex"),
    expression[Upper]("upper"),
    expression[XPathList]("xpath"),
    expression[XPathBoolean]("xpath_boolean"),
    expression[XPathDouble]("xpath_double"),
    expression[XPathDouble]("xpath_number", true),
    expression[XPathFloat]("xpath_float"),
    expression[XPathInt]("xpath_int"),
    expression[XPathLong]("xpath_long"),
    expression[XPathShort]("xpath_short"),
    expression[XPathString]("xpath_string"),

    // 日期时间函数
    expression[AddMonths]("add_months"),
    expression[CurrentDate]("current_date"),
    expression[CurrentTimestamp]("current_timestamp"),
    expression[CurrentTimeZone]("current_timezone"),
    expression[DateDiff]("datediff"),
    expression[DateAdd]("date_add"),
    expression[DateFormatClass]("date_format"),
    expression[DateSub]("date_sub"),
    expression[DayOfMonth]("day", true),
    expression[DayOfYear]("dayofyear"),
    expression[DayOfMonth]("dayofmonth"),
    expression[FromUnixTime]("from_unixtime"),
    expression[FromUTCTimestamp]("from_utc_timestamp"),
    expression[Hour]("hour"),
    expression[LastDay]("last_day"),
    expression[Minute]("minute"),
    expression[Month]("month"),
    expression[MonthsBetween]("months_between"),
    expression[NextDay]("next_day"),
    expression[Now]("now"),
    expression[Quarter]("quarter"),
    expression[Second]("second"),
    expression[ParseToTimestamp]("to_timestamp"),
    expression[ParseToDate]("to_date"),
    expression[ToBinary]("to_binary"),
    expression[ToUnixTimestamp]("to_unix_timestamp"),
    expression[ToUTCTimestamp]("to_utc_timestamp"),
    expression[TruncDate]("trunc"),
    expression[TruncTimestamp]("date_trunc"),
    expression[UnixTimestamp]("unix_timestamp"),
    expression[DayOfWeek]("dayofweek"),
    expression[WeekDay]("weekday"),
    expression[WeekOfYear]("weekofyear"),
    expression[Year]("year"),
    expression[TimeWindow]("window"),
    expression[SessionWindow]("session_window"),
    expression[MakeDate]("make_date"),
    expression[MakeTimestamp]("make_timestamp"),
    expression[MakeInterval]("make_interval"),
    expression[MakeDTInterval]("make_dt_interval"),
    expression[MakeYMInterval]("make_ym_interval"),
    expression[Extract]("extract"),
    // We keep the `DatePartExpressionBuilder` to have different function docs.
    expressionBuilder("date_part", DatePartExpressionBuilder, setAlias = true),
    expression[DateFromUnixDate]("date_from_unix_date"),
    expression[UnixDate]("unix_date"),
    expression[SecondsToTimestamp]("timestamp_seconds"),
    expression[MillisToTimestamp]("timestamp_millis"),
    expression[MicrosToTimestamp]("timestamp_micros"),
    expression[UnixSeconds]("unix_seconds"),
    expression[UnixMillis]("unix_millis"),
    expression[UnixMicros]("unix_micros"),

    // 集合函数
    expression[CreateArray]("array"),
    expression[ArrayContains]("array_contains"),
    expression[ArraysOverlap]("arrays_overlap"),
    expression[ArrayIntersect]("array_intersect"),
    expression[ArrayJoin]("array_join"),
    expression[ArrayPosition]("array_position"),
    expression[ArraySize]("array_size"),
    expression[ArraySort]("array_sort"),
    expression[ArrayExcept]("array_except"),
    expression[ArrayUnion]("array_union"),
    expression[CreateMap]("map"),
    expression[CreateNamedStruct]("named_struct"),
    expression[ElementAt]("element_at"),
    expression[MapContainsKey]("map_contains_key"),
    expression[MapFromArrays]("map_from_arrays"),
    expression[MapKeys]("map_keys"),
    expression[MapValues]("map_values"),
    expression[MapEntries]("map_entries"),
    expression[MapFromEntries]("map_from_entries"),
    expression[MapConcat]("map_concat"),
    expression[Size]("size"),
    expression[Slice]("slice"),
    expression[Size]("cardinality", true),
    expression[ArraysZip]("arrays_zip"),
    expression[SortArray]("sort_array"),
    expression[Shuffle]("shuffle"),
    expression[ArrayMin]("array_min"),
    expression[ArrayMax]("array_max"),
    expression[Reverse]("reverse"),
    expression[Concat]("concat"),
    expression[Flatten]("flatten"),
    expression[Sequence]("sequence"),
    expression[ArrayRepeat]("array_repeat"),
    expression[ArrayRemove]("array_remove"),
    expression[ArrayDistinct]("array_distinct"),
    expression[ArrayTransform]("transform"),
    expression[MapFilter]("map_filter"),
    expression[ArrayFilter]("filter"),
    expression[ArrayExists]("exists"),
    expression[ArrayForAll]("forall"),
    expression[ArrayAggregate]("aggregate"),
    expression[TransformValues]("transform_values"),
    expression[TransformKeys]("transform_keys"),
    expression[MapZipWith]("map_zip_with"),
    expression[ZipWith]("zip_with"),

    CreateStruct.registryEntry,

    // 杂项函数
    expression[AssertTrue]("assert_true"),
    expression[RaiseError]("raise_error"),
    expression[Crc32]("crc32"),
    expression[Md5]("md5"),
    expression[Uuid]("uuid"),
    expression[Murmur3Hash]("hash"),
    expression[XxHash64]("xxhash64"),
    expression[Sha1]("sha", true),
    expression[Sha1]("sha1"),
    expression[Sha2]("sha2"),
    expression[AesEncrypt]("aes_encrypt"),
    expression[AesDecrypt]("aes_decrypt"),
    expression[SparkPartitionID]("spark_partition_id"),
    expression[InputFileName]("input_file_name"),
    expression[InputFileBlockStart]("input_file_block_start"),
    expression[InputFileBlockLength]("input_file_block_length"),
    expression[MonotonicallyIncreasingID]("monotonically_increasing_id"),
    expression[CurrentDatabase]("current_database"),
    expression[CurrentCatalog]("current_catalog"),
    expression[CurrentUser]("current_user"),
    expression[CallMethodViaReflection]("reflect"),
    expression[CallMethodViaReflection]("java_method", true),
    expression[SparkVersion]("version"),
    expression[TypeOf]("typeof"),

    // 分组集
    expression[Grouping]("grouping"),
    expression[GroupingID]("grouping_id"),

    // 窗口函数
    expression[Lead]("lead"),
    expression[Lag]("lag"),
    expression[RowNumber]("row_number"),
    expression[CumeDist]("cume_dist"),
    expression[NthValue]("nth_value"),
    expression[NTile]("ntile"),
    expression[Rank]("rank"),
    expression[DenseRank]("dense_rank"),
    expression[PercentRank]("percent_rank"),

    // 谓词
    expression[And]("and"),
    expression[In]("in"),
    expression[Not]("not"),
    expression[Or]("or"),

    // 比较运算符
    expression[EqualNullSafe]("<=>"),
    expression[EqualTo]("="),
    expression[EqualTo]("=="),
    expression[GreaterThan](">"),
    expression[GreaterThanOrEqual](">="),
    expression[LessThan]("<"),
    expression[LessThanOrEqual]("<="),
    expression[Not]("!"),

    // 按位运算
    expression[BitwiseAnd]("&"),
    expression[BitwiseNot]("~"),
    expression[BitwiseOr]("|"),
    expression[BitwiseXor]("^"),
    expression[BitwiseCount]("bit_count"),
    expression[BitAndAgg]("bit_and"),
    expression[BitOrAgg]("bit_or"),
    expression[BitXorAgg]("bit_xor"),
    expression[BitwiseGet]("bit_get"),
    expression[BitwiseGet]("getbit", true),

    // json
    expression[StructsToJson]("to_json"),
    expression[JsonToStructs]("from_json"),
    expression[SchemaOfJson]("schema_of_json"),
    expression[LengthOfJsonArray]("json_array_length"),
    expression[JsonObjectKeys]("json_object_keys"),

    // 类型转换
    expression[Cast]("cast"),
    // 类型转换别名（SPARK-16730）
    castAlias("boolean", BooleanType),
    castAlias("tinyint", ByteType),
    castAlias("smallint", ShortType),
    castAlias("int", IntegerType),
    castAlias("bigint", LongType),
    castAlias("float", FloatType),
    castAlias("double", DoubleType),
    castAlias("decimal", DecimalType.USER_DEFAULT),
    castAlias("date", DateType),
    castAlias("timestamp", TimestampType),
    castAlias("binary", BinaryType),
    castAlias("string", StringType),

    // csv
    expression[CsvToStructs]("from_csv"),
    expression[SchemaOfCsv]("schema_of_csv"),
    expression[StructsToCsv]("to_csv")
  ) ++ expressionsForTimestampNTZSupport

  //////////////  内置的普通函数处理集合 //////////////
  val builtin: SimpleFunctionRegistry = {
    val fr = new SimpleFunctionRegistry
    expressions.foreach {
      case (name, (info, builder)) =>
        fr.internalRegisterFunction(FunctionIdentifier(name), info, builder)
    }
    fr
  }

  val functionSet: Set[FunctionIdentifier] = builtin.listFunction().toSet

  private def makeExprInfoForVirtualOperator(name: String, usage: String): ExpressionInfo = {
    new ExpressionInfo(
      null,
      null,
      name,
      usage,
      "",
      "",
      "",
      "",
      "",
      "",
      "built-in")
  }

  val builtinOperators: Map[String, ExpressionInfo] = Map(
    "<>" -> makeExprInfoForVirtualOperator("<>",
      "expr1 <> expr2 - Returns true if `expr1` is not equal to `expr2`."),
    "!=" -> makeExprInfoForVirtualOperator("!=",
      "expr1 != expr2 - Returns true if `expr1` is not equal to `expr2`."),
    "between" -> makeExprInfoForVirtualOperator("between",
      "expr1 [NOT] BETWEEN expr2 AND expr3 - " +
        "evaluate if `expr1` is [not] in between `expr2` and `expr3`."),
    "case" -> makeExprInfoForVirtualOperator("case",
      "CASE expr1 WHEN expr2 THEN expr3 [WHEN expr4 THEN expr5]* [ELSE expr6] END " +
        "- When `expr1` = `expr2`, returns `expr3`; when `expr1` = `expr4`, return `expr5`; " +
        "else return `expr6`."),
    "||" -> makeExprInfoForVirtualOperator("||",
      "expr1 || expr2 - Returns the concatenation of `expr1` and `expr2`.")
  )

  /**
   * 创建 SQL 函数构建器和相应的 ExpressionInfo。
   * @param name     函数名称。
   * @param setAlias SQL 表示字符串中使用的别名。
   * @param since    添加函数的 Spark 版本。
   * @tparam T       实际的表达式类。
   * @return (函数名称, (表达式信息, 函数构建器))
   */
  private def expression[T <: Expression : ClassTag](
      name: String,
      setAlias: Boolean = false,
      since: Option[String] = None): (String, (ExpressionInfo, FunctionBuilder)) = {

    // 基于反射，构建函数表达式构建信息 : (ExpressionInfo, FunctionBuilder)
    val (expressionInfo, builder) = FunctionRegistryBase.build[T](name, since)

    // FunctionBuilder 分装器， 增加 ExpressionTag
    val newBuilder = (expressions: Seq[Expression]) => {
      val expr = builder(expressions)
      if (setAlias) expr.setTagValue(FUNC_ALIAS, name)
      expr
    }
    // (函数名, (函数表达式信息， 函数构建方法))
    (name, (expressionInfo, newBuilder))
  }

  private def expressionBuilder[T <: ExpressionBuilder : ClassTag](
      name: String,
      builder: T,
      setAlias: Boolean = false): (String, (ExpressionInfo, FunctionBuilder)) = {

    val info = FunctionRegistryBase.expressionInfo[T](name, None)

    val funcBuilder = (expressions: Seq[Expression]) => {
      assert(expressions.forall(_.resolved), "function arguments must be resolved.")
      val expr = builder.build(name, expressions)
      if (setAlias) expr.setTagValue(FUNC_ALIAS, name)
      expr
    }

    (name, (info, funcBuilder))
  }

  /**
   * 为类型转换别名创建函数注册表查找条目（SPARK-16730）。
   * 例如，如果 name 是 "int"，dataType 是 IntegerType，这意味着 int(x) 将成为cast(x as IntegerType) 的别名。
   * 参见上面的用法。
   */
  private def castAlias(
      name: String,
      dataType: DataType): (String, (ExpressionInfo, FunctionBuilder)) = {
    val builder = (args: Seq[Expression]) => {
      if (args.size != 1) {
        throw QueryCompilationErrors.functionAcceptsOnlyOneArgumentError(name)
      }
      Cast(args.head, dataType)
    }
    val clazz = scala.reflect.classTag[Cast].runtimeClass
    val usage = "_FUNC_(expr) - Casts the value `expr` to the target data type `_FUNC_`."
    val expressionInfo =
      new ExpressionInfo(clazz.getCanonicalName, null, name, usage, "", "", "",
        "conversion_funcs", "2.0.1", "", "built-in")
    (name, (expressionInfo, builder))
  }

  private def expressionGeneratorOuter[T <: Generator : ClassTag](name: String)
    : (String, (ExpressionInfo, FunctionBuilder)) = {
    val (_, (info, generatorBuilder)) = expression[T](name)
    val outerBuilder = (args: Seq[Expression]) => {
      GeneratorOuter(generatorBuilder(args).asInstanceOf[Generator])
    }
    (name, (info, outerBuilder))
  }
}

/**
 * 用于查找表函数的目录。
 */
trait TableFunctionRegistry extends FunctionRegistryBase[LogicalPlan] {

  /** 创建此注册表的副本，其中包含与此注册表相同的函数。 */
  override def clone(): TableFunctionRegistry = throw new CloneNotSupportedException()
}

class SimpleTableFunctionRegistry extends SimpleFunctionRegistryBase[LogicalPlan]
    with TableFunctionRegistry {

  override def clone(): SimpleTableFunctionRegistry = synchronized {
    val registry = new SimpleTableFunctionRegistry
    functionBuilders.iterator.foreach { case (name, (info, builder)) =>
      registry.internalRegisterFunction(name, info, builder)
    }
    registry
  }
}

object EmptyTableFunctionRegistry extends EmptyFunctionRegistryBase[LogicalPlan]
    with TableFunctionRegistry {

  override def clone(): TableFunctionRegistry = this
}

object TableFunctionRegistry {

  type TableFunctionBuilder = Seq[Expression] => LogicalPlan

  private def logicalPlan[T <: LogicalPlan : ClassTag](name: String)
      : (String, (ExpressionInfo, TableFunctionBuilder)) = {
    val (info, builder) = FunctionRegistryBase.build[T](name, since = None)
    val newBuilder = (expressions: Seq[Expression]) => {
      try {
        builder(expressions)
      } catch {
        case e: AnalysisException =>
          val argTypes = expressions.map(_.dataType.typeName).mkString(", ")
          throw QueryCompilationErrors.cannotApplyTableValuedFunctionError(
            name, argTypes, info.getUsage, e.getMessage)
      }
    }
    (name, (info, newBuilder))
  }

  val logicalPlans: Map[String, (ExpressionInfo, TableFunctionBuilder)] = Map(
    logicalPlan[Range]("range")
  )

  ////////////// 内置的表函数处理集合 //////////////
  val builtin: SimpleTableFunctionRegistry = {
    val fr = new SimpleTableFunctionRegistry
    logicalPlans.foreach {
      case (name, (info, builder)) =>
        fr.internalRegisterFunction(FunctionIdentifier(name), info, builder)
    }
    fr
  }

  val functionSet: Set[FunctionIdentifier] = builtin.listFunction().toSet
}

trait ExpressionBuilder {
  def build(funcName: String, expressions: Seq[Expression]): Expression
}
