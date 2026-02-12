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

package org.apache.spark.sql

import java.lang.reflect.ParameterizedType

import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

import org.apache.spark.annotation.Stable
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.catalyst.{JavaTypeInference, ScalaReflection}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.aggregate.ScalaUDAF
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.expressions.{SparkUserDefinedFunction, UserDefinedAggregateFunction, UserDefinedAggregator, UserDefinedFunction}
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.Utils

/**
 * 用于注册用户自定义函数的功能类。通过 [[SparkSession.udf]] 访问：
 *
 * {{{ spark.udf }}}
 *
 * @since 1.3.0
 */
@Stable
class UDFRegistration private[sql] (functionRegistry: FunctionRegistry) extends Logging {

  import UDFRegistration._

  // 注册 Python 函数
  protected[sql] def registerPython(name: String, udf: UserDefinedPythonFunction): Unit = {
    log.debug(
      s"""
        | Registering new PythonUDF:
        | name: $name
        | command: ${udf.func.command.toSeq}
        | envVars: ${udf.func.envVars}
        | pythonIncludes: ${udf.func.pythonIncludes}
        | pythonExec: ${udf.func.pythonExec}
        | dataType: ${udf.dataType}
        | pythonEvalType: ${PythonEvalType.toString(udf.pythonEvalType)}
        | udfDeterministic: ${udf.udfDeterministic}
      """.stripMargin)

    functionRegistry.createOrReplaceTempFunction(name, udf.builder, "python_udf")
  }

  /**
   * 注册用户自定义聚合函数（UDAF）。
   *
   * @param name UDAF 的名称。
   * @param udaf 需要注册的 UDAF。
   * @return 已注册的 UDAF。
   *
   * @since 1.5.0
   * @deprecated 此方法和 UserDefinedAggregateFunction 的使用已被弃用。
   * 现在应该通过 functions.udaf(agg) 方法将 Aggregator[IN, BUF, OUT] 注册为 UDF。
   */
  @deprecated("Aggregator[IN, BUF, OUT] should now be registered as a UDF" +
    " via the functions.udaf(agg) method.", "3.0.0")
  def register(name: String, udaf: UserDefinedAggregateFunction): UserDefinedAggregateFunction = {
    def builder(children: Seq[Expression]) = ScalaUDAF(children, udaf, udafName = Some(name))
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    udaf
  }

  /**
   * 注册用户自定义函数（UDF/UDAF），用于已经使用 Dataset API 定义的 UDF（即 UserDefinedFunction 类型）。
   * 要将 UDF 改为非确定性的，调用 `UserDefinedFunction.asNondeterministic()` API。
   * 要将 UDF 改为非空的，调用 `UserDefinedFunction.asNonNullable()` API。
   *
   * 示例：
   * {{{
   *   val foo = udf(() => Math.random())
   *   spark.udf.register("random", foo.asNondeterministic())
   *
   *   val bar = udf(() => "bar")
   *   spark.udf.register("stringLit", bar.asNonNullable())
   * }}}
   *
   * @param name UDF 的名称。
   * @param udf 需要注册的 UDF。
   * @return 已注册的 UDF。
   *
   * @since 2.2.0
   */
  def register(name: String, udf: UserDefinedFunction): UserDefinedFunction = {
    udf.withName(name) match {
      case udaf: UserDefinedAggregator[_, _, _] =>
        // FunctionBuiler 封装 ScalaAggregator 构造过程
        def builder(children: Seq[Expression]) = udaf.scalaAggregator(children)
        functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
        udaf
      case other =>
        // FunctionBuiler 处理 UDF  : UserDefinedFunction.apply 方法
        def builder(children: Seq[Expression]) = other.apply(children.map(Column.apply) : _*).expr
        functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
        other
    }
  }

  /**
   * 将 0 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   * @tparam RT UDF 的返回类型。
   * @since 1.3.0
   */
  def register[RT: TypeTag](name: String, func: Function0[RT]): UserDefinedFunction = {

    // 输出的列编码器
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption   // 返回类型 RT，需要 TypeTag 保留运行时类型信息

    // 输出对象的序列化类型， 用于将返回值序列化成 InternalRow
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])

    // 输入字段的列编码器
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Nil

    // 定义 UDF 的包装器
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)

    val finalUdf = if (nullable) udf else udf.asNonNullable()

    def builder(e: Seq[Expression]) = if (e.length == 0) {
      finalUdf.createScalaUDF(e)   // SparkUserDefinedFunction -> ScalaUDF
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "0", e.length)
    }

    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  /**
   * 将 1 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   * @tparam RT UDF 的返回类型。
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag](name: String, func: Function1[A1, RT]): UserDefinedFunction = {
    // 返回类型的 ExpressionEncoder
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    // 输入类型的 ExpressionEncoder
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 1) {
      finalUdf.createScalaUDF(e)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "1", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  /**
   * 将 2 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   * @tparam RT UDF 的返回类型。
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag](name: String, func: Function2[A1, A2, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 2) {
      finalUdf.createScalaUDF(e)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "2", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  /**
   * 将 3 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   * @tparam RT UDF 的返回类型。
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag](name: String, func: Function3[A1, A2, A3, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 3) {
      finalUdf.createScalaUDF(e)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "3", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  /**
   * 将 4 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag](name: String, func: Function4[A1, A2, A3, A4, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 4) {
      finalUdf.createScalaUDF(e)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "4", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  /**
   * 将 5 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag](name: String, func: Function5[A1, A2, A3, A4, A5, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 5) {
      finalUdf.createScalaUDF(e)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "5", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  /**
   * 将 6 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag](name: String, func: Function6[A1, A2, A3, A4, A5, A6, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 6) {
      finalUdf.createScalaUDF(e)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "6", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  /**
   * 将 7 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag](name: String, func: Function7[A1, A2, A3, A4, A5, A6, A7, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 7) {
      finalUdf.createScalaUDF(e)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "7", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  /**
   * 将 8 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag](name: String, func: Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 8) {
      finalUdf.createScalaUDF(e)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "8", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  /**
   * 将 9 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag](name: String, func: Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 9) {
      finalUdf.createScalaUDF(e)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "9", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  /**
   * 将 10 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag](name: String, func: Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 10) {
      finalUdf.createScalaUDF(e)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "10", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  /**
   * 将11 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag](name: String, func: Function11[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 11) {
      finalUdf.createScalaUDF(e)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "11", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  /**
   * 将 12 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag](name: String, func: Function12[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Try(ExpressionEncoder[A12]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 12) {
      finalUdf.createScalaUDF(e)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "12", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  /**
   * 将 13 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag](name: String, func: Function13[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Try(ExpressionEncoder[A12]()).toOption :: Try(ExpressionEncoder[A13]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 13) {
      finalUdf.createScalaUDF(e)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "13", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  /**
   * 将 14 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag](name: String, func: Function14[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Try(ExpressionEncoder[A12]()).toOption :: Try(ExpressionEncoder[A13]()).toOption :: Try(ExpressionEncoder[A14]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 14) {
      finalUdf.createScalaUDF(e)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "14", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  /**
   * 将 15 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag](name: String, func: Function15[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Try(ExpressionEncoder[A12]()).toOption :: Try(ExpressionEncoder[A13]()).toOption :: Try(ExpressionEncoder[A14]()).toOption :: Try(ExpressionEncoder[A15]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 15) {
      finalUdf.createScalaUDF(e)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "15", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  /**
   * 将 16 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag](name: String, func: Function16[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Try(ExpressionEncoder[A12]()).toOption :: Try(ExpressionEncoder[A13]()).toOption :: Try(ExpressionEncoder[A14]()).toOption :: Try(ExpressionEncoder[A15]()).toOption :: Try(ExpressionEncoder[A16]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 16) {
      finalUdf.createScalaUDF(e)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "16", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  /**
   * 将 17 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag](name: String, func: Function17[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Try(ExpressionEncoder[A12]()).toOption :: Try(ExpressionEncoder[A13]()).toOption :: Try(ExpressionEncoder[A14]()).toOption :: Try(ExpressionEncoder[A15]()).toOption :: Try(ExpressionEncoder[A16]()).toOption :: Try(ExpressionEncoder[A17]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 17) {
      finalUdf.createScalaUDF(e)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "17", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  /**
   * 将 18 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag](name: String, func: Function18[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Try(ExpressionEncoder[A12]()).toOption :: Try(ExpressionEncoder[A13]()).toOption :: Try(ExpressionEncoder[A14]()).toOption :: Try(ExpressionEncoder[A15]()).toOption :: Try(ExpressionEncoder[A16]()).toOption :: Try(ExpressionEncoder[A17]()).toOption :: Try(ExpressionEncoder[A18]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 18) {
      finalUdf.createScalaUDF(e)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "18", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  /**
   * 将 19 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag](name: String, func: Function19[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Try(ExpressionEncoder[A12]()).toOption :: Try(ExpressionEncoder[A13]()).toOption :: Try(ExpressionEncoder[A14]()).toOption :: Try(ExpressionEncoder[A15]()).toOption :: Try(ExpressionEncoder[A16]()).toOption :: Try(ExpressionEncoder[A17]()).toOption :: Try(ExpressionEncoder[A18]()).toOption :: Try(ExpressionEncoder[A19]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 19) {
      finalUdf.createScalaUDF(e)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "19", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  /**
   * 将 20 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag, A20: TypeTag](name: String, func: Function20[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Try(ExpressionEncoder[A12]()).toOption :: Try(ExpressionEncoder[A13]()).toOption :: Try(ExpressionEncoder[A14]()).toOption :: Try(ExpressionEncoder[A15]()).toOption :: Try(ExpressionEncoder[A16]()).toOption :: Try(ExpressionEncoder[A17]()).toOption :: Try(ExpressionEncoder[A18]()).toOption :: Try(ExpressionEncoder[A19]()).toOption :: Try(ExpressionEncoder[A20]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 20) {
      finalUdf.createScalaUDF(e)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "20", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  /**
   * 将 21 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag, A20: TypeTag, A21: TypeTag](name: String, func: Function21[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Try(ExpressionEncoder[A12]()).toOption :: Try(ExpressionEncoder[A13]()).toOption :: Try(ExpressionEncoder[A14]()).toOption :: Try(ExpressionEncoder[A15]()).toOption :: Try(ExpressionEncoder[A16]()).toOption :: Try(ExpressionEncoder[A17]()).toOption :: Try(ExpressionEncoder[A18]()).toOption :: Try(ExpressionEncoder[A19]()).toOption :: Try(ExpressionEncoder[A20]()).toOption :: Try(ExpressionEncoder[A21]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 21) {
      finalUdf.createScalaUDF(e)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "21", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  /**
   * 将 22 个参数的确定性 Scala 闭包注册为用户自定义函数（UDF）。
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag, A20: TypeTag, A21: TypeTag, A22: TypeTag](name: String, func: Function22[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, A22, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Try(ExpressionEncoder[A12]()).toOption :: Try(ExpressionEncoder[A13]()).toOption :: Try(ExpressionEncoder[A14]()).toOption :: Try(ExpressionEncoder[A15]()).toOption :: Try(ExpressionEncoder[A16]()).toOption :: Try(ExpressionEncoder[A17]()).toOption :: Try(ExpressionEncoder[A18]()).toOption :: Try(ExpressionEncoder[A19]()).toOption :: Try(ExpressionEncoder[A20]()).toOption :: Try(ExpressionEncoder[A21]()).toOption :: Try(ExpressionEncoder[A22]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 22) {
      finalUdf.createScalaUDF(e)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "22", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    finalUdf
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * 使用反射注册 Java UDF 类，供 pyspark 使用
   *
   * @param name            UDF 名称
   * @param className       UDF 的完全限定类名
   * @param returnDataType  UDF 的返回类型。如果为 null，Spark 会尝试推断
   *                        通过反射。
   */
  private[sql] def registerJava(name: String, className: String, returnDataType: DataType): Unit = {

    try {
      val clazz = Utils.classForName[AnyRef](className)
      val udfInterfaces = clazz.getGenericInterfaces
        .filter(_.isInstanceOf[ParameterizedType])
        .map(_.asInstanceOf[ParameterizedType])
        .filter(e => e.getRawType.isInstanceOf[Class[_]] && e.getRawType.asInstanceOf[Class[_]].getCanonicalName.startsWith("org.apache.spark.sql.api.java.UDF"))
      if (udfInterfaces.length == 0) {
        throw QueryCompilationErrors.udfClassDoesNotImplementAnyUDFInterfaceError(className)
      } else if (udfInterfaces.length > 1) {
        throw QueryCompilationErrors.udfClassNotAllowedToImplementMultiUDFInterfacesError(className)
      } else {
        try {
          val udf = clazz.getConstructor().newInstance()
          val udfReturnType = udfInterfaces(0).getActualTypeArguments.last
          var returnType = returnDataType
          if (returnType == null) {
            returnType = JavaTypeInference.inferDataType(udfReturnType)._1
          }

          udfInterfaces(0).getActualTypeArguments.length match {
            case 1 => register(name, udf.asInstanceOf[UDF0[_]], returnType)
            case 2 => register(name, udf.asInstanceOf[UDF1[_, _]], returnType)
            case 3 => register(name, udf.asInstanceOf[UDF2[_, _, _]], returnType)
            case 4 => register(name, udf.asInstanceOf[UDF3[_, _, _, _]], returnType)
            case 5 => register(name, udf.asInstanceOf[UDF4[_, _, _, _, _]], returnType)
            case 6 => register(name, udf.asInstanceOf[UDF5[_, _, _, _, _, _]], returnType)
            case 7 => register(name, udf.asInstanceOf[UDF6[_, _, _, _, _, _, _]], returnType)
            case 8 => register(name, udf.asInstanceOf[UDF7[_, _, _, _, _, _, _, _]], returnType)
            case 9 => register(name, udf.asInstanceOf[UDF8[_, _, _, _, _, _, _, _, _]], returnType)
            case 10 => register(name, udf.asInstanceOf[UDF9[_, _, _, _, _, _, _, _, _, _]], returnType)
            case 11 => register(name, udf.asInstanceOf[UDF10[_, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 12 => register(name, udf.asInstanceOf[UDF11[_, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 13 => register(name, udf.asInstanceOf[UDF12[_, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 14 => register(name, udf.asInstanceOf[UDF13[_, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 15 => register(name, udf.asInstanceOf[UDF14[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 16 => register(name, udf.asInstanceOf[UDF15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 17 => register(name, udf.asInstanceOf[UDF16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 18 => register(name, udf.asInstanceOf[UDF17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 19 => register(name, udf.asInstanceOf[UDF18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 20 => register(name, udf.asInstanceOf[UDF19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 21 => register(name, udf.asInstanceOf[UDF20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 22 => register(name, udf.asInstanceOf[UDF21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 23 => register(name, udf.asInstanceOf[UDF22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case n =>
              throw QueryCompilationErrors.udfClassWithTooManyTypeArgumentsError(n)
          }
        } catch {
          case e @ (_: InstantiationException | _: IllegalArgumentException) =>
            throw QueryCompilationErrors.classWithoutPublicNonArgumentConstructorError(className)
        }
      }
    } catch {
      case e: ClassNotFoundException => throw QueryCompilationErrors.cannotLoadClassNotOnClassPathError(className)
    }

  }

  /**
   * 使用反射注册 Java UDAF 类，供 pyspark 使用
   *
   * @param name     UDAF 名称
   * @param className    UDAF 的完全限定类名
   */
  private[sql] def registerJavaUDAF(name: String, className: String): Unit = {
    try {
      val clazz = Utils.classForName[AnyRef](className)
      if (!classOf[UserDefinedAggregateFunction].isAssignableFrom(clazz)) {
        throw QueryCompilationErrors.classDoesNotImplementUserDefinedAggregateFunctionError(className)
      }
      val udaf = clazz.getConstructor().newInstance().asInstanceOf[UserDefinedAggregateFunction]
      register(name, udaf)
    } catch {
      case e: ClassNotFoundException => throw QueryCompilationErrors.cannotLoadClassNotOnClassPathError(className)
      case e @ (_: InstantiationException | _: IllegalArgumentException) =>
        throw QueryCompilationErrors.classWithoutPublicNonArgumentConstructorError(className)
    }
  }

  /**
   * 将确定性的 Java UDF0 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF0[_], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = () => f.asInstanceOf[UDF0[Any]].call()
    def builder(e: Seq[Expression]) = if (e.length == 0) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "0", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  /**
   * 将确定性的 Java UDF1 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF1[_, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF1[Any, Any]].call(_: Any)
    def builder(e: Seq[Expression]) = if (e.length == 1) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "1", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  /**
   * 将确定性的 Java UDF2 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF2[_, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF2[Any, Any, Any]].call(_: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 2) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "2", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  /**
   * 将确定性的 Java UDF3 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF3[_, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF3[Any, Any, Any, Any]].call(_: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 3) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "3", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  /**
   * 将确定性的 Java UDF4 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF4[_, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF4[Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 4) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "4", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  /**
   * 将确定性的 Java UDF5 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF5[_, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF5[Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 5) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "5", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  /**
   * 将确定性的 Java UDF6 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF6[_, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF6[Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 6) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "6", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  /**
   * 将确定性的 Java UDF7 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF7[_, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF7[Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 7) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "7", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  /**
   * 将确定性的 Java UDF8 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF8[_, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF8[Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 8) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "8", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  /**
   * 将确定性的 Java UDF9 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF9[_, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF9[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 9) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "9", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  /**
   * 将确定性的 Java UDF10 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF10[_, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 10) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "10", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  /**
   * 将确定性的 Java UDF11 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF11[_, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF11[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 11) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "11", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  /**
   * 将确定性的 Java UDF12 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF12[_, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF12[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 12) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "12", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  /**
   * 将确定性的 Java UDF13 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF13[_, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF13[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 13) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "13", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  /**
   * 将确定性的 Java UDF14 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF14[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF14[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 14) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "14", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  /**
   * 将确定性的 Java UDF15 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF15[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 15) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "15", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  /**
   * 将确定性的 Java UDF16 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF16[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 16) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "16", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  /**
   * 将确定性的 Java UDF17 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF17[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 17) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "17", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  /**
   * 将确定性的 Java UDF18 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF18[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 18) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "18", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  /**
   * 将确定性的 Java UDF19 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF19[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 19) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "19", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  /**
   * 将确定性的 Java UDF20 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF20[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 20) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "20", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  /**
   * 将确定性的 Java UDF21 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF21[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 21) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "21", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  /**
   * 将确定性的 Java UDF22 实例注册为用户自定义函数（UDF）。
   * @since 2.3.0
   */
  def register(name: String, f: UDF22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF22[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 22) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentsError(name, "22", e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, "java_udf")
  }

  // scalastyle:on line.size.limit

}

private[sql] object UDFRegistration {
  /**
   * 获取 `ScalaUDF` 的输出编码器的 schema。
   *
   * 由于 `ScalaUDF` 中的序列化是针对单个列而不是整行，我们只取原始对象序列化器的数据类型，而不是为顶层行转换过的 `serializer`。
   */
  def outputSchema(outputEncoder: ExpressionEncoder[_]): ScalaReflection.Schema = {
    ScalaReflection.Schema(outputEncoder.objSerializer.dataType,
      outputEncoder.objSerializer.nullable)
  }
}
