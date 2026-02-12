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

import java.util.Locale

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.{BinaryLike, LeafLike, QuaternaryLike, TernaryLike, TreeNode, UnaryLike}
import org.apache.spark.sql.catalyst.trees.TreePattern.{RUNTIME_REPLACEABLE, TreePattern}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines the basic expression abstract classes in Catalyst.
////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 *
 * Catalyst 中的表达式。
 *
 * TreeNode[Expression]
 *  └── [[Expression]] (抽象基类)
 *     ├── [[LeafExpression]] (叶子表达式)
 *     │   ├── [[Literal]] (字面量)
 *     │   ├── [[Attribute]] (属性引用)
 *     │   └── [[CurrentRow]] (当前行)
 *     ├── [[UnaryExpression]] (一元表达式)
 *     │   ├── [[UnaryMinus]] (负号)
 *     │   ├── [[Cast]] (类型转换)
 *     │   ├── [[IsNull]] (空值判断)
 *     │   └── [[Not]] (逻辑非)
 *     ├── [[BinaryExpression]] (二元表达式)
 *     │   ├── [[BinaryArithmetic]] (算术运算)
 *     │   │   ├── [[Add]] (加法)
 *     │   │   ├── [[Subtract]] (减法)
 *     │   │   ├── [[Multiply]] (乘法)
 *     │   │   └── [[Divide]] (除法)
 *     │   ├── [[BinaryComparison]] (比较运算)
 *     │   │   ├── [[EqualTo]] (等于)
 *     │   │   ├── [[LessThan]] (小于)
 *     │   │   └── [[GreaterThan]] (大于)
 *     │   └── [[BinaryOperator]] (逻辑运算)
 *     │       ├── [[And]] (逻辑与)
 *     │       └── [[Or]] (逻辑或)
 *     ├── [[TernaryExpression]] (三元表达式)
 *     │   └── [[If]] (条件表达式)
 *     ├── [[ComplexExpression]] (复杂表达式)
 *     │    ├── [[CaseWhen]] (Case表达式)
 *     │    ├── [[Coalesce]] (合并表达式)
 *     │    └── [[In]] (包含表达式)
 *     ├── [[Attribute]] (trait) - 属性抽象
 *     │    ├── [[AttributeReference]] - 具体的列引用 : 不能直接求值，需要绑定到具体的行, 有唯一的表达式 ID
 *     │    └── [[PrettyAttribute]] - 美化显示的属性
 *     ├── [[Alias]] - 表达式别名
 *     ├── [[ExpectsInputTypes]]  - 定义期望的输入类型 : 用于类型检查和强制转换
 *     │    ├── [[ImplicitCastInputTypes]] - 支持隐式类型转换 : 自动插入 Cast 表达式
 *     ├── [[NullIntolerant]]       任何 null 输入都产生 null 输出 : 用于优化 null 值处理
 *     ├── [[Unevaluable]]          (无法求值表达式， 在分析/优化后不存在)
 *     ├── [[RuntimeReplaceable]]   (运行时被替换的表达式，用于兼容其他数据库（如 nvl 替换为 coalesce）)
 *     ├── [[Nondeterministic]]     (非确定性表达式（如 rand()），需要初始化分区索引)
 *     ├── [[Stateful]]             (包含可变状态的表达式，每行只能求值一次)
 *     ├── [[ConditionalExpression]] (条件分支表达式（如 CASE WHEN），不是所有分支都会执行)
 *
 *
 * 如果一个表达式希望在函数注册表中暴露（以便用户可以使用 "name(arguments...)" 调用它），
 * 具体的实现必须是一个 case 类，其构造函数参数都是表达式类型。例如，参见 [[Substring]]。
 *
 *
 * 表达式求值机制：
 * #### **解释执行（Interpreted Execution）**
 *
 * override def eval(input: InternalRow): Any = {
 *   val value = child.eval(input)
 *   if (value == null) null
 *   else nullSafeEval(value)
 * }
 *
 * #### **代码生成（Code Generation）**
 * 通过 genCode() 生成 Java 代码，避免虚函数调用开销：
 *
 * def genCode(ctx: CodegenContext): ExprCode = {
 *  // 生成 isNull 和 value 变量
 *  // 调用 doGenCode 生成具体逻辑
 *  // 支持子表达式消除优化
 * }
 */
abstract class Expression extends TreeNode[Expression] {

  ///////////////////// 基础属性 /////////////////////

  def dataType: DataType                                               /// 表达式的返回数据类型
  def nullable: Boolean                                                /// 是否可能返回 null
  def foldable: Boolean = false                                        /// 是否可在编译时常量折叠优化
  lazy val deterministic: Boolean = children.forall(_.deterministic)   /// 是否确定性（相同输入产生相同输出）
  def references: AttributeSet = _references                           /// 表达式引用的列属性集合

  ///////////////////// 求值方法 /////////////////////

  def eval(input: InternalRow = null): Any                             ///  解释执行，在给定行上求值
  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode ///  具体的代码生成实现
  def genCode(ctx: CodegenContext): ExprCode = {                        /// 生成Java代码用于代码生成优化
    ctx.subExprEliminationExprs.get(ExpressionEquals(this)).map { subExprState =>
      // This expression is repeated which means that the code to evaluate it has already been added
      // as a function before. In that case, we just re-use it.
      ExprCode(
        ctx.registerComment(this.toString),
        subExprState.eval.isNull,
        subExprState.eval.value)
    }.getOrElse {
      val isNull = ctx.freshName("isNull")
      val value = ctx.freshName("value")
      val eval = doGenCode(ctx, ExprCode(
        JavaCode.isNullVariable(isNull),
        JavaCode.variable(value, dataType)))
      reduceCodeSize(ctx, eval)
      if (eval.code.toString.nonEmpty) {
        // Add `this` in the comment.
        eval.copy(code = ctx.registerComment(this.toString) + eval.code)
      } else {
        eval
      }
    }
  }

  ///////////////////// 解析和类型检查 /////////////////////

  lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess   /// 表达式是否已解析（子表达式已解析且类型检查通过）
  def childrenResolved: Boolean = children.forall(_.resolved)                        /// 判断所有子表达式是否已解析
  def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.TypeCheckSuccess      /// 检查输入类型是否有效

  ///////////////////// 语义比较 /////////////////////

  lazy val canonicalized: Expression = {                                             /// 规范化表达式，用于语义比较
    val canonicalizedChildren = children.map(_.canonicalized)
    withNewChildren(canonicalizedChildren)
  }

  final def semanticEquals(other: Expression): Boolean =                             //// 判断两个表达式是否语义相等
    deterministic && other.deterministic && canonicalized == other.canonicalized

  def semanticHash(): Int = canonicalized.hashCode()                                 ////  返回语义哈希值

  ///////////////////// 其他辅助方法 /////////////////////

  def prettyName: String = getTagValue(FunctionRegistry.FUNC_ALIAS)                    // 返回用户友好的表达式名称
    .getOrElse(nodeName.toLowerCase(Locale.ROOT))

  override def toString: String = prettyName + truncatedString(                        // 返回表达式的字符串表示
    flatArguments.toSeq, "(", ", ", ")", SQLConf.get.maxToStringFields)

  def sql: String = {                                                                    // 返回表达式的SQL表示
    val childrenSQL = children.map(_.sql).mkString(", ")
    s"$prettyName($childrenSQL)"
  }

  @transient
  private lazy val _references: AttributeSet = AttributeSet.fromAttributeSets(children.map(_.references))

  private def reduceCodeSize(ctx: CodegenContext, eval: ExprCode): Unit = {
    // TODO: support whole stage codegen too
    val splitThreshold = SQLConf.get.methodSplitThreshold
    if (eval.code.length > splitThreshold && ctx.INPUT_ROW != null && ctx.currentVars == null) {
      val setIsNull = if (!eval.isNull.isInstanceOf[LiteralValue]) {
        val globalIsNull = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "globalIsNull")
        val localIsNull = eval.isNull
        eval.isNull = JavaCode.isNullGlobal(globalIsNull)
        s"$globalIsNull = $localIsNull;"
      } else {
        ""
      }

      val javaType = CodeGenerator.javaType(dataType)
      val newValue = ctx.freshName("value")

      val funcName = ctx.freshName(nodeName)
      val funcFullName = ctx.addNewFunction(funcName,
        s"""
           |private $javaType $funcName(InternalRow ${ctx.INPUT_ROW}) {
           |  ${eval.code}
           |  $setIsNull
           |  return ${eval.value};
           |}
           """.stripMargin)

      eval.value = JavaCode.variable(newValue, dataType)
      eval.code = code"$javaType $newValue = $funcFullName(${ctx.INPUT_ROW});"
    }
  }

  /**
   * 返回此表达式名称的用户可见字符串表示形式。
   * 这通常应与 SQL 中函数的名称匹配。
   */
  protected def flatArguments: Iterator[Any] = stringArgs.flatMap {
    case t: Iterable[_] => t
    case single => single :: Nil
  }

  // 将此标记为 final，Expression.verboseString 不应该被调用，
  // 因此不应该被具体类覆盖。
  final override def verboseString(maxFields: Int): String = simpleString(maxFields)

  override def simpleString(maxFields: Int): String = toString

  override def simpleStringWithNodeId(): String = {
    throw new IllegalStateException(s"$nodeName does not implement simpleStringWithNodeId")
  }

  protected def typeSuffix =
    if (resolved) {
      dataType match {
        case LongType => "L"
        case _ => ""
      }
    } else {
      ""
    }
}


/**
 * 无法求值的表达式。 这些表达式不会在分析或优化阶段之后存在（例如 Star），不应在查询规划和执行期间进行评估。
 */
trait Unevaluable extends Expression {

  /** Unevaluable is not foldable because we don't have an eval for it. */
  final override def foldable: Boolean = false

  final override def eval(input: InternalRow = null): Any =
    throw QueryExecutionErrors.cannotEvaluateExpressionError(this)

  final override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)
}

/**
 *  在运行时（目前由优化器）被替换为不同表达式进行求值的表达式。 这主要用于提供与其他数据库的兼容性。
 *  例如，我们使用它通过将 "nvl" 替换为 "coalesce" 来支持 nvl 函数。
 */
trait RuntimeReplaceable extends Expression {
  def replacement: Expression

  override val nodePatterns: Seq[TreePattern] = Seq(RUNTIME_REPLACEABLE)
  override def nullable: Boolean = replacement.nullable
  override def dataType: DataType = replacement.dataType

  // 由于此表达式在优化时被其 `child` 表达式替换，如果两个 `RuntimeReplaceable` 的 "child" 表达式在语义上相等，则认为它们在语义上相等。
  override lazy val canonicalized: Expression = replacement.canonicalized

  final override def eval(input: InternalRow = null): Any =
    throw QueryExecutionErrors.cannotEvaluateExpressionError(this)
  final override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)
}

/**
 * [[RuntimeReplaceable]] 的附加特性。
 * 它使 `replacement` 成为表达式的子节点，以继承其分析规则，例如类型强制转换。
 * 实现应该将 `replacement` 放在 case 类构造函数中， 并定义一个只接受原始参数的普通构造函数。例如，参见 [[TryAdd]]。
 * 为了确保 explain 计划和表达式 SQL 正确工作，实现还应该实现 `parameters` 方法。
 */
trait InheritAnalysisRules extends UnaryLike[Expression] { self: RuntimeReplaceable =>
  override def child: Expression = replacement
  def parameters: Seq[Expression]
  override def flatArguments: Iterator[Any] = parameters.iterator
  def makeSQLString(childrenSQL: Seq[String]): String = {
    prettyName + childrenSQL.mkString("(", ", ", ")")
  }
  final override def sql: String = makeSQLString(parameters.map(_.sql))
}

/**
 * [[AggregateFunction]] 的附加特性。它被重写（目前由优化器）为不同的聚合表达式进行求值。
 * 这主要用于提供与其他数据库的兼容性。
 * 例如，我们使用它通过将 every, any/some 聚合,分别重写为 Min 和 Max 来支持它们。
 */
trait RuntimeReplaceableAggregate extends RuntimeReplaceable { self: AggregateFunction =>
  override def aggBufferSchema: StructType = throw new IllegalStateException(
    "RuntimeReplaceableAggregate.aggBufferSchema should not be called")
  override def aggBufferAttributes: Seq[AttributeReference] = throw new IllegalStateException(
    "RuntimeReplaceableAggregate.aggBufferAttributes should not be called")
  override def inputAggBufferAttributes: Seq[AttributeReference] = throw new IllegalStateException(
    "RuntimeReplaceableAggregate.inputAggBufferAttributes should not be called")
}

/**
 * 没有 SQL 表示的表达式应该扩展此特性。例如 `ScalaUDF`、`ScalaUDAF`
 * 以及像 `MapObjects` 和 `Invoke` 这样的对象表达式。
 */
trait NonSQLExpression extends Expression {
  final override def sql: String = {
    transform {
      case a: Attribute => new PrettyAttribute(a)
      case a: Alias => PrettyAttribute(a.sql, a.dataType)
      case p: PythonUDF => PrettyPythonUDF(p.name, p.dataType, p.children)
    }.toString
  }
}


/**  非确定性表达式。 */
trait Nondeterministic extends Expression {
  final override lazy val deterministic: Boolean = false
  final override def foldable: Boolean = false

  @transient
  private[this] var initialized = false

  /** 根据当前分区索引初始化内部状态，并将其标记为已初始化。子类应该重写 [[initializeInternal()]]。 */
  final def initialize(partitionIndex: Int): Unit = {
    initializeInternal(partitionIndex)
    initialized = true
  }

  protected def initializeInternal(partitionIndex: Int): Unit

  /**
   * @inheritdoc
   * 如果尚未调用 [[initialize()]]，则抛出异常。 子类应该重写 [[evalInternal()]]。
   */
  final override def eval(input: InternalRow = null): Any = {
    require(initialized, s"Nondeterministic expression ${this.getClass.getName} should be initialized before eval.")
    evalInternal(input)
  }

  protected def evalInternal(input: InternalRow): Any
}

/**
 * 包含条件表达式分支的表达式，因此不是所有分支都会被执行。 所有优化都应该注意求值顺序。
 */
trait ConditionalExpression extends Expression {
  final override def foldable: Boolean = children.forall(_.foldable)

  /** 返回在运行时总是会被执行的子表达式。 */
  def alwaysEvaluatedInputs: Seq[Expression]

  /** 返回分支组。对于每个组，至少有一个分支会在运行时被执行， 这样我们就可以急切地求值组的公共表达式。*/
  def branchGroups: Seq[Seq[Expression]]
}

/**
 * 包含可变状态的表达式。
 * 有状态表达式总是非确定性的， 因为它在求值期间产生的结果不仅取决于给定的输入，还取决于其内部状态。
 *
 * 表达式的状态通常不会在参数列表中暴露，这使得比较有状态表达式变得有问题， 因为相似的有状态表达式（具有相同的参数列表）但具有不同的内部状态将被认为是相等的。
 * 这在树转换期间尤其成问题。
 * 为了解决这个问题，有状态表达式的 `fastEquals` 方法只对相同的引用返回 `true`。
 *
 * 有状态表达式不应该对单行多次求值。这应该只是解释执行的问题。
 * 这可以通过在执行前创建有状态表达式的新副本来防止，这些副本可以使用 `freshCopy` 函数创建。
 */
trait Stateful extends Nondeterministic {
  /**
   * 返回有状态表达式的新的未初始化副本。.
   */
  def freshCopy(): Stateful

  /**
   * 只有相同的引用才被认为是相等的.
   */
  override def fastEquals(other: TreeNode[_]): Boolean = this eq other
}

/**  叶子表达式，即没有任何子表达式的表达式。*/
abstract class LeafExpression extends Expression with LeafLike[Expression]


/** 具有一个输入和一个输出的表达式。如果输入求值为 null，则默认情况下输出也求值为 null */
abstract class UnaryExpression extends Expression with UnaryLike[Expression] {

  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable

  /**
   * 根据 UnaryExpression 的默认可空性的默认求值行为。
   * 如果 UnaryExpression 的子类重写了 nullable，可能也应该重写此方法。
   */
  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      nullSafeEval(value)
    }
  }

  /**
   * 由默认的 [[eval]] 实现调用。
   * 如果 UnaryExpression 的子类保持默认的可空性，它们可以重写此方法以节省 null 检查代码。
   * 如果我们需要完全控制求值过程， 应该重写 [[eval]]。
   */
  protected def nullSafeEval(input: Any): Any =
    throw QueryExecutionErrors.notOverrideExpectedMethodsError("UnaryExpressions",
      "eval", "nullSafeEval")

  /**
   * 由一元表达式调用以生成代码块，如果其父节点返回 null 则返回 null， 如果不为 null，则使用 `f` 生成表达式。
   *
   * 例如，以下代码执行布尔反转（即 NOT）。
   * {{{
   *   defineCodeGen(ctx, ev, c => s"!($c)")
   * }}}
   *
   * @param f 接受变量名并返回 Java 代码以计算输出的函数。
   */
  protected def defineCodeGen(ctx: CodegenContext, ev: ExprCode, f: String => String): ExprCode = {
    nullSafeCodeGen(ctx, ev, eval => {
      s"${ev.value} = ${f(eval)};"
    })
  }


  /**
   * 由一元表达式调用以生成代码块，如果其父节点返回 null 则返回 null，如果不为 null，则使用 `f` 生成表达式。
   *
   * @param f 接受子节点的非 null 求值结果名称并返回 Java 代码以计算输出的函数。
   */
  protected def nullSafeCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: String => String): ExprCode = {
    val childGen = child.genCode(ctx)
    val resultCode = f(childGen.value)

    if (nullable) {
      val nullSafeEval = ctx.nullSafeExec(child.nullable, childGen.isNull)(resultCode)
      ev.copy(code = code"""
        ${childGen.code}
        boolean ${ev.isNull} = ${childGen.isNull};
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval
      """)
    } else {
      ev.copy(code = code"""
        ${childGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}

/**
 * 带有 SQL 查询上下文的表达式。上下文字符串可以从 Driver 序列化到 executor。 它也会在规则转换后保留。
 */
trait SupportQueryContext extends Expression with Serializable {
  protected var queryContext: String = initQueryContext()

  def initQueryContext(): String

  // 注意：即使查询上下文被序列化到 executor，它也会在规则转换期间从空的 "Origin" 重新生成，
  //       因为 "Origin" 不会被序列化到 executor 以获得更好的性能。因此，我们需要在转换期间
  //       复制原始查询上下文。查询上下文字符串在这里被视为表达式上的 "标签"。
  override def copyTagsFrom(other: Expression): Unit = {
    other match {
      case s: SupportQueryContext =>
        queryContext = s.queryContext
      case _ =>
    }
    super.copyTagsFrom(other)
  }
}

object UnaryExpression {
  def unapply(e: UnaryExpression): Option[Expression] = Some(e.child)
}



/**
 * 具有两个输入和一个输出的表达式。如果任何输入求值为 null，则默认情况下输出也求值为 null。
 */
abstract class BinaryExpression extends Expression with BinaryLike[Expression] {

  override def foldable: Boolean = left.foldable && right.foldable

  override def nullable: Boolean = left.nullable || right.nullable

  /**
   * 根据 BinaryExpression 的默认可空性的默认求值行为。
   * 如果 BinaryExpression 的子类重写了 nullable，可能也应该重写此方法。
   */
  override def eval(input: InternalRow): Any = {
    val value1 = left.eval(input)
    if (value1 == null) {
      null
    } else {
      val value2 = right.eval(input)
      if (value2 == null) {
        null
      } else {
        nullSafeEval(value1, value2)
      }
    }
  }

  /**
   * 由默认的 [[eval]] 实现调用。如果 BinaryExpression 的子类保持默认的可空性，
   * 它们可以重写此方法以节省 null 检查代码。如果我们需要完全控制求值过程， 应该重写 [[eval]]。
   */
  protected def nullSafeEval(input1: Any, input2: Any): Any =
    throw QueryExecutionErrors.notOverrideExpectedMethodsError("BinaryExpressions",
      "eval", "nullSafeEval")

  /**
   * 生成二元求值代码的简写。 如果任何子表达式为 null，则假定此计算的结果为 null。
   *
   * @param f 接受两个变量名并返回 Java 代码以计算输出的函数。
   */
  protected def defineCodeGen(ctx: CodegenContext, ev: ExprCode, f: (String, String) => String): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      s"${ev.value} = ${f(eval1, eval2)};"
    })
  }

  /**
   * 生成二元求值代码的简写。
   * 如果任何子表达式为 null，则假定此计算的结果为 null。
   *
   * @param f 接受子节点的 2 个非 null 求值结果名称并返回 Java 代码以计算输出的函数。
   */
  protected def nullSafeCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String) => String): ExprCode = {
    val leftGen = left.genCode(ctx)
    val rightGen = right.genCode(ctx)
    val resultCode = f(leftGen.value, rightGen.value)

    if (nullable) {
      val nullSafeEval =
        leftGen.code + ctx.nullSafeExec(left.nullable, leftGen.isNull) {
          rightGen.code + ctx.nullSafeExec(right.nullable, rightGen.isNull) {
            s"""
              ${ev.isNull} = false; // resultCode could change nullability.
              $resultCode
            """
          }
      }

      ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval
      """)
    } else {
      ev.copy(code = code"""
        ${leftGen.code}
        ${rightGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}


object BinaryExpression {
  def unapply(e: BinaryExpression): Option[(Expression, Expression)] = Some((e.left, e.right))
}


/**
 * 一个 [[BinaryExpression]]，它是一个运算符，具有两个属性：
 *
 * 1. 字符串表示是 "x symbol y"，而不是 "funcName(x, y)"。
 * 2. 两个输入应该是相同的类型。如果两个输入具有不同的类型， 分析器将找到最紧密的公共类型并进行适当的类型转换。
 */
abstract class BinaryOperator extends BinaryExpression with ExpectsInputTypes {

  /** 左/右子表达式的期望输入类型，类似于 [[ImplicitCastInputTypes]] 特性。 */
  def inputType: AbstractDataType
  def symbol: String                 // 操作运算符
  def sqlOperator: String = symbol

  override def toString: String = s"($left $sqlOperator $right)"

  override def inputTypes: Seq[AbstractDataType] = Seq(inputType, inputType)

  override def checkInputDataTypes(): TypeCheckResult = {
    // // 首先检查左右是否具有相同的类型，然后检查类型是否可接受。.
    if (!left.dataType.sameType(right.dataType)) {
      TypeCheckResult.TypeCheckFailure(s"differing types in '$sql' " +
        s"(${left.dataType.catalogString} and ${right.dataType.catalogString}).")
    } else if (!inputType.acceptsType(left.dataType)) {
      TypeCheckResult.TypeCheckFailure(s"'$sql' requires ${inputType.simpleString} type," +
        s" not ${left.dataType.catalogString}")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def sql: String = s"(${left.sql} $sqlOperator ${right.sql})"
}


object BinaryOperator {
  def unapply(e: BinaryOperator): Option[(Expression, Expression)] = Some((e.left, e.right))
}

/**
 * 具有三个输入和一个输出的表达式。如果任何输入求值为 null，则默认情况下输出也求值为 null。
 */
abstract class TernaryExpression extends Expression with TernaryLike[Expression] {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  /**
   * 根据 TernaryExpression 的默认可空性的默认求值行为。
   * 如果 TernaryExpression 的子类重写了 nullable，可能也应该重写此方法。
   */
  override def eval(input: InternalRow): Any = {
    val value1 = first.eval(input)
    if (value1 != null) {
      val value2 = second.eval(input)
      if (value2 != null) {
        val value3 = third.eval(input)
        if (value3 != null) {
          return nullSafeEval(value1, value2, value3)
        }
      }
    }
    null
  }

  /**
   * 由默认的 [[eval]] 实现调用。如果 TernaryExpression 的子类保持默认的可空性，
   * 它们可以重写此方法以节省 null 检查代码。如果我们需要完全控制求值过程， 应该重写 [[eval]]。
   */
  protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any =
    throw QueryExecutionErrors.notOverrideExpectedMethodsError("TernaryExpressions",
      "eval", "nullSafeEval")

  /**
   * 生成三元求值代码的简写。
   * 如果任何子表达式为 null，则假定此计算的结果为 null。
   *
   * @param f 接受三个变量名并返回 Java 代码以计算输出的函数。
   */
  protected def defineCodeGen(
    ctx: CodegenContext,
    ev: ExprCode,
    f: (String, String, String) => String): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2, eval3) => {
      s"${ev.value} = ${f(eval1, eval2, eval3)};"
    })
  }

  /**
   * 生成三元求值代码的简写。 如果任何子表达式为 null，则假定此计算的结果为 null。
   * @param f 接受子节点的 3 个非 null 求值结果名称并返回 Java 代码以计算输出的函数。
   */
  protected def nullSafeCodeGen(
    ctx: CodegenContext,
    ev: ExprCode,
    f: (String, String, String) => String): ExprCode = {
    val leftGen = children(0).genCode(ctx)
    val midGen = children(1).genCode(ctx)
    val rightGen = children(2).genCode(ctx)
    val resultCode = f(leftGen.value, midGen.value, rightGen.value)

    if (nullable) {
      val nullSafeEval =
        leftGen.code + ctx.nullSafeExec(children(0).nullable, leftGen.isNull) {
          midGen.code + ctx.nullSafeExec(children(1).nullable, midGen.isNull) {
            rightGen.code + ctx.nullSafeExec(children(2).nullable, rightGen.isNull) {
              s"""
                ${ev.isNull} = false; // resultCode could change nullability.
                $resultCode
              """
            }
          }
      }

      ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval""")
    } else {
      ev.copy(code = code"""
        ${leftGen.code}
        ${midGen.code}
        ${rightGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}


/**
 * 具有四个输入和一个输出的表达式。如果任何输入求值为 null，则默认情况下输出也求值为 null。
 */
abstract class QuaternaryExpression extends Expression with QuaternaryLike[Expression] {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  /**
   * 根据 QuaternaryExpression 的默认可空性的默认求值行为。
   * 如果 QuaternaryExpression 的子类重写了 nullable，可能也应该重写此方法。
   */
  override def eval(input: InternalRow): Any = {
    val value1 = first.eval(input)
    if (value1 != null) {
      val value2 = second.eval(input)
      if (value2 != null) {
        val value3 = third.eval(input)
        if (value3 != null) {
          val value4 = fourth.eval(input)
          if (value4 != null) {
            return nullSafeEval(value1, value2, value3, value4)
          }
        }
      }
    }
    null
  }

  /**
   * 由默认的 [[eval]] 实现调用。
   * 如果 QuaternaryExpression 的子类保持默认的可空性， 它们可以重写此方法以节省 null 检查代码。
   * 如果我们需要完全控制求值过程， 应该重写 [[eval]]。
   */
  protected def nullSafeEval(input1: Any, input2: Any, input3: Any, input4: Any): Any =
    throw QueryExecutionErrors.notOverrideExpectedMethodsError("QuaternaryExpressions",
      "eval", "nullSafeEval")

  /**
   * 生成四元求值代码的简写。
   * 如果任何子表达式为 null，则假定此计算的结果为 null。
   *
   * @param f 接受四个变量名并返回 Java 代码以计算输出的函数。
   */
  protected def defineCodeGen(
    ctx: CodegenContext,
    ev: ExprCode,
    f: (String, String, String, String) => String): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2, eval3, eval4) => {
      s"${ev.value} = ${f(eval1, eval2, eval3, eval4)};"
    })
  }

  /**
   * 生成四元求值代码的简写。
   * 如果任何子表达式为 null，则假定此计算的结果为 null。
   *
   * @param f 接受子节点的 4 个非 null 求值结果名称并返回 Java 代码以计算输出的函数。
   */
  protected def nullSafeCodeGen(
    ctx: CodegenContext,
    ev: ExprCode,
    f: (String, String, String, String) => String): ExprCode = {
    val firstGen = children(0).genCode(ctx)
    val secondGen = children(1).genCode(ctx)
    val thridGen = children(2).genCode(ctx)
    val fourthGen = children(3).genCode(ctx)
    val resultCode = f(firstGen.value, secondGen.value, thridGen.value, fourthGen.value)

    if (nullable) {
      val nullSafeEval =
        firstGen.code + ctx.nullSafeExec(children(0).nullable, firstGen.isNull) {
          secondGen.code + ctx.nullSafeExec(children(1).nullable, secondGen.isNull) {
            thridGen.code + ctx.nullSafeExec(children(2).nullable, thridGen.isNull) {
              fourthGen.code + ctx.nullSafeExec(children(3).nullable, fourthGen.isNull) {
                s"""
                  ${ev.isNull} = false; // resultCode could change nullability.
                  $resultCode
                """
              }
            }
          }
      }

      ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval""")
    } else {
      ev.copy(code = code"""
        ${firstGen.code}
        ${secondGen.code}
        ${thridGen.code}
        ${fourthGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}

/**
 * 具有六个输入 + 第七个可选输入和一个输出的表达式。
 * 如果任何输入求值为 null，则默认情况下输出也求值为 null。
 */
abstract class SeptenaryExpression extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  /**
   * 根据 SeptenaryExpression 的默认可空性的默认求值行为。
   * 如果 SeptenaryExpression 的子类重写了 nullable，可能也应该重写此方法。
   */
  override def eval(input: InternalRow): Any = {
    val exprs = children
    val v1 = exprs(0).eval(input)
    if (v1 != null) {
      val v2 = exprs(1).eval(input)
      if (v2 != null) {
        val v3 = exprs(2).eval(input)
        if (v3 != null) {
          val v4 = exprs(3).eval(input)
          if (v4 != null) {
            val v5 = exprs(4).eval(input)
            if (v5 != null) {
              val v6 = exprs(5).eval(input)
              if (v6 != null) {
                if (exprs.length > 6) {
                  val v7 = exprs(6).eval(input)
                  if (v7 != null) {
                    return nullSafeEval(v1, v2, v3, v4, v5, v6, Some(v7))
                  }
                } else {
                  return nullSafeEval(v1, v2, v3, v4, v5, v6, None)
                }
              }
            }
          }
        }
      }
    }
    null
  }

  /**
   * 由默认的 [[eval]] 实现调用。如果 SeptenaryExpression 的子类保持默认的可空性，
   * 它们可以重写此方法以节省 null 检查代码。如果我们需要完全控制求值过程， 应该重写 [[eval]]。
   */
  protected def nullSafeEval(
      input1: Any,
      input2: Any,
      input3: Any,
      input4: Any,
      input5: Any,
      input6: Any,
      input7: Option[Any]): Any = {
    throw QueryExecutionErrors.notOverrideExpectedMethodsError("SeptenaryExpression",
      "eval", "nullSafeEval")
  }

  /**
   * 生成七元求值代码的简写。 如果任何子表达式为 null，则假定此计算的结果为 null。
   *
   * @param f 接受七个变量名并返回 Java 代码以计算输出的函数。
   */
  protected def defineCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String, String, String, String, String, Option[String]) => String
    ): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2, eval3, eval4, eval5, eval6, eval7) => {
      s"${ev.value} = ${f(eval1, eval2, eval3, eval4, eval5, eval6, eval7)};"
    })
  }


  /**
   * 生成七元求值代码的简写。  如果任何子表达式为 null，则假定此计算的结果为 null。
   *
   * @param f 接受子节点的 7 个非 null 求值结果名称并返回 Java 代码以计算输出的函数。
   */
  protected def nullSafeCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String, String, String, String, String, Option[String]) => String
    ): ExprCode = {
    val firstGen = children(0).genCode(ctx)
    val secondGen = children(1).genCode(ctx)
    val thirdGen = children(2).genCode(ctx)
    val fourthGen = children(3).genCode(ctx)
    val fifthGen = children(4).genCode(ctx)
    val sixthGen = children(5).genCode(ctx)
    val seventhGen = if (children.length > 6) Some(children(6).genCode(ctx)) else None
    val resultCode = f(
      firstGen.value,
      secondGen.value,
      thirdGen.value,
      fourthGen.value,
      fifthGen.value,
      sixthGen.value,
      seventhGen.map(_.value))

    if (nullable) {
      val nullSafeEval =
        firstGen.code + ctx.nullSafeExec(children(0).nullable, firstGen.isNull) {
          secondGen.code + ctx.nullSafeExec(children(1).nullable, secondGen.isNull) {
            thirdGen.code + ctx.nullSafeExec(children(2).nullable, thirdGen.isNull) {
              fourthGen.code + ctx.nullSafeExec(children(3).nullable, fourthGen.isNull) {
                fifthGen.code + ctx.nullSafeExec(children(4).nullable, fifthGen.isNull) {
                  sixthGen.code + ctx.nullSafeExec(children(5).nullable, sixthGen.isNull) {
                    val nullSafeResultCode =
                      s"""
                      ${ev.isNull} = false; // resultCode could change nullability.
                      $resultCode
                      """
                    seventhGen.map { gen =>
                      gen.code + ctx.nullSafeExec(children(6).nullable, gen.isNull) {
                        nullSafeResultCode
                      }
                    }.getOrElse(nullSafeResultCode)
                  }
                }
              }
            }
          }
        }

      ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval""")
    } else {
      ev.copy(code = code"""
        ${firstGen.code}
        ${secondGen.code}
        ${thirdGen.code}
        ${fourthGen.code}
        ${fifthGen.code}
        ${sixthGen.code}
        ${seventhGen.map(_.code).getOrElse("")}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}

/**
 * 用于解析可空标志的特性，包括 [[ArrayType]] 的 `nullable`、`containsNull`
 * 和 [[MapType]] 的 `valueContainsNull`，以及输出数据类型的 containsNull、valueContainsNull 标志。
 * 这通常由组合来自多个非原始类型子表达式的数据的表达式（例如 [[CaseWhen]]）使用。
 */
trait ComplexTypeMergingExpression extends Expression {

  /** 用于解析表达式输出类型的数据类型集合。默认情况下，是所有子表达式的数据类型。 该集合不能为空。 */
  @transient
  lazy val inputTypesForMerging: Seq[DataType] = children.map(_.dataType)

  def dataTypeCheck: Unit = {
    require(inputTypesForMerging.nonEmpty, "The collection of input data types must not be empty.")

    require(TypeCoercion.haveSameType(inputTypesForMerging),
      "All input types must be the same except nullable, containsNull, valueContainsNull flags." +
        s" The input types found are\n\t${inputTypesForMerging.mkString("\n\t")}")
  }

  private lazy val internalDataType: DataType = {
    dataTypeCheck
    inputTypesForMerging.reduceLeft(TypeCoercion.findCommonTypeDifferentOnlyInNullFlags(_, _).get)
  }

  override def dataType: DataType = internalDataType
}

/**
 * 用户定义函数的通用基础特性，包括不同语言的 UDF/UDAF/UDTF 和 Hive 函数包装器。
 */
trait UserDefinedExpression {
  def name: String
}

trait CommutativeExpression extends Expression {
  /** 收集相邻的可交换操作。 */
  private def gatherCommutative(
      e: Expression,
      f: PartialFunction[CommutativeExpression, Seq[Expression]]): Seq[Expression] = e match {
    case c: CommutativeExpression if f.isDefinedAt(c) => f(c).flatMap(gatherCommutative(_, f))
    case other => other.canonicalized :: Nil
  }

  /**
   * 根据非可交换节点的 `hashCode`，重新排序表达式树中相邻的可交换运算符（如 [[And]]），以消除表面差异。
   */
  protected def orderCommutative(
      f: PartialFunction[CommutativeExpression, Seq[Expression]]): Seq[Expression] =
    gatherCommutative(this, f).sortBy(_.hashCode())
}
