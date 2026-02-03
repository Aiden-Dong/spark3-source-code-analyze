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
 *     ├── [[NullIntolerant]]  - 任何 null 输入都产生 null 输出 : 用于优化 null 值处理
 *
 * 如果一个表达式希望在函数注册表中暴露（以便用户可以使用 "name(arguments...)" 调用它），
 * 具体的实现必须是一个 case 类，其构造函数参数都是表达式类型。例如，参见 [[Substring]]。
 * 有一些重要的特征或抽象类：
 *
 *
 */
abstract class Expression extends TreeNode[Expression] {

  /*====================================================*
   * 基础属性方法
   *====================================================*/

  /// 返回表达式的数据类型
  def dataType: DataType

  ///  判断表达式是否可在编译时常量折叠优化
  def foldable: Boolean = false

  /// 判断表达式是否确定性（相同输入总是产生相同输出）
  lazy val deterministic: Boolean = children.forall(_.deterministic)

  /// 返回表达式引用的属性集合
  def references: AttributeSet = _references

  /*====================================================*
   * 求值方法
   *====================================================*/

  /// 在给定输入行上求值表达式，返回结果
  def eval(input: InternalRow = null): Any

  /// 生成Java代码用于代码生成优化
  def genCode(ctx: CodegenContext): ExprCode = {
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

  ///  具体的代码生成实现
  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode

  /*====================================================*
   * 解析和类型检查
   *====================================================*/

  //// 判断表达式是否已解析（所有子表达式已解析且类型检查通过）
  lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess

  /// 判断所有子表达式是否已解析
  def childrenResolved: Boolean = children.forall(_.resolved)

  /// 检查输入数据类型是否有效
  def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.TypeCheckSuccess

  /*====================================================*
   * 语义比较
   *====================================================*/

  //// 返回规范化的表达式，用于语义比较
  lazy val canonicalized: Expression = {
    val canonicalizedChildren = children.map(_.canonicalized)
    withNewChildren(canonicalizedChildren)
  }

  //// 判断两个表达式是否语义相等s
  final def semanticEquals(other: Expression): Boolean =
    deterministic && other.deterministic && canonicalized == other.canonicalized

  //// 返回语义哈希值
  def semanticHash(): Int = canonicalized.hashCode()

  /*====================================================*
   * 字符串表示
   *====================================================*/

  // 返回用户友好的表达式名称
  def prettyName: String = getTagValue(FunctionRegistry.FUNC_ALIAS)
    .getOrElse(nodeName.toLowerCase(Locale.ROOT))

  // 返回表达式的字符串表示
  override def toString: String = prettyName + truncatedString(
    flatArguments.toSeq, "(", ", ", ")", SQLConf.get.maxToStringFields)

  // 返回表达式的SQL表示
  def sql: String = {
    val childrenSQL = children.map(_.sql).mkString(", ")
    s"$prettyName($childrenSQL)"
  }


  def nullable: Boolean

  // 为了在懒加载值上调用 super，可以使用以下工作方式
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
 * 无法求值的表达式。
 * 这些表达式不会在分析或优化阶段之后存在（例如 Star），不应在查询规划和执行期间进行评估。
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
 * An expression that gets replaced at runtime (currently by the optimizer) into a different
 * expression for evaluation. This is mainly used to provide compatibility with other databases.
 * For example, we use this to support "nvl" by replacing it with "coalesce".
 */
trait RuntimeReplaceable extends Expression {
  def replacement: Expression

  override val nodePatterns: Seq[TreePattern] = Seq(RUNTIME_REPLACEABLE)
  override def nullable: Boolean = replacement.nullable
  override def dataType: DataType = replacement.dataType
  // As this expression gets replaced at optimization with its `child" expression,
  // two `RuntimeReplaceable` are considered to be semantically equal if their "child" expressions
  // are semantically equal.
  override lazy val canonicalized: Expression = replacement.canonicalized

  final override def eval(input: InternalRow = null): Any =
    throw QueryExecutionErrors.cannotEvaluateExpressionError(this)
  final override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)
}

/**
 * An add-on of [[RuntimeReplaceable]]. It makes `replacement` the child of the expression, to
 * inherit the analysis rules for it, such as type coercion. The implementation should put
 * `replacement` in the case class constructor, and define a normal constructor that accepts only
 * the original parameters. For an example, see [[TryAdd]]. To make sure the explain plan and
 * expression SQL works correctly, the implementation should also implement the `parameters` method.
 */
trait InheritAnalysisRules extends UnaryLike[Expression] { self: RuntimeReplaceable =>
  override def child: Expression = replacement
  def parameters: Seq[Expression]
  override def flatArguments: Iterator[Any] = parameters.iterator
  // This method is used to generate a SQL string with transformed inputs. This is necessary as
  // the actual inputs are not the children of this expression.
  def makeSQLString(childrenSQL: Seq[String]): String = {
    prettyName + childrenSQL.mkString("(", ", ", ")")
  }
  final override def sql: String = makeSQLString(parameters.map(_.sql))
}

/**
 * An add-on of [[AggregateFunction]]. This gets rewritten (currently by the optimizer) into a
 * different aggregate expression for evaluation. This is mainly used to provide compatibility
 * with other databases. For example, we use this to support every, any/some aggregates by rewriting
 * them with Min and Max respectively.
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
 * Expressions that don't have SQL representation should extend this trait.  Examples are
 * `ScalaUDF`, `ScalaUDAF`, and object expressions like `MapObjects` and `Invoke`.
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


/**
 * An expression that is nondeterministic.
 */
trait Nondeterministic extends Expression {
  final override lazy val deterministic: Boolean = false
  final override def foldable: Boolean = false

  @transient
  private[this] var initialized = false

  /**
   * Initializes internal states given the current partition index and mark this as initialized.
   * Subclasses should override [[initializeInternal()]].
   */
  final def initialize(partitionIndex: Int): Unit = {
    initializeInternal(partitionIndex)
    initialized = true
  }

  protected def initializeInternal(partitionIndex: Int): Unit

  /**
   * @inheritdoc
   * Throws an exception if [[initialize()]] is not called yet.
   * Subclasses should override [[evalInternal()]].
   */
  final override def eval(input: InternalRow = null): Any = {
    require(initialized,
      s"Nondeterministic expression ${this.getClass.getName} should be initialized before eval.")
    evalInternal(input)
  }

  protected def evalInternal(input: InternalRow): Any
}

/**
 * An expression that contains conditional expression branches, so not all branches will be hit.
 * All optimization should be careful with the evaluation order.
 */
trait ConditionalExpression extends Expression {
  final override def foldable: Boolean = children.forall(_.foldable)

  /**
   * Return the children expressions which can always be hit at runtime.
   */
  def alwaysEvaluatedInputs: Seq[Expression]

  /**
   * Return groups of branches. For each group, at least one branch will be hit at runtime,
   * so that we can eagerly evaluate the common expressions of a group.
   */
  def branchGroups: Seq[Seq[Expression]]
}

/**
 * An expression that contains mutable state. A stateful expression is always non-deterministic
 * because the results it produces during evaluation are not only dependent on the given input
 * but also on its internal state.
 *
 * The state of the expressions is generally not exposed in the parameter list and this makes
 * comparing stateful expressions problematic because similar stateful expressions (with the same
 * parameter list) but with different internal state will be considered equal. This is especially
 * problematic during tree transformations. In order to counter this the `fastEquals` method for
 * stateful expressions only returns `true` for the same reference.
 *
 * A stateful expression should never be evaluated multiple times for a single row. This should
 * only be a problem for interpreted execution. This can be prevented by creating fresh copies
 * of the stateful expression before execution, these can be made using the `freshCopy` function.
 */
trait Stateful extends Nondeterministic {
  /**
   * Return a fresh uninitialized copy of the stateful expression.
   */
  def freshCopy(): Stateful

  /**
   * Only the same reference is considered equal.
   */
  override def fastEquals(other: TreeNode[_]): Boolean = this eq other
}

/**
 * A leaf expression, i.e. one without any child expressions.
 */
abstract class LeafExpression extends Expression with LeafLike[Expression]


/**
 * An expression with one input and one output. The output is by default evaluated to null
 * if the input is evaluated to null.
 */
abstract class UnaryExpression extends Expression with UnaryLike[Expression] {

  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable

  /**
   * Default behavior of evaluation according to the default nullability of UnaryExpression.
   * If subclass of UnaryExpression override nullable, probably should also override this.
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
   * Called by default [[eval]] implementation.  If subclass of UnaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input: Any): Any =
    throw QueryExecutionErrors.notOverrideExpectedMethodsError("UnaryExpressions",
      "eval", "nullSafeEval")

  /**
   * Called by unary expressions to generate a code block that returns null if its parent returns
   * null, and if not null, use `f` to generate the expression.
   *
   * As an example, the following does a boolean inversion (i.e. NOT).
   * {{{
   *   defineCodeGen(ctx, ev, c => s"!($c)")
   * }}}
   *
   * @param f function that accepts a variable name and returns Java code to compute the output.
   */
  protected def defineCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: String => String): ExprCode = {
    nullSafeCodeGen(ctx, ev, eval => {
      s"${ev.value} = ${f(eval)};"
    })
  }

  /**
   * Called by unary expressions to generate a code block that returns null if its parent returns
   * null, and if not null, use `f` to generate the expression.
   *
   * @param f function that accepts the non-null evaluation result name of child and returns Java
   *          code to compute the output.
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
 * An expression with SQL query context. The context string can be serialized from the Driver
 * to executors. It will also be kept after rule transforms.
 */
trait SupportQueryContext extends Expression with Serializable {
  protected var queryContext: String = initQueryContext()

  def initQueryContext(): String

  // Note: Even though query contexts are serialized to executors, it will be regenerated from an
  //       empty "Origin" during rule transforms since "Origin"s are not serialized to executors
  //       for better performance. Thus, we need to copy the original query context during
  //       transforms. The query context string is considered as a "tag" on the expression here.
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
 * An expression with two inputs and one output. The output is by default evaluated to null
 * if any input is evaluated to null.
 */
abstract class BinaryExpression extends Expression with BinaryLike[Expression] {

  override def foldable: Boolean = left.foldable && right.foldable

  override def nullable: Boolean = left.nullable || right.nullable

  /**
   * Default behavior of evaluation according to the default nullability of BinaryExpression.
   * If subclass of BinaryExpression override nullable, probably should also override this.
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
   * Called by default [[eval]] implementation.  If subclass of BinaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input1: Any, input2: Any): Any =
    throw QueryExecutionErrors.notOverrideExpectedMethodsError("BinaryExpressions",
      "eval", "nullSafeEval")

  /**
   * Short hand for generating binary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts two variable names and returns Java code to compute the output.
   */
  protected def defineCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String) => String): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      s"${ev.value} = ${f(eval1, eval2)};"
    })
  }

  /**
   * Short hand for generating binary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 2 non-null evaluation result names of children
   *          and returns Java code to compute the output.
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
 * A [[BinaryExpression]] that is an operator, with two properties:
 *
 * 1. The string representation is "x symbol y", rather than "funcName(x, y)".
 * 2. Two inputs are expected to be of the same type. If the two inputs have different types,
 *    the analyzer will find the tightest common type and do the proper type casting.
 */
abstract class BinaryOperator extends BinaryExpression with ExpectsInputTypes {

  /**
   * Expected input type from both left/right child expressions, similar to the
   * [[ImplicitCastInputTypes]] trait.
   */
  def inputType: AbstractDataType

  def symbol: String

  def sqlOperator: String = symbol

  override def toString: String = s"($left $sqlOperator $right)"

  override def inputTypes: Seq[AbstractDataType] = Seq(inputType, inputType)

  override def checkInputDataTypes(): TypeCheckResult = {
    // First check whether left and right have the same type, then check if the type is acceptable.
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
 * An expression with three inputs and one output. The output is by default evaluated to null
 * if any input is evaluated to null.
 */
abstract class TernaryExpression extends Expression with TernaryLike[Expression] {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  /**
   * Default behavior of evaluation according to the default nullability of TernaryExpression.
   * If subclass of TernaryExpression override nullable, probably should also override this.
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
   * Called by default [[eval]] implementation.  If subclass of TernaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any =
    throw QueryExecutionErrors.notOverrideExpectedMethodsError("TernaryExpressions",
      "eval", "nullSafeEval")

  /**
   * Short hand for generating ternary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts three variable names and returns Java code to compute the output.
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
   * Short hand for generating ternary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 3 non-null evaluation result names of children
   *          and returns Java code to compute the output.
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
 * An expression with four inputs and one output. The output is by default evaluated to null
 * if any input is evaluated to null.
 */
abstract class QuaternaryExpression extends Expression with QuaternaryLike[Expression] {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  /**
   * Default behavior of evaluation according to the default nullability of QuaternaryExpression.
   * If subclass of QuaternaryExpression override nullable, probably should also override this.
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
   * Called by default [[eval]] implementation.  If subclass of QuaternaryExpression keep the
   *  default nullability, they can override this method to save null-check code.  If we need
   *  full control of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input1: Any, input2: Any, input3: Any, input4: Any): Any =
    throw QueryExecutionErrors.notOverrideExpectedMethodsError("QuaternaryExpressions",
      "eval", "nullSafeEval")

  /**
   * Short hand for generating quaternary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts four variable names and returns Java code to compute the output.
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
   * Short hand for generating quaternary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 4 non-null evaluation result names of children
   *          and returns Java code to compute the output.
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
 * An expression with six inputs + 7th optional input and one output.
 * The output is by default evaluated to null if any input is evaluated to null.
 */
abstract class SeptenaryExpression extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  /**
   * Default behavior of evaluation according to the default nullability of SeptenaryExpression.
   * If subclass of SeptenaryExpression override nullable, probably should also override this.
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
   * Called by default [[eval]] implementation.  If subclass of SeptenaryExpression keep the
   * default nullability, they can override this method to save null-check code.  If we need
   * full control of evaluation process, we should override [[eval]].
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
   * Short hand for generating septenary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts seven variable names and returns Java code to compute the output.
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
   * Short hand for generating septenary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 7 non-null evaluation result names of children
   *          and returns Java code to compute the output.
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
 * A trait used for resolving nullable flags, including `nullable`, `containsNull` of [[ArrayType]]
 * and `valueContainsNull` of [[MapType]], containsNull, valueContainsNull flags of the output date
 * type. This is usually utilized by the expressions (e.g. [[CaseWhen]]) that combine data from
 * multiple child expressions of non-primitive types.
 */
trait ComplexTypeMergingExpression extends Expression {

  /**
   * A collection of data types used for resolution the output type of the expression. By default,
   * data types of all child expressions. The collection must not be empty.
   */
  @transient
  lazy val inputTypesForMerging: Seq[DataType] = children.map(_.dataType)

  def dataTypeCheck: Unit = {
    require(
      inputTypesForMerging.nonEmpty,
      "The collection of input data types must not be empty.")
    require(
      TypeCoercion.haveSameType(inputTypesForMerging),
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
 * Common base trait for user-defined functions, including UDF/UDAF/UDTF of different languages
 * and Hive function wrappers.
 */
trait UserDefinedExpression {
  def name: String
}

trait CommutativeExpression extends Expression {
  /** Collects adjacent commutative operations. */
  private def gatherCommutative(
      e: Expression,
      f: PartialFunction[CommutativeExpression, Seq[Expression]]): Seq[Expression] = e match {
    case c: CommutativeExpression if f.isDefinedAt(c) => f(c).flatMap(gatherCommutative(_, f))
    case other => other.canonicalized :: Nil
  }

  /**
   * Reorders adjacent commutative operators such as [[And]] in the expression tree, according to
   * the `hashCode` of non-commutative nodes, to remove cosmetic variations.
   */
  protected def orderCommutative(
      f: PartialFunction[CommutativeExpression, Seq[Expression]]): Seq[Expression] =
    gatherCommutative(this, f).sortBy(_.hashCode())
}
