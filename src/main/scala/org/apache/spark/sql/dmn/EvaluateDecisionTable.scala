package org.apache.spark.sql.dmn

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.camunda.bpm.engine.variable.VariableMap

/**
 * A Spark SQL expression that evaluates a DMN decision table.
 *
 * @param left The input data as a struct.
 * @param right The path to the DMN file.
 * @param dataType The output schema of the decision table (optional, inferred if not provided).
 * @param dmnFileContent The content of the DMN file (optional, read if not provided).
 */
@ExpressionDescription(
  usage = "_FUNC_(data, dmn) - Returns a struct containing the result of a DMN evaluation.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(named_struct('col1', 1), 'decision_table.dmn');
       {"result": true}
  """,
  since = "1.0.0")
case class EvaluateDecisionTable(left: Expression, right: Expression,
                                 dataType: DataType = null, dmnFileContent: String = null)
  extends BinaryExpression
    with CodegenFallback
    with ImplicitCastInputTypes
    with NullIntolerant {

  /**
   * Constructor that initializes the output schema and DMN file content by evaluating the DMN file.
   * @param left The input data as a struct.
   * @param right The path to the DMN file.
   */
  def this(left: Expression, right: Expression) = {
    this(left, right,
      DmnService.getInstance().getOrCreate(right.eval().asInstanceOf[UTF8String].toString).outputSchema,
      DmnService.getInstance().getOrCreate(right.eval().asInstanceOf[UTF8String].toString).dmnFileContent
    )
  }

  /**
   * Specifies the expected input types for the expression.
   * @return A sequence of expected input types.
   */
  override def inputTypes: Seq[AbstractDataType] = Seq(StructType, StringType)

  /**
   * Evaluates the DMN decision table with the provided input data and DMN file.
   * @param leftAny The input data as an InternalRow.
   * @param rightAny The path to the DMN file as a UTF8String.
   * @return The result of the DMN evaluation as an InternalRow.
   */
  override protected def nullSafeEval(leftAny: Any, rightAny: Any): Any = {
    val inputRow: InternalRow = leftAny.asInstanceOf[InternalRow]
    val decisionTable: DecisionTable = DmnService.getInstance().getOrCreate(rightAny.asInstanceOf[UTF8String].toString, dmnFileContent)
    val variables: VariableMap = DmnService.getInstance().structToDmnVariables(left.dataType.asInstanceOf[StructType], inputRow)
    val result: InternalRow = DmnService.getInstance().evaluateDecisionTable(decisionTable, variables)

    result
  }

  /**
   * Creates a new instance of the expression with updated child expressions.
   * @param newLeft The updated left child expression.
   * @param newRight The updated right child expression.
   * @return A new instance of EvaluateDecisionTable.
   */
  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): EvaluateDecisionTable =
    copy(left = newLeft, right = newRight)

  /**
   * Returns the name of the expression for pretty printing.
   * @return The name of the expression.
   */
  override def prettyName: String = "evaluate_decision_table"
}