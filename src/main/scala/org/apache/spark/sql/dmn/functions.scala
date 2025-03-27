package org.apache.spark.sql.dmn

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.unsafe.types.UTF8String

/**
 * Object containing custom Spark SQL functions for DMN evaluation.
 */
object functions {
  /**
   * Wraps an expression into a Spark SQL Column.
   * @param expr The expression to wrap.
   * @return A Column representing the expression.
   */
  private def withExpr(expr: Expression): Column = new Column(expr)

  /**
   * Returns a struct containing the result of a DMN evaluation.
   *
   * @param data The input data as a Spark SQL Column.
   * @param dmn The path to the DMN file as a Spark SQL Column.
   * @return A Column containing the result of the DMN evaluation.
   * @since 1.0.0
   */
  def evaluate_decision_table(data: Column, dmn: Column): Column = {
    val decisionTable: DecisionTable = DmnService.getInstance()
      .getOrCreate(dmn.expr.eval().asInstanceOf[UTF8String].toString)

    withExpr(EvaluateDecisionTable(data.expr, dmn.expr, decisionTable.outputSchema, decisionTable.dmnFileContent))
  }

}
