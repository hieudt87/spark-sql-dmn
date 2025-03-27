package org.apache.spark.sql

import org.apache.spark.sql.types.StructType
import org.camunda.bpm.dmn.engine.DmnDecision

/**
 * Package object for DMN-related utilities and methods.
 * Provides functionality to manually register or deregister custom Spark SQL functions.
 */
package object dmn {
  /**
   * Registers all custom DMN-related functions in the given SparkSession.
   * @param sparkSession The SparkSession instance where functions will be registered.
   */
  def registerAll(sparkSession: SparkSession): Unit = {
    val functionRegistry = sparkSession.sessionState.functionRegistry
      FunctionRegistry.expressions.foreach { case (functionId, info, builder) =>
      functionRegistry.registerFunction(functionId, info, builder)
    }
  }

  /**
   * Deregisters all custom DMN-related functions from the given SparkSession.
   * @param sparkSession The SparkSession instance where functions will be deregistered.
   */
  def dropAll(sparkSession: SparkSession): Unit = {
    val functionRegistry = sparkSession.sessionState.functionRegistry
    FunctionRegistry.expressions.foreach { case (functionId, _, _) =>
      functionRegistry.dropFunction(functionId)
    }
  }

  /**
   * Case class representing a DMN decision table.
   * @param dmnDecision The parsed DMN decision.
   * @param outputSchema The output schema of the decision table.
   * @param dmnFileContent The content of the DMN file.
   */
  case class DecisionTable(dmnDecision: DmnDecision,
                           outputSchema: StructType,
                           dmnFileContent: String)
}
