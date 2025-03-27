package org.apache.spark.sql.dmn

import org.apache.spark.sql.{SparkSessionExtensions, SparkSessionExtensionsProvider}

/**
 * A Spark SQL extensions provider that registers custom DMN-related functions.
 */
class DmnSqlExtensions extends SparkSessionExtensionsProvider {
  /**
   * Registers custom functions into the SparkSessionExtensions.
   * @param sse The SparkSessionExtensions instance to inject functions into.
   */
  override def apply(sse: SparkSessionExtensions): Unit = {
    FunctionRegistry.expressions.foreach(sse.injectFunction)
  }
}
