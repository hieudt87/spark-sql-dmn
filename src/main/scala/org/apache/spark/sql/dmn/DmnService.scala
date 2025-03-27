package org.apache.spark.sql.dmn

import com.databricks.sdk.scala.dbutils.DBUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema, Literal}
import org.apache.spark.sql.types._
import org.camunda.bpm.dmn.engine.impl.{DmnDecisionTableImpl, DmnDecisionTableOutputImpl}
import org.camunda.bpm.dmn.engine.{DmnEngine, DmnEngineConfiguration}
import org.camunda.bpm.engine.variable.{VariableMap, Variables}

import java.io.{ByteArrayInputStream, InputStream}
import scala.collection.mutable
import scala.io.Source

/**
 * Singleton object to manage the lifecycle of the DmnService instance.
 */
object DmnService {
  @transient private lazy val instance: ThreadLocal[DmnService] = new ThreadLocal[DmnService] {
    override def initialValue(): DmnService = new DmnService()
  }

  /**
   * Retrieves the singleton instance of DmnService.
   * @return The DmnService instance.
   */
  def getInstance(): DmnService = instance.get()
}

/**
 * Service class for evaluating DMN decision tables and managing their lifecycle.
 */
class DmnService {
  private val dmnEngine: DmnEngine = DmnEngineConfiguration.createDefaultDmnEngineConfiguration.buildEngine()
  private val decisionTableInstances: mutable.HashMap[String, DecisionTable] = new mutable.HashMap[String, DecisionTable]()
  private val supportedDataTypes: Set[DataType] = Set(
    StringType,
    BooleanType,
    IntegerType,
    LongType,
    DoubleType,
    FloatType,
    DateType,
    TimestampType,
    TimestampNTZType
  )

  /**
   * Evaluates a DMN decision table with the provided variables.
   * @param decisionTable The decision table to evaluate.
   * @param variables The input variables for the decision table.
   * @return The result as an InternalRow, or null if no result is produced.
   */
  def evaluateDecisionTable(decisionTable: DecisionTable, variables: VariableMap): InternalRow = {
    val result = this.dmnEngine.evaluateDecisionTable(decisionTable.dmnDecision, variables).getSingleResult
    if (result != null) {
      val rowWithSchema = new GenericRowWithSchema(
        result.values().toArray.asInstanceOf[Array[Any]],
        decisionTable.outputSchema
      )

      val data = Literal.create(rowWithSchema, decisionTable.outputSchema)

      data.value.asInstanceOf[InternalRow]

    } else {
      null
    }
  }

  /**
   * Converts a Spark StructType and corresponding InternalRow into a DMN-compatible VariableMap.
   * @param schema The schema of the input data.
   * @param data The input data as an InternalRow.
   * @return A VariableMap compatible with the DMN engine.
   * @throws Exception If unsupported data types are found in the schema.
   */
  def structToDmnVariables(schema: StructType, data: InternalRow): VariableMap = {
    // Check for unsupported data types
    val unsupportedFields = schema.fields.filter(field => !supportedDataTypes.contains(field.dataType)).toList
    if (unsupportedFields.nonEmpty) {
      throw new Exception("Unsupported Data Types: " + unsupportedFields.mkString(","))
    }

    // Maps the input data into an object that is compatible with the DMN Engine
    val variables: VariableMap = Variables.createVariables()
    schema.zipWithIndex.foreach(field => {
      variables.put(field._1.name, data.get(field._2, field._1.dataType))
    })

    variables
  }

  /**
   * Reads the content of a DMN file from the specified path.
   * @param dmn The path to the DMN file.
   * @return The content of the DMN file as a String.
   */
  private def readFile(dmn: String): String = {
    if(dmn == null || dmn.isEmpty) {
      throw new IllegalArgumentException()
    }

    var dmnFilePath: String = null
    var dmnFileSize: Int = -1
    var dmnFileContent: String = null

    if(dmn.startsWith("/Workspace")) {
      dmnFilePath = f"file:$dmn"
    } else if(dmn.startsWith("/Volumes")) {
      dmnFilePath = f"dbfs:$dmn"
    } else {
      dmnFilePath = dmn
    }

    try {
      val dbutils = DBUtils.getDBUtils()
      dmnFileSize = dbutils.fs.ls(dmnFilePath).toList.head.size.toInt
      dmnFileContent = dbutils.fs.head(dmnFilePath, dmnFileSize)

    } catch {
      case _: Throwable =>
        val sourceFile = Source.fromFile(dmnFilePath)
        dmnFileContent = sourceFile.getLines().mkString("")
        sourceFile.close()
    }

    dmnFileContent
  }

  /**
   * Retrieves an existing DecisionTable instance or creates a new one if it does not exist or has changed.
   * @param dmnFile The path to the DMN file.
   * @param dmnFileContent Optional content of the DMN file. If not provided, it will be read from the file.
   * @return The DecisionTable instance.
   */
  def getOrCreate(dmnFile: String, dmnFileContent: String = null): DecisionTable = {
    // Read the DMN XML file from the filesystem
    var _dmnFileContent: String = dmnFileContent
    if(_dmnFileContent == null) {
      _dmnFileContent = readFile(dmnFile)
    }

    // Check if the DMN is already available in the state
    // This will also check if the cached decision table object has changed or not
    if (!this.decisionTableInstances.contains(dmnFile) ||
      this.decisionTableInstances(dmnFile).dmnFileContent != _dmnFileContent) {

      // Read the DMN file and build the Decision table object
      val inputStream: InputStream = new ByteArrayInputStream(_dmnFileContent.getBytes("UTF-8"))
      val dmnDecision = dmnEngine.parseDecisions(inputStream).get(0)
      // Extract the decision table metadata to identify the output fields
      val dmnDecisionLogic: DmnDecisionTableImpl = dmnDecision.getDecisionLogic.asInstanceOf[DmnDecisionTableImpl]
      val outputFields: Array[StructField] = dmnDecisionLogic.getOutputs.toArray().map(outputField => {
        val field: DmnDecisionTableOutputImpl = outputField.asInstanceOf[DmnDecisionTableOutputImpl]
        StructField(
          name = field.getName,
          dataType = DataType.fromDDL(field.getTypeDefinition.getTypeName)
        )
      })

      this.decisionTableInstances.put(dmnFile,
        DecisionTable(dmnDecision,
          StructType(Array(outputFields: _*)),
          _dmnFileContent))
    }

    decisionTableInstances(dmnFile)
  }
}