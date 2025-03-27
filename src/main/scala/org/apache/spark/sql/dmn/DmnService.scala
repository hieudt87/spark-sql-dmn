package org.apache.spark.sql.dmn

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType, TimestampNTZType, TimestampType}
import org.camunda.bpm.dmn.engine.impl.{DmnDecisionTableImpl, DmnDecisionTableOutputImpl}
import org.camunda.bpm.dmn.engine.{DmnDecision, DmnEngine, DmnEngineConfiguration}
import org.camunda.bpm.engine.variable.{VariableMap, Variables}
import org.apache.spark.sql.catalyst.expressions.Literal
import com.databricks.sdk.scala.dbutils.DBUtils

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.io.Source

case class DecisionTable(decisionTable: DmnDecision, outputSchema: StructType)

object DmnService {
  @transient private lazy val instance: ThreadLocal[DmnService] = new ThreadLocal[DmnService] {
    override def initialValue(): DmnService = new DmnService()
  }

  def getInstance(): DmnService = instance.get()
}

class DmnService {
  private val dmnEngine: DmnEngine = DmnEngineConfiguration.createDefaultDmnEngineConfiguration.buildEngine()
  private val decisionTableInstances = new mutable.HashMap[String, DecisionTable]()
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

  private val dbutils = DBUtils.getDBUtils()

  def evaluateDecisionTable(dmn: String, variables: VariableMap): InternalRow = {
    val decisionTableInstance = this.getOrCreate(dmn)
    val result = this.dmnEngine.evaluateDecisionTable(decisionTableInstance.decisionTable, variables).getSingleResult

    if (result != null) {
      val rowWithSchema = new GenericRowWithSchema(
        result.values().toArray.asInstanceOf[Array[Any]],
        decisionTableInstance.outputSchema)

      val data = Literal.create(rowWithSchema, decisionTableInstance.outputSchema)

      data.value.asInstanceOf[InternalRow]

    } else {
      null
    }
  }

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

  private def readDmnFile(dmn: String): String = {
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

  def getOrCreate(dmn: String): DecisionTable = {
    if (!this.decisionTableInstances.contains(dmn)) {
      // Read the DMN file and build the Decision table object
      val inputStream: InputStream = new ByteArrayInputStream(readDmnFile(dmn).getBytes(StandardCharsets.UTF_8))
      val decisionTable = dmnEngine.parseDecisions(inputStream).get(0)
      inputStream.close()

      // Extract the decision table metadata to identify the output fields
      val dmnDecisionLogic: DmnDecisionTableImpl = decisionTable.getDecisionLogic.asInstanceOf[DmnDecisionTableImpl]
      val outputFields: Array[StructField] = dmnDecisionLogic.getOutputs.toArray().map(outputField => {
        val field: DmnDecisionTableOutputImpl = outputField.asInstanceOf[DmnDecisionTableOutputImpl]
        StructField(
          name = field.getName,
          dataType = DataType.fromDDL(field.getTypeDefinition.getTypeName)
        )
      })

      this.decisionTableInstances.put(dmn,
        DecisionTable(decisionTable, StructType(Array(outputFields: _*))))
    }

    decisionTableInstances(dmn)
  }
}