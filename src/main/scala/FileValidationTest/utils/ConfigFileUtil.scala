package FileValidationTest.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.ListBuffer


object ConfigFileUtil {

  val listOfRules = new ListBuffer[String]
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hive test").setMaster("local")
    val spark = SparkSession
      .builder()
      .config(conf)
      .config("spark.sql.warehouse.dir", "/Users/amaraj0/intellijSbt/SparkScala1/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val configFile = "/Users/amaraj0/Documents/MyData/SchemaValidation/config.json";
    val confFile = spark.read.option("header","true").option("inferSchema","true").json(configFile)
    /*val numberOfHeaderRow = getHeaderCount(confFile)
    //  println(numberOfHeaderRow)
    val columnHeaderPosition = getDataRowStartPosition(confFile) -1
    // println(columnHeaderPosition)
    val columnNamesDF = getColumnNames(confFile)
    val rulesList = getRules(confFile)

    getrecordCount(confFile)*/
    getColTypes(confFile, spark)

  }

  def getHeaderCount(confFile:DataFrame): Int ={
    val headerCountDF = confFile.select(explode(col("files.fileDefinition.fileHeader.columnHeadersRow.rowNum")) as "headerRowCount")
    val numberOfHeader = headerCountDF.select("headerRowCount").take(1).toList
    return numberOfHeader(0)(0).toString().toInt
  }

  def getDataRowStartPosition(confFile:DataFrame): Int={
    val dataRowStartDF = confFile.select(explode(col("files.fileDefinition.dataRowStart.rowNum")) as "dataStartPos")
    val dataRowStartPosition = dataRowStartDF.select("dataStartPos").take(1).toList
    return dataRowStartPosition(0)(0).toString.toInt
  }

  def getColumnNames(confFile:DataFrame):DataFrame={
    val colNamesDF = confFile.select(explode(col("files.fileDefinition.fileHeader.requiredColumnHeaders")) as "colNames")
    val columnNamesDF = colNamesDF.select(explode(col("colNames")) as "columnNames")
    return columnNamesDF
  }

  def getrecordCount(confFile:DataFrame):Long={
    val recCount = confFile.select(explode(col("files.recordCount")) as "recCount")
    return recCount.take(1)(0)(0).toString.toLong
  }

  def getRules(confFile:DataFrame): List[String] ={
    val fvRulesArray = confFile.select(explode(col("files.fileValidation.rules")) as "rules")
    val fvRules = fvRulesArray.select(explode(col("rules.cdapRuleType")) as "rules")
    fvRules.select("rules").collect().foreach(x => listOfRules += getListOfRules(x(0).toString()))
  //  println(listOfRules.toList)
    return listOfRules.toList
  }

  def getListOfRules(rulesType:String): String  = rulesType match {

    case "BR_FILENAME_CHECK" => "FILE_NAME"
    case "BR_VALIDATE_RECORD_COUNT" => "RECORD_COUNT"
    case "BR_VALIDATE_COLUMN_HEADER" => "COLUMN_HEADER"

  }

  def getColTypes(confFile:DataFrame, spark:SparkSession): Unit ={

    val fvCols = confFile.select(explode(col("files.records.schema.columns")) as "cols")
    val fvDataType = fvCols.select(explode(col("cols.dataType")) as "dataType")
    fvDataType.na.replace("dataType", Map("string"->"StringType", "int"->"IntegerType")).show()
  }



}
