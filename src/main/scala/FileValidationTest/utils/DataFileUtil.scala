package FileValidationTest.utils

import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object DataFileUtil {

  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("File Validation").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .config(conf)
      .config("spark.sql.warehouse.dir", "/Users/amaraj0/intellijSbt/SparkScala1/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val inputFile = "/Users/amaraj0/Documents/MyData/SchemaValidation/Hitwise_webtraffic_product-level_demographic-permutations_20180317_20180318_0651.csv"
    val configFile = "/Users/amaraj0/Documents/MyData/SchemaValidation/config.json"
    val confFile = spark.read.option("header","true").option("inferSchema","true").json(configFile)
    getFileHeader(spark, confFile, inputFile)
    getFileRecordCount(spark,confFile,inputFile)
  }

  def getFileHeader(spark:SparkSession,confFile:DataFrame, inputFile:String): List[String] ={

    val numOfHeaderRow = ConfigFileUtil.getHeaderCount(confFile)
    val dataRowStart = ConfigFileUtil.getDataRowStartPosition(confFile)
    var fileHeaderList:List[String] = null
    var colHeaderNameList:List[String] = null

    if(numOfHeaderRow == 0){
      val dataFile = spark.read.csv(inputFile)
    }else{
      val dataFile = spark.read.textFile(inputFile).select("*")
      val headerRow = dataFile.take(dataRowStart-1).drop(dataRowStart-2)
      fileHeaderList = headerRow.toList(0).toSeq.toList.map(x => (x.toString))
      colHeaderNameList = fileHeaderList(0).split(",").toList
    }
    return colHeaderNameList
  }

  def getFileRecordCount(spark:SparkSession,confFile:DataFrame, inputFile:String): Long ={
    val numOfHeaderRow = ConfigFileUtil.getHeaderCount(confFile)
    val dataRowStart = ConfigFileUtil.getDataRowStartPosition(confFile)
    var fileRecCount:Long = 0
    if(numOfHeaderRow == 0){
      val dataFile = spark.read.csv(inputFile)
      fileRecCount = dataFile.count()
    }else {
      val dataFile = spark.read.csv(inputFile).select("*").withColumn("id", monotonically_increasing_id())
      fileRecCount = dataFile.select("*").where(col("id") >= dataRowStart).count() + 1
    }
    return fileRecCount
  }

  def getFileName(spark:SparkSession,confFile:DataFrame, inputFile:String): String ={
    val numOfHeaderRow = ConfigFileUtil.getHeaderCount(confFile)
    val dataRowStart = ConfigFileUtil.getDataRowStartPosition(confFile)
    var fileNameRow:String = null
    if(numOfHeaderRow > 1){
       fileNameRow = spark.read.textFile(inputFile).first().split(",")(0)
    }

    return fileNameRow
  }
}
