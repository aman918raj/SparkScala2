package FileValidationTest.Validation

import java.util.{Calendar, StringJoiner}

import FileValidationTest.utils.{ConfigFileUtil, DataFileUtil, HdfsUtil}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer

object FValidation {

  def main(args:Array[String]): Unit = {
    val conf = new SparkConf().setAppName("File Validation").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .config(conf)
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val inputPath = "/Users/amaraj0/Documents/MyData/FileValidation/data"
    val vaildFilesPath = "/Users/amaraj0/Documents/MyData/FileValidation/data_files"
    val invalidFilesPath = "/Users/amaraj0/Documents/MyData/FileValidation/invalid_files"
    val listOfFiles = HdfsUtil.getListOfDataFiles(inputPath)
    val configFile = "/Users/amaraj0/Documents/MyData/FileValidation/config/config.json"
    val confFile = spark.read.option("header", "true").option("inferSchema", "true").json(configFile)
    val listOfRules = ConfigFileUtil.getRules(confFile)
    listOfFiles.foreach(x => validateBasedOnRules(spark, confFile, x, listOfRules, vaildFilesPath, invalidFilesPath))

  }

  def validateBasedOnRules(spark:SparkSession, confFile:DataFrame, inputFile: String, listOfRules:List[String], vaildFilesPath:String, invalidFilesPath:String): Unit ={

    val fvStatusList = new ListBuffer[String]
    val fvStatus = new ListBuffer[String]
    listOfRules.foreach(x => if(validationRules(x,confFile, spark, inputFile) == true){
      println(x + " : passed ")
      fvStatusList += x + " : passed "
      fvStatus += "passed"
    }else{
      println(x + " : failed")
      fvStatusList += x + " : failed "
      fvStatus += "failed"
    })
    writeToHive(spark,inputFile,fvStatusList.toList)
    println(fvStatusList.toList)
    if(!fvStatus.contains("failed")){
        HdfsUtil.moveFileToValidorInvalid(vaildFilesPath, inputFile)
    }else{
      HdfsUtil.moveFileToValidorInvalid(invalidFilesPath, inputFile)
    }
  }

  def validationRules(rules:String, confFile:DataFrame, spark:SparkSession, inputFile: String):Boolean = rules match{

  //  case "FILE_NAME" => fileNameCheck()
    case "RECORD_COUNT" => recCountCheck(confFile, spark, inputFile)
    case "COLUMN_HEADER" => columnHeaderCheck(confFile, spark, inputFile)
    case "FILE_NAME" => fileNameCheck(confFile, spark, inputFile)
    case _ => true
  }

  def fileNameCheck(confFile:DataFrame, spark:SparkSession, inputFile: String): Boolean ={

    val dataFileName = DataFileUtil.getFileName(spark,confFile,inputFile)
    val fileName = HdfsUtil.getFileName(inputFile)
    println("fileNames : " + dataFileName + "   " + fileName)
    if(dataFileName.equals(fileName)){
      true
    }else{
      false
    }
  }

  def recCountCheck(confFile:DataFrame, spark:SparkSession, inputFile: String): Boolean ={

    val configRecCount = ConfigFileUtil.getrecordCount(confFile)
    val dataFileRecCount = DataFileUtil.getFileRecordCount(spark, confFile, inputFile)
    if(configRecCount == dataFileRecCount){
      true
    }
    else {
      false
    }
  }

  def columnHeaderCheck(confFile:DataFrame, spark:SparkSession, inputFile: String): Boolean ={

    import spark.implicits._
    val configColumnHeader = ConfigFileUtil.getColumnNames(confFile)
    val dataFileHeader = DataFileUtil.getFileHeader(spark, confFile, inputFile)
    val dataFileColumnHeader = dataFileHeader.toDF(Seq("colNames"):_*)
    val dataFileHeaderListCount = dataFileColumnHeader.count()
    val configHeaderListCount = configColumnHeader.count()
    var flag:Boolean = false

    if(dataFileHeaderListCount == configHeaderListCount){
      val matchColNames = dataFileColumnHeader.join(configColumnHeader, dataFileColumnHeader.col("colNames") === configColumnHeader.col("columnNames"), "inner" )
      if((matchColNames.count() == dataFileHeaderListCount) &&  (matchColNames.count() == configHeaderListCount)){
        flag = true
      }
    }else{
      flag = false
    }
    return flag
  }

  def writeToHive(spark:SparkSession, inputFile:String, fvStatusList:List[String]): Unit ={

    val date = Calendar.getInstance()
    val timeMillis = date.getTimeInMillis.toString
    val rowKey = timeMillis + "|" + inputFile
    spark.sql("create database if not exists FValidationDB1")
    spark.sql("create external table if not exists fvalidationdb1.file_Validation(rowKey String, jobId String, fileName String, status Array<String>) row format delimited fields terminated by ',' stored as textFile location '/Users/amaraj0/hive/hive-warehouse'")

    val sj = new StringJoiner(",")
    val lisOfHiveColumns = List(rowKey,timeMillis,inputFile, fvStatusList.toArray)
    val row = Row.fromSeq(lisOfHiveColumns)
    val fieldList = List(StructField("rowKey",StringType, nullable = true),
      StructField("jobId",StringType, nullable = true),
      StructField("fileName",StringType, nullable = true),
      StructField("status",ArrayType(StringType,true), nullable = true)
    )
    val rdd = spark.sparkContext.makeRDD(List(row))
    val rddToDF = spark.createDataFrame(rdd,StructType(fieldList)).createOrReplaceTempView("tempTable")
    spark.sql("insert into table fvalidationdb1.file_Validation select * from tempTable")
    spark.sql("select * from fvalidationdb1.file_Validation").show()

  }

}
