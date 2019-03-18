package com.schemaValidation

import java.util

import com.LogKafka.KafkaProducerLogs
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._


object SchemaValidate {

  val kafkaProducerObject = new KafkaProducerLogs();
  def main(args:Array[String]): Unit ={

    val conf = new SparkConf().setAppName("Schema Validation").setMaster("local")
    val spark = SparkSession
                .builder()
                .config(conf)
                .config("spark.sql.warehouse.dir", "/Users/amaraj0/intellijSbt/SparkScala1/spark-warehouse")
                .enableHiveSupport()
                .getOrCreate()

    val inputFile = "/Users/amaraj0/Documents/MyData/SchemaValidation/Hitwise_webtraffic_product-level_demographic-permutations_20180317_20180318_0651.csv";
    val configFile = "/Users/amaraj0/Documents/MyData/SchemaValidation/config.json";
//    spark.sql("create database SchemaValidation")
    spark.sql("drop table SchemaValidation.hitwise_validrecords")
    spark.sql("drop table SchemaValidation.hitwise_invalidrecords")

    spark.sql("create table if not exists SchemaValidation.hitwise_validrecords(Brand string," +
      "Domain string, Date string, Region string, Mosaic_UK_2013_Group_Code string," +
      "Mosaic_UK_2013_Group string, Product string, " +
      "Total_Visits int, Threshold_Index int) row format delimited fields terminated by ',' stored as textFile")

    spark.sql("create table if not exists SchemaValidation.hitwise_invalidrecords(Brand string," +
      "Domain string, Date string, Region string, Mosaic_UK_2013_Group_Code string, " +
      "Mosaic_UK_2013_Group string, Product string, " +
      "Total_Visits int, Threshold_Index int) row format delimited fields terminated by ',' stored as textFile")

    ReadData.readFile(spark, inputFile, configFile)
    spark.sql("select * from SchemaValidation.hitwise_validrecords").show()
    spark.sql("select * from SchemaValidation.hitwise_invalidrecords").show()
  }

  def validate(spark:SparkSession,newFile:DataFrame,configDataType:List[String], configColName:List[String]): Unit ={
    var compareList = List[String]()
    import spark.implicits._
    for(i <- 1 to newFile.count().toInt){

      for(n <- 0 to configColName.size -1){
        val newFileColumn = newFile.select(configColName(n))
        val newFileRow = newFileColumn.schema.fields.map(x => x.dataType).take(i).last
        val test = newFileColumn.take(i).last.toString
        val fileElement = getDataTypeFromFile(newFileRow)
        //println(newFileColumn.schema.fields.map(x => x.toString()).take(i).last)
        /*println(fileElement)
        println("test")*/
        compareList = compareDataType(configDataType, fileElement, n) :: compareList

        }

      if( compareList.contains("false")){
        newFile.createOrReplaceTempView("newFile")
        val inValidFileDf = spark.sql("select * , row_num from (select *,row_number() over (order by null) as row_num from newFile) newFile where row_num = "+i)
        writeIntoInvalidHiveTable(spark,inValidFileDf)
        kafkaProducerObject.logMessage(inValidFileDf.select("*").collect().map(row => row.toString()).mkString(","), "jobId")
      }
      else {
        newFile.createOrReplaceTempView("newFile")
        val validFileDf = spark.sql("select * , row_num from (select *,row_number() over (order by null) as row_num from newFile) newFile where row_num = "+i)
        writeIntoValidHiveTable(spark,validFileDf)
        kafkaProducerObject.logMessage(validFileDf.select("*").collect().map(row => row.toString()).mkString(","), "jobId")

      }
      compareList.filter(x=> !x.equals("true") || !x.equals("false"))

    }
  }

  def getDataTypeFromFile(elementDataType : org.apache.spark.sql.types.DataType): String = elementDataType match {

    case IntegerType => "int"
    case StringType => "string"
    case FloatType => "float"
    case LongType => "long"
    case DoubleType => "double"
  }

  def compareDataType(configDataType:List[String], fileElement:String, counter:Int): String ={

    if(configDataType(counter).equalsIgnoreCase(fileElement)){
      return "true"
    }
    else{
      return "false"
    }
  }

  def writeIntoValidHiveTable(spark:SparkSession, df:DataFrame): Unit ={
    val validDf = df.drop(df.col("row_num"))
    validDf.createOrReplaceTempView("temptable")
    spark.sql("Insert into SchemaValidation.hitwise_validrecords select * from temptable")
  }

  def writeIntoInvalidHiveTable(spark:SparkSession, df:DataFrame): Unit ={
    val inValidDf = df.drop(df.col("row_num"))
    inValidDf.createOrReplaceTempView("temptable")
    spark.sql("Insert into SchemaValidation.hitwise_invalidrecords select * from temptable")
  }

}
