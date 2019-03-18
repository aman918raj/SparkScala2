package com.fileValidation

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ReadConfig {

  def main(args:Array[String]): Unit ={

    val conf = new SparkConf().setAppName("File Validation").setMaster("local")
    val spark = SparkSession.builder().config(conf).config("spark.sql.warehouse.dir", "/Users/amaraj0/intellijSbt/SparkScala1/spark-warehouse").enableHiveSupport().getOrCreate()

    val inputFile = "/Users/amaraj0/Documents/MyData/SchemaValidation/Hitwise_webtraffic_product-level_demographic-permutations_20180317_20180318_0651.csv";
    val configFile = "/Users/amaraj0/Documents/MyData/SchemaValidation/config.json";
    readFile(spark,configFile)
  }

  def readFile(spark:SparkSession,configFile:String): (Int, DataFrame) ={



    val file = spark.read.option("header","true").json(configFile)
    file.printSchema()
    validationRules(file)
    val getHeaderRowCount = file.select(explode(col("files.fileDefinition.fileHeader.rowsCount"))as "hdrCount")
      .collect().map(row => row.toString())
    val hdrRowCount = getHeaderRowCount(0).replace("[","").replace("]","").toInt
    val getColHdrNames = file.select(explode(col("files.fileDefinition.fileHeader.requiredColumnHeaders"))as "colName")
    val configColNames = getColHdrNames.select(explode(col("colName")) as "columnNames")
    configColNames.show()
    (hdrRowCount, configColNames)
  }

  def validationRules(file:DataFrame): Unit ={

    val fileValidation = file.select(explode(col("files.fileValidation.rules")) as "rules")
    val rulesDF = fileValidation.select(explode(col("rules.cdapRuleType")) as "rules")
    val rulesMap:Map[String,String] = Map()
    var rulesMap2:Map[String,String] = Map()
  //  rulesDF.select("rules").collect().foreach(x => (rulesMap2.+(getRules(x(0).toString()))))

    rulesDF.show()
    println(rulesMap2)
  }

  def getRules(rulesType:String): Map[String,String]  = rulesType match {

    case "BR_FILENAME_CHECK" => Map("FILENAME" -> "True")
    case "BR_VALIDATE_RECORD_COUNT" => Map("RECORD_COUNT" -> "True")
    case "BR_VALIDATE_COLUMN_HEADER" => Map("COLUMN_HEADER" -> "True")

  }

}
