package com.schemaValidation

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadConfig {
/*
  def main(args:Array[String]): Unit ={

    val conf = new SparkConf().setAppName("hive test").setMaster("local")
    val spark = SparkSession
      .builder()
      .config(conf)
      .config("spark.sql.warehouse.dir", "/Users/amaraj0/intellijSbt/SparkScala1/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

    getConfig(spark)
  }*/

  def readFile(spark:SparkSession, configFile:String): DataFrame ={
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val config = spark
      .read
      .option("header","true")
      .option("inferSchema","true")
      .json(configFile)

    val configColumns = config.select(explode(col("files.records.schema.columns")) as "columns")
    val colNames = configColumns.select(explode(col("columns.original_name")) as "columnNames").withColumn("id",monotonically_increasing_id())
    val colDataType = configColumns.select(explode(col("columns.dataType")) as "dataType").withColumn("id1",monotonically_increasing_id())
    val colNamesDataTypeId = colNames.join(colDataType,colDataType.col("id1")=== colNames.col("id"), "inner")
    val colNamesDataType = colNamesDataTypeId.select("columnNames", "dataType")
    colNamesDataType
  //  colNames.write.json("/Users/amaraj0/Documents/MyData/SchemaValidation/out")
  }

  def getConfig(spark:SparkSession, configFile:String): (List[String],List[String]) ={

    val colNamesDataType = readFile(spark,configFile)

    val n = colNamesDataType.count().toInt

    var colNameList = List[String]()
    var colDataType = List[String]()

    for(i <- 1 to n ){

      val colConfigNames = colNamesDataType.select("columnNames").take(i).drop(i-1).map(row => row.mkString.replaceAll(" ","_"))
      val colConfigDataType = colNamesDataType.select("dataType").take(i).drop(i-1).map(row => row.mkString.replaceAll(" ","_"))

      colNameList = colConfigNames(0) :: colNameList
      colDataType = colConfigDataType(0) :: colDataType
    }
    val colNameDataType = (colNameList.reverse,colDataType.reverse)
    colNameDataType
  }

}
