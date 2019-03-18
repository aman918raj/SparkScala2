package com.fileValidation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadData {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("File Validation").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .config(conf)
      .config("spark.sql.warehouse.dir", "/Users/amaraj0/intellijSbt/SparkScala1/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val inputFile = "/Users/amaraj0/Documents/MyData/SchemaValidation/Hitwise_webtraffic_product-level_demographic-permutations_20180317_20180318_0651.csv";
    val configFile = "/Users/amaraj0/Documents/MyData/SchemaValidation/config.json";
    readFile(spark, inputFile, configFile, sc)
  }

  def readFile(spark:SparkSession,inputFile:String, configFile:String, sc: SparkContext): Unit ={

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val getConfigFileData = ReadConfig.readFile(spark,configFile)

    if (getConfigFileData._1 == 0){
      val file = spark.read.csv(inputFile)
    }
    else{
      val file = spark.read.option("inferSchema","true").csv(inputFile)
      val colHdrPosition = getConfigFileData._1
      val hdr = file.take(colHdrPosition).drop(colHdrPosition-1).toSeq.map(row => row.toString())
      val header = file.take(colHdrPosition).drop(colHdrPosition-1)(0).toString
      val headerReplace = header.substring(1,header.length-1)
      val colHeader = (1,headerReplace.split(","))
      val dataHeader = spark.createDataFrame(List(colHeader)).select(explode(col("_2")) as "columnNames")
      dataHeader.show()
      val fileHdr = file.withColumn("uniqueId",monotonically_increasing_id()).filter(col("uniqueId") !== colHdrPosition-1).drop("uniqueId")
      val fileHdrAssign = fileHdr.toDF(headerReplace.split(",").toSeq:_*)
      fileHdrAssign.show(10)
    }

  }

}
