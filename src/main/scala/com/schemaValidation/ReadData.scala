package com.schemaValidation

import com.schemaValidation.ReadConfig.readFile
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object ReadData {

  var i = 0
  /*def main(args:Array[String]): Unit ={

    val conf = new SparkConf().setAppName("hive test").setMaster("local")
    val spark = SparkSession
      .builder()
      .config(conf)
      .config("spark.sql.warehouse.dir", "/Users/amaraj0/intellijSbt/SparkScala1/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val sc = new SparkContext(conf)

    readFile(spark)
  }*/

  def readFile(spark:SparkSession, dataFile: String, configFile:String): Unit ={

    import org.apache.spark.sql.functions._
    import spark.implicits._
    val file = spark
      .read
      .option("header","true")
      .option("inferSchema","true")
      .csv(dataFile)
    
    val configColNameDataType = ReadConfig.getConfig(spark,configFile)
    val configColName = configColNameDataType._1
    val configDataType = configColNameDataType._2
    val newHeaderFile = file.toDF(configColName:_*)

     val  newFile = configColName
         .foldLeft(newHeaderFile)((df, name) => df.withColumn(name, col(name)
           .cast(getType(getTypeFromList(configDataType)))))

    SchemaValidate.validate(spark, newFile, configDataType, configColName)

  }

  def getType(configDataType:String): org.apache.spark.sql.types.DataType = configDataType match {

    case "string" => StringType
    case "int" => IntegerType
    case "long" => LongType
    case "boolean" => BooleanType
    case "byte" => ByteType
    case "double" => DoubleType
    case "float" => FloatType
  }

  def getTypeFromList(configDataType:List[String]): String ={

    val returnType = configDataType(i)
    i += 1
    returnType
  }

}
