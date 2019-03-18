package HomeBudget

import java.util
import java.util.StringJoiner

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, lit}

object SparkHomeBudget {

  def main(args:Array[String]): Unit ={

    val conf = new SparkConf().setAppName("Dataframe test").setMaster("local")
    val spark = SparkSession.builder()
      .config(conf)
      .config("spark.sql.warehouse.dir", "/Users/amaraj0/intellijSbt/SparkScala1/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val inputFile = "/Users/amaraj0/Documents/MyData/budget/budget.xlsx";
    val finalVal = readFile(spark, inputFile)
 //   SwingExample.createUI(finalVal._1,finalVal._2)
  //  SwingExample.createUI(finalVal)
 //   println(finalVal)
    val javaHtmlWriter = new JavaHtmlWriter()
    javaHtmlWriter.createUI(finalVal._2)
  }

  /*def createConf(): (String, String) ={
    val conf = new SparkConf().setAppName("Dataframe test").setMaster("local")
    val spark = SparkSession.builder()
      .config(conf)
      .config("spark.sql.warehouse.dir", "/Users/amaraj0/intellijSbt/SparkScala1/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val inputFile = "/Users/amaraj0/Documents/MyData/budget/budget.xlsx";
    val finalVal = readFile(spark, inputFile)
    println(finalVal)
    return finalVal
  }*/


  def readFile(spark:SparkSession, inputFile:String): (String,util.ArrayList[String])= {

    val colNames = Seq("Saransh","Aman","Rahul")
   val excelFile = spark.read.format("com.crealytics.spark.excel").option("location", inputFile).option("useHeader","true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema","true")
      .option("sheetName","Sheet1")
      .option("addColorColumns", "False").load()

    excelFile.printSchema()

    val sumOfEachCol = excelFile.select("*").groupBy().sum()
    val sumOfEachColDF = sumOfEachCol.toDF(colNames:_*)
    val sumOfEachRow = sumOfEachColDF.select("*").withColumn("Total",expr("Saransh + Aman + Rahul"))
      .withColumn("TotalMembers", lit(colNames.size).cast("double"))
    val totalPerHead = sumOfEachRow.select("*").withColumn("PerHead", col("Total").divide(col("TotalMembers")))

    val gainLoss = colNames.toList.foldLeft(totalPerHead)((df,value) => df.withColumn(value+"-p",col("perHead").-(col(value))))
    gainLoss.show()
    val schemaName = new StringJoiner("   ")
    val values = new util.ArrayList[String]()
    gainLoss.schema.fieldNames.toStream.foreach(x => schemaName.add(x))
    gainLoss.take(1)(0).toSeq.toStream.foreach(x => values.add(x.toString))
    val schemaNames = schemaName.toString
    println(values)
    return (schemaNames , values)
 //   gainLoss.take(1).foreach(x => println(x.toSeq.toList))
  //  gainLoss.schema.fieldNames.foreach(x => println(x))
  }
}
