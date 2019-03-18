import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkDataframeGroup {

  def main(args:Array[String]): Unit =
  {

    val conf = new SparkConf().setAppName("Dataframe test").setMaster("local")
    val spark = SparkSession.builder().config(conf).config("spark.sql.warehouse.dir", "/Users/amaraj0/intellijSbt/SparkScala1/spark-warehouse").enableHiveSupport().getOrCreate()

    val file = spark.read.option("header","true").option("inferSchema","true").csv("/Users/amaraj0/Documents/MyData/questions_dataset/question.csv")

    file.groupBy("Id").sum().show()
    println(1)
    file.groupBy("Id").sum("AnswerCount","Score").show()
    println(2)
    //file.groupBy("Id","CreationDate").sum("AnswerCount","Score").show()

    file.groupBy("Id").agg(sum("AnswerCount")).show()
    println(3)
    file.groupBy("Id").sum("AnswerCount").show()
    println(4)
    file.agg(sum("AnswerCount"))
    println(5)
    //file.na.fill(Map("ClosedDate"->"0")).show()
    //file.na.fill(Map("AnswerCount"->0)).show()
    //file.na.fill("0").show()
    //file.select("*").where(col("Id") === 1).show()
    //file.groupBy("Id").sum().show()

  }

}
