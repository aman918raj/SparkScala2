import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkDataFrame {

  def main(args:Array[String]): Unit ={

    val conf = new SparkConf().setAppName("hive test").setMaster("local")
    val spark = SparkSession
      .builder()
      .config(conf)
      .config("spark.sql.warehouse.dir", "/Users/amaraj0/intellijSbt/SparkScala1/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val sc = new SparkContext(conf)

    val file = spark.read.option("header","true").option("inferSchema","true").csv("/Users/amaraj0/Documents/MyData/questions_dataset/question.csv")
    file.show()

    val colNames = Seq("Id","ScoreSum","AnserCountSum")
    file.groupBy("Id").sum("Score","AnswerCount").toDF(colNames:_*).where("ScoreSum > 1").show()
    file.groupBy("Id").count().show()

    spark.stop()

    //the below line will not be implemented as spark session has been stopped
  //  sc.textFile("/Users/amaraj0/Documents/MyData/questions_dataset/question.csv").collect().foreach(println)

  }
}
