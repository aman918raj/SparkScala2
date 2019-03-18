package kafka

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkStreamDF {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Emp File").setMaster("local")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val streamInputDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","mysql_test").load()
    var streamSelectDF = streamInputDF.select("*")
    streamInputDF.show()
  }

}
