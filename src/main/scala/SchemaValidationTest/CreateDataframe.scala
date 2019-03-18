package SchemaValidationTest

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object CreateDataframe {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Create Dataframe").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

  }

}
