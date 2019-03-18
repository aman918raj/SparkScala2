package SchemaValidationTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SVValidation {

  def main(args:Array[String]): Unit ={

    val conf = new SparkConf().setAppName("hive test").setMaster("local")
    val spark = SparkSession
      .builder()
      .config(conf)
      .config("spark.sql.warehouse.dir", "/Users/amaraj0/intellijSbt/SparkScala1/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

    spark.read.csv("/Users/amaraj0/Documents/MyData/FileValidation/data/Hitwise_webtraffic_product-level_demographic-permutations_20180317_20180318_0651.csv")
      
  }
}
