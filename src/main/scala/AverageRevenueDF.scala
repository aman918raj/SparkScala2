import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AverageRevenueDF {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Avg revenue").setMaster("local")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val file = spark.read.option("header","false").csv("/Users/amaraj0/Downloads/data-master/retail_db/orders")

    spark.sparkContext.longAccumulator("counter")
  }

}
