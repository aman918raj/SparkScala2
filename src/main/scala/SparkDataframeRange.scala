import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkDataframeRange {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Range function").setMaster("local")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    spark.range(10).withColumnRenamed("id","newId").withColumn("monoId",monotonically_increasing_id() ).show()

  }

}
