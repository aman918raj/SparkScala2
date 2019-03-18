import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType

object SparkHive {

  def main(args:Array[String]): Unit ={

    val conf = new SparkConf().setAppName("hive test").setMaster("local")
    val spark = SparkSession
      .builder()
      .config(conf)
      .config("spark.sql.warehouse.dir", "/Users/amaraj0/intellijSbt/SparkScala1/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

    hiveTest1(spark)

  }

  def hiveTest1(spark:SparkSession): Unit ={
    val file = spark
      .read
      .option("header","true")
      .option("inferSchema","true")
      .csv("/Users/amaraj0/Documents/MyData/SchemaValidation/Hitwise_webtraffic_product-level_demographic-permutations_20180317_20180318_0651.csv")

    file.createOrReplaceTempView("fileSql")
    file.printSchema()
    val file1 = file.withColumn("Total Visits2",file.col("Total Visits").cast(IntegerType)).show()
 //   spark.sql("select * from fileSql limit 10").show()

  }

}
