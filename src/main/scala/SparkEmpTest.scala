import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object SparkEmpTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Emp File").setMaster("local")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val file = spark.read.option("header","true").option("inferSchema","true").csv("/Users/amaraj0/Documents/MyData/file_test.txt")

    val schema = Seq("EmpId","MgrId","EmpName","EmpId_1","MgrId_2","EmpName_2")
    val schema2 = new StructType().add("Emp",IntegerType,true)
    val fileJoin = file.as("file1").join(file.as("file2"),$"file1.EmpId" === $"file2.MgrId","inner").toDF(schema:_*)
    fileJoin.withColumn("MgrName", when(col("EmpId") === col("MgrId_2"), col("EmpName"))).show()

  }

}
