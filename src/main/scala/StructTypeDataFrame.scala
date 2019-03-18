import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object StructTypeDataFrame {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Dataframe test").setMaster("local")
    val spark = SparkSession.builder()
      .config(conf)
      .config("spark.sql.warehouse.dir", "/Users/amaraj0/intellijSbt/SparkScala1/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val questionSchema = new StructType()
      .add("Id",IntegerType,true)
      .add("CreationDate",StringType,true)
      .add("ClosedDate",StringType,true)
      .add("DeletionDate",StringType,true)
      .add("Score",IntegerType,true)
      .add("OwnerUserId",StringType,true)
      .add("AnswerCount",IntegerType,true)

    val questionFile =  spark.read.schema(questionSchema).csv("/Users/amaraj0/Documents/MyData/questions_dataset/question_no_header.csv");
    questionFile.show()

  }
}
