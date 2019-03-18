import org.apache.spark.{SparkConf, SparkContext}

object CaseClassRDD {

  case class FileHeader(Id:Int,CreationDate:String,ClosedDate:String,DeletionDate:String,Score:String,OwnerUserId:String,AnswerCount:String)
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("test rdd case class").setMaster("local")
    val sc = new SparkContext(conf)

    val questionFile = sc.textFile("/Users/amaraj0/Documents/MyData/questions_dataset/question_no_header.csv")

    val questionMap = questionFile.map(x => FileHeader(x.split(",")(0).toInt,x.split(",")(1),x.split(",")(2),x.split(",")(3),
      x.split(",")(4),x.split(",")(5),x.split(",")(6)))
    val questionFilter = questionMap.filter(x => x.AnswerCount != 10)
    questionMap.collect().foreach(println)

  }

}
