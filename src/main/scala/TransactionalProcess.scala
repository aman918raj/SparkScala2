import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object TransactionalProcess {

  def main(args:Array[String]): Unit ={

    val conf = new SparkConf().setAppName("Finding Delta ACD Transcational process").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val lastMonth = "/Users/amaraj0/Documents/MyData/Transactional/M0918.txt"
    val currMonth = "/Users/amaraj0/Documents/MyData/Transactional/M1018.txt"
    val deltaFile = "/Users/amaraj0/Documents/MyData/Transactional/delta"


    val fs = FileSystem.get(sc.hadoopConfiguration)
    val lastMonthPathExists = RddTransactional.checkPath(fs,lastMonth)
    val currMonthPathExists = RddTransactional.checkPath(fs,currMonth)
    val deltaFilePathExists = RddTransactional.checkPath(fs,deltaFile)

    if(!lastMonthPathExists || !currMonthPathExists){
      println("input path does not exists")
      return
    }
    if(deltaFilePathExists){
      fs.delete(new Path(deltaFile), true)
    }

    //val finalDeltaRecords = RddTransactional.rddImplementation(sc, lastMonth, currMonth, deltaFile)
    //finalDeltaRecords.saveAsTextFile(deltaFile)

    DataframeTransactional.dfImplementation(spark, lastMonth, currMonth, deltaFile)

  }

  case class Schema(id:Int, company:String, revenue:Long)

}
