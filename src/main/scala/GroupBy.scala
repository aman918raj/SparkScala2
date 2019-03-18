import org.apache.spark.{SparkConf, SparkContext}

object GroupBy {
  def main(args:Array[String]): Unit = {

 //   val appConf = ConfigFactory.load()
    /*val conf = {
      new SparkConf(conf).setAppName("Group").setMaster(appConf.getString("local"))
    }*/
    val sc = new SparkContext(new SparkConf().setAppName("Group").setMaster("local"))

    val file = sc.textFile("/Users/amaraj0/Documents/MyData/test1.csv")
    val header = file.first()
    val fileFilter = file.filter(x => x != header)
    val fileMap = fileFilter.map(x => (x.split(",")(1), x.split(",")(0).toInt))
    val fileGroup = fileMap.groupByKey()
    val fileGroupMap = fileGroup.map(x => (x._1,x._2.toList))

    def sum(list: List[Int]): Int = {
      var total = 0
      for (i <- 0 to list.size - 1) {
        total += list(i)
      }
      total
    }
    val fileGroupSum = fileGroupMap.map(x => (x._1, sum(x._2)))
    fileGroupSum.collect.foreach(println)
  }

}
