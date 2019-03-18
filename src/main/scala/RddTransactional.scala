import TransactionalProcess.Schema
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object RddTransactional {

  def checkPath(fs: FileSystem, path: String):Boolean ={
    val bool = fs.exists(new Path(path))
    bool
  }

  def rddImplementation(sc: SparkContext, lastMonth:String, currMonth:String, deltaFile:String):  RDD[(Int, String, Long, String)] ={

    val lastMonthFile = sc.textFile(lastMonth)
    val lastMonthSplit = lastMonthFile.map(x => x.split(","))
    val lastMonthMap = lastMonthSplit.map(x => (x(0).toInt,(Schema(x(0).toInt, x(1), x(2).toLong))))

    val currMonthFile = sc.textFile(currMonth)
    val currMonthSplit = currMonthFile.map(x => x.split(","))
    val currMonthMap = currMonthSplit.map(x => (x(0).toInt,(Schema(x(0).toInt, x(1), x(2).toLong))))

    val addChangeDelete = addsRdd(lastMonthMap, currMonthMap).union(changesRdd(lastMonthMap, currMonthMap)).
      union(deleteRdd(lastMonthMap,currMonthMap))
    addChangeDelete
  }

  def addsRdd(lastMonthMap: RDD[(Int, Schema)], currMonthMap: RDD[(Int,Schema)]): RDD[(Int, String, Long, String)] ={
    val currLastJoin = currMonthMap.subtractByKey(lastMonthMap)
    val addedRecords = currLastJoin.map(x => x._2)
    currLastJoin.map(x => findType(x._2.id)).collect().foreach(println)
    val finalAdds = addedRecords.map(x => (x.id, x.company, x.revenue, "A"))
    finalAdds
  }

  def changesRdd(lastMonthMap: RDD[(Int, Schema)], currMonthMap: RDD[(Int,Schema)]): RDD[(Int, String, Long, String)] ={
    val currLastJoin = currMonthMap.join(lastMonthMap)
    val changedRecords = currLastJoin.map(x => if((x._2._1.company != x._2._2.company) || (x._2._1.revenue != x._2._2.revenue)){
      x._2._1
    }
    )
    val changedRecordsFilter = changedRecords.filter(x => !x.equals())
    val changedRecordsSchema = changedRecordsFilter.map(x => (x.asInstanceOf[Schema]))
    val finalChanged = changedRecordsSchema.map(x => (x.id, x.company, x.revenue, "C"))
    finalChanged
  }

  def deleteRdd(lastMonthMap: RDD[(Int, Schema)], currMonthMap: RDD[(Int,Schema)]): RDD[(Int, String, Long, String)] ={
    val currLastJoin = lastMonthMap.subtractByKey(currMonthMap)
    val deleteRecords = currLastJoin.map(x => x._2)
    val finaldelete = deleteRecords.map(x => (x.id, x.company, x.revenue, "D"))
    finaldelete
  }

  def findType(x : Any):String = x match {
    case _: Int => "Int"
    case _: String => "String"
    case _: Long => "Long"
  }

}
