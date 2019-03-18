import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}


object DailyRevenue {

  def main(args:Array[String]): Unit ={

 //   val conf = new SparkConf(conf).setAppName("Daily Revenue").setMaster("local")
    val sc = new SparkContext(new SparkConf().setAppName("Daily Revenue").setMaster("local"))

    val inputPath = "/Users/amaraj0/data-master/retail_db"
    val outputPath = "/Users/Documents/amaraj0/BigData/output/DailyRevenue"

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val inputPathExists = fs.exists(new Path(inputPath))
    val outputPathExists = fs.exists(new Path(outputPath))

    if(!inputPathExists){
      println("Input Path does not exist")
      return
    }
    if(outputPathExists){
      fs.delete(new Path(outputPath), true)
    }

    val orders = sc.textFile(inputPath + "/orders")
    var ordersCompletedAccumulator = sc.longAccumulator("Orders Completed")
    var ordersNotCompletedAccumulator = sc.longAccumulator("Orders Not Completed")
    val ordersFiltered = orders.
      filter(order => {
        val isCompletedOrder = order.split(",")(3) == "COMPLETE" || order.split(",")(3) == "CLOSED"
        if(isCompletedOrder) {
          ordersCompletedAccumulator.add(1)
        }
        else{
          ordersNotCompletedAccumulator.add(1)
        }
        isCompletedOrder
      })

    val ordersMap = ordersFiltered.map(x => (x.split(",")(0).toInt, x.split(",")(1)))

    val orderitems = sc.textFile(inputPath + "/order_items")
    val orderitemsMap = orderitems.map(x => (x.split(",")(1).toInt,(x.split(",")(2).toInt , x.split(",")(4).toFloat)))
    val ordersJoin = ordersMap.join(orderitemsMap)
   // val ordersLeftOuterJoin = orderitemsMap.leftOuterJoin(ordersMap)
    val ordersJoinMap = ordersJoin.map(x => (x._2._1 + "," + x._2._2._1.toString , x._2._2._2))
    val ordersReduce = ordersJoinMap.reduceByKey(_+_)
  }
}
