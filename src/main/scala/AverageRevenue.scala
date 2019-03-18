import org.apache.spark.{SparkConf, SparkContext}

object AverageRevenue {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Avg revenue").setMaster("local")
    val sc = new SparkContext(conf)

    val order = sc.textFile("/Users/amaraj0/Downloads/data-master/retail_db/orders",2)
    val orderMap = order.map(x => (x.split(",")(0).toInt,x.split(",")(3)))
    val orderFilter = orderMap.filter(x => (x._2.equals("COMPLETE")))

    val orderItems = sc.textFile("/Users/amaraj0/Downloads/data-master/retail_db/order_items")
    val orderItemsMap = orderItems.map(x => (x.split(",")(1).toInt,x.split(",")(4).toDouble))
    val orderItemsReduce = orderItemsMap.reduceByKey((acc,value) => acc + value)

    val ordersJoin = orderFilter.join(orderItemsReduce).map(x => ((x._1 , x._2._1),(x._2._2)))

    val orderAggregate = ordersJoin.aggregateByKey((0.0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (total1, total2) => (total1._1 + total2._1, total1._2 + total2._2)
    )

    val avg = orderAggregate.map(x => (x._1, x._2._1/x._2._2))

    avg.take(10).foreach(println)

  }

}
