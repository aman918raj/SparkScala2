import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.lit

object DataframeTransactional {

  def dfImplementation(spark: SparkSession, lastMonth:String, currMonth:String, deltaFile:String){
    import spark.implicits._
    val lastMonthFile = spark.read.option("inferSchema","true").csv(lastMonth)
    val colNames1 = Seq("id","company","revenue")
    val lastMonthFileDF = lastMonthFile.toDF(colNames1:_*)

    val list:List[String] = List("a","b","c")
    list.toDF
    val currMonthFile = spark.read.option("inferSchema","true").csv(currMonth)
    val colNames2 = Seq("id","company","revenue")
    val currMonthFileDF = currMonthFile.toDF(colNames2:_*)
    addDF(lastMonthFileDF, currMonthFileDF)
    changeDF(lastMonthFileDF, currMonthFileDF)
  }

  def addDF(lastMonthFileDF: org.apache.spark.sql.DataFrame, currMonthFileDF: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame ={

    val currLastJoin = currMonthFileDF.join(lastMonthFileDF,Seq("Id"),"leftanti")
    val finalAdds = currLastJoin.withColumn("Delta", lit("A"))
    finalAdds
  }

  def changeDF(lastMonthFileDF: org.apache.spark.sql.DataFrame, currMonthFileDF: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame ={

    val currLastJoin = currMonthFileDF.join(lastMonthFileDF,
      currMonthFileDF.col("Id") === lastMonthFileDF.col("Id"),"inner")
      .where((currMonthFileDF.col("company") !== lastMonthFileDF.col("company"))
      || (currMonthFileDF.col("revenue") !== lastMonthFileDF.col("revenue")))
    val finalChange = currLastJoin.withColumn("Delta", lit("C"))
    val finalChangeCol = currLastJoin.select(currLastJoin.col("Id"),currLastJoin.col("company"),currLastJoin.col("revenue"))
    finalChangeCol.show()
    finalChangeCol
  }

}
