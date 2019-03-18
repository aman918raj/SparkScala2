import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession;

object SparkDataFrameTest {

  def main(args:Array[String]): Unit ={

    val conf = new SparkConf().setAppName("Dataframe test").setMaster("local")
    val spark = SparkSession.builder()
      .config(conf)
      .config("spark.sql.warehouse.dir", "/Users/amaraj0/intellijSbt/SparkScala1/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()
   // readQuestionTagFile(spark,args)
    //readQuestionFile(spark)
    //readXmlFile(spark)
    //useHive(spark)
    joinCol(spark)

  }

  def readQuestionTagFile(spark: SparkSession, args:Array[String]): Unit ={
    val questionTag = spark.read.option("header","true").option("inferSchema", "true").csv("/Users/amaraj0/Documents/MyData/questions_dataset/question_tags.csv");
 //   val questionTag = spark.read.option("header","true").option("inferSchema", "true").csv(args(1));
 //   questionTag.write.csv(args(2))
    //option("inferSchema", "true") => automatically selets the data type of schema // for `when`
    import spark.implicits._ // for `toDF` and $""
    import org.apache.spark.sql.functions._ // for `when`
    questionTag.withColumn("new", when(col("Id") === 4, "test").otherwise("testFail")).show()
    questionTag.createOrReplaceTempView("questionTagSql")
    spark.sql("select Id, min(rank) from (select Id,Tag,rank() over(order by Tag) as rank from questionTagSql) group by Id").show()

    questionTag.printSchema()
    questionTag.select("Id","Tag").show()
    questionTag.filter("Tag == 'decimal'").show()
    questionTag.filter("Tag like 'w%'").show()
    questionTag.groupBy("Id").count().show()
    questionTag.groupBy("Id").count().filter("count > 1").show()
    questionTag.groupBy("Id").count().filter("count > 1").orderBy("Id").show()
  }

  def readQuestionFile(spark: SparkSession): Unit ={
    val question = spark.read.option("header","true").option("inferSchema", "true").csv("/Users/amaraj0/Documents/MyData/questions_dataset/question.csv");
    question.printSchema()
    val castQuestion = question.select(
      question.col("Id").cast("integer"),
      question.col("CreationDate").cast("timestamp"),
      question.col("ClosedDate").cast("timestamp"),
      question.col("DeletionDate").cast("date"),
      question.col("Score").cast("integer"),
      question.col("OwnerUserId").cast("integer"),
      question.col("AnswerCount").cast("integer")
    ).printSchema()

  }

  def readXmlFile(spark: SparkSession): Unit ={
    val file = spark.read.format("com.databricks.spark.xml").option("rowTag", "name").load("/Users/amaraj0/Documents/MyData/questions_dataset/file.xml")

  }

  def useHive(spark: SparkSession): Unit ={
    spark.sql("drop table retail_db.order_full")
  //  spark.sql("create database retail_db")
    spark.sql("create table if not exists retail_db.order_full (orderId Int, orderDate String, custId Int, status String) " +
      "row format delimited fields terminated by ',' stored as textfile")
    spark.sql("load data local inpath '/Users/amaraj0/data-master/retail_db/orders' into table retail_db.order_full")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("create table if not exists retail_db.order_partition (orderId Int, orderDate String, custId Int) " +
      " partitioned by (status String)")
    spark.sql("Insert into table retail_db.order_partition partition(status) select * from retail_db.order_full")

  }

  def joinCol(spark: SparkSession): Unit ={
    val questionTag = spark.read.option("header","true").option("inferSchema", "true").csv("/Users/amaraj0/Documents/MyData/questions_dataset/question_tags.csv");
    val question = spark.read.option("header","true").option("inferSchema", "true").csv("/Users/amaraj0/Documents/MyData/questions_dataset/question.csv");
    questionTag.join(question, "Id").show()
    println("inner join")

    questionTag.join(question,Seq("Id"),"FullOuter").show()
    println("FullOuter")

    questionTag.join(question,Seq("Id"),"Outer").show()
    println("outer")

    questionTag.join(question, Seq("Id"), "left").show()
    println("leftjoin")

    questionTag.join(question, Seq("Id"), "leftsemi").show()
    println("leftsemi")

    questionTag.join(question, Seq("Id"), "leftouter").show()
    println("leftouter")

    questionTag.join(question, Seq("Id"), "leftanti").show()
    println("leftanti")

  }

}
