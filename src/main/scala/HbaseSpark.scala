//import org.apache.hadoop.hbase.HBaseConfiguration

/*object HbaseSpark {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Read Hbase").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    val tableName = "custTable"

    conf.set("hbase.master","localhost:60000")
    conf.setInt("timeout", 120000)
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set(TableInputFor,"")
  }

}*/
