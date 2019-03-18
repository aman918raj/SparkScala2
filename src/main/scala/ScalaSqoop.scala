import org.apache.sqoop.SqoopOptions

object ScalaSqoop {

  def main(args:Array[String]): Unit ={

    val options = new SqoopOptions()
    options.setConnectString("jdbc:sqlserver://localhost:1433;databaseName=mydb")
    options.setUsername("SA")
    options.setPassword("bmcAdm1n")
    options.setNumMappers(1)
    options.setSqlQuery("select * from mydb.dbo.application_test")
    options.setTargetDir("/Users/amaraj0/Documents/MyData/sqlserver")
    options.doHiveImport()
 //   val ret = options.
   // println(result)
  }

}
