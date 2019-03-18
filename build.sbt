name := "SparkScala2"

version := "0.1"

scalaVersion := "2.11.12"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"

dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0"

libraryDependencies += "org.apache.spark"  %  "spark-sql_2.11" % "2.2.0"

libraryDependencies += "org.apache.spark"  %  "spark-hive_2.11" % "2.2.0"

libraryDependencies += "com.databricks" % "spark-avro_2.11" % "3.2.0"

libraryDependencies += "com.typesafe" % "config" % "1.3.0"

libraryDependencies += "com.databricks" % "spark-xml_2.11" % "0.4.1"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.1.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.3"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.3"

libraryDependencies += "org.apache.hbase" % "hbase-mapreduce" % "2.0.0"

libraryDependencies += "com.crealytics" %% "spark-excel" % "0.8.2"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.3"

libraryDependencies += "org.scala-lang" % "scala-swing" % "2.11.0-M7"

libraryDependencies += "org.apache.sqoop" % "sqoop" % "1.4.1-incubating"

libraryDependencies += "org.webjars.bower" % "ajax-form" % "2.0.2"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.47"

//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10_2.11" % "2.1.0"

//libraryDependencies += "it.nerdammer.bigdata" % "spark-hbase-connector_2.11" % "1.0.3"
