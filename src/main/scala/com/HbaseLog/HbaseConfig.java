package com.HbaseLog;/*package com.HbaseLog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;


public class HbaseConfig {


  // public void createConfig(String message, String jobId) throws Exception {
  public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        String path = HbaseConfig.class.getClassLoader().getResource("hbase-site.xml").getPath();
        //config.addResource(new Path(path));
      config.set("fs.defaultFS", "hdfs://127.0.0.1:8020/");
      config.set("hbase.zookeeper.property.clientPort", "2181");
      config.set("hbase.zookeeper.quorum", "sandbox-hdp.hortonworks.com");
      config.set("zookeeper.znode.parent", "/hbase-unsecure");
     // HBaseAdmin.checkHBaseAvailable(config);
      HBaseAdmin hbase = new HBaseAdmin(config);
        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();
        tableConfig(connection);
    }

 //   public void tableConfig(String message,String jobId, Connection connection) throws Exception{
 public static void tableConfig( Connection connection) throws Exception{

        TableName tableName = TableName.valueOf("Table1");
        Table table = connection.getTable(tableName);

        Put p = new Put(Bytes.toBytes("row1"));
        p.add(Bytes.toBytes("details"),Bytes.toBytes("jobId"),Bytes.toBytes("test1"));
        p.add(Bytes.toBytes("details"),Bytes.toBytes("value"),Bytes.toBytes("test2"));

        table.put(p);
        table.close();

    }

}*/
