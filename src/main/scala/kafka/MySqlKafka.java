package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class MySqlKafka {

    public static void main(String[] args){

        String topicName = "mysql_test";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("ack","all");
        props.put("retries",1);
        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String,String> producer = new KafkaProducer<String, String>(props);

        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb","root","sapient@123");
            Statement st = con.createStatement();
            ResultSet rs = st.executeQuery("select * from test");
            while (rs.next()){
                producer.send(new ProducerRecord<String, String>(topicName,String.valueOf(rs.getInt("id")),rs.getString("name")));
                System.out.println(rs.getInt("id")+","+rs.getString("name"));
            }
            con.close();
        }catch (Exception e){
            System.out.println(e);
        }
    }
}
