package com.LogKafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerLogs {

    public void logMessage(String message, String jobId){

        String topicName = jobId;

        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092");
        prop.put("ack","all");
        prop.put("retries",0);
        prop.put("batch.size",16384);
        prop.put("linger.ms",1);
        prop.put("buffer.memory",33554432);
        prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        Producer<String,String> producer = new KafkaProducer<String,String>(prop);

        producer.send(new ProducerRecord<String,String>(topicName, jobId, message));
        System.out.println(message + " : Message sent successfully");
        KafkaConsumerLogs kafkaConsumerLogs = new KafkaConsumerLogs();
    }
}
