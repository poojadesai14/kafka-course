package com.github.simplepooja.kafka.tutorial1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProducerDemoWithCallBack {
    public static void main(String[] args) {
        log.info("Kafka project starts");
        String bootStrapServers = "127.0.0.1:9092";
//        create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

//        create producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
//        create ProducerRecord
       for(int i=0 ;i<10 ;i++) {
           ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world!"
           + Integer.toString(i));

//        send data--asynchronous
           producer.send(record, new Callback() {
               @Override
               public void onCompletion(RecordMetadata metadata, Exception exception) {
                   //executes when a record is successfully sent or an exception is thrown
                   if (exception == null) {
                       log.info("Received new metadata.\n" +
                               "Topic: " + metadata.topic() + "\n" +
                               "Partition: " + metadata.offset() + "\n" +
                               "Offset: " + metadata.offset() + "\n" +
                               "Timestamp: " + metadata.timestamp());

                   } else {
                       log.error("Error while producing", exception);
                   }

               }

           });
       }
//        flush the data
        producer.flush();
//        flush and close producer
        producer.close();



    }
}
