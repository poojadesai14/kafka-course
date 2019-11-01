package com.github.simplepooja.kafka.tutorial1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
@Slf4j
public class ConsumerDemo {
    public static void main(String[] args) {
        String bootStrapServers = "127.0.0.1:9092";
        String groupId = "my-third-application";

//        create Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


//        create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
//        subscribe consumer to topic
        consumer.subscribe(Arrays.asList("first_topic"));

        while (true) {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record: records) {
                log.info("Key: "+record.key() + "Value: "+record.value());
                log.info("Partition: "+record.partition() + "Offsest :" +record.offset());

                
            }
        }




    }
}
