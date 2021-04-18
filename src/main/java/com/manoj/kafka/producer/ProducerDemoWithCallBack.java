package com.manoj.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

        String bootStrapServers = "127.0.0.1:9092";

        // Create Producer Properties
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProperties);

        for (int i = 0; i < 10; i++) {

            // Create a Producer Record
            ProducerRecord<String, String> record = new ProducerRecord<>("First-topic", "Hello World!! This is a message from Java Producer-" + Integer.toString(i));

            //send data - asynchronous
            producer.send(record, (recordMetadata, e) -> {
                // This method is executed on successful producing the message or if an exception is thrown
                if (e == null) {
                    logger.info("Topic is: " + recordMetadata.topic() + "\n" +
                            "partition to which message is published: " + recordMetadata.partition() + "\n" +
                            "offset value in that partition: " + recordMetadata.offset() + "\n" +
                            "Timestamp when published: " + recordMetadata.timestamp());
                } else {
                    logger.error("Exception occured while producing", e);
                }
            });
        }
            //flush data
            producer.flush();

            // close producer
            producer.close();
        }

}
