package com.manoj.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

        String bootStrapServers = "127.0.0.1:9092";

        // Create Producer Properties
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProperties);

        for (int i = 0; i < 10; i++) {

            String key = "key-" + Integer.toString(i);

        // Synchronized calls
           /* key-0 - 1
              key-1 - 0
              key-2 - 2
              key-3 - 2
              key-4 - 0
              key-5 - 2
              key-6 - 2
              key-7 - 1
              key-8 - 1
              key-9 - 0
            */

            // Asynchronized calls  - Tested with asynchronous call too. Messages with same key has gone to same partition.
           /* key-0 - 1
              key-1 - 0
              key-2 - 2
              key-3 - 2
              key-4 - 0
              key-5 - 2
              key-6 - 2
              key-7 - 1
              key-8 - 1
              key-9 - 0
            */

            // Create a Producer Record
            ProducerRecord<String, String> record = new ProducerRecord<>("First-topic", key,"Hello World!! This is a message from Java Producer-" + Integer.toString(i));

            //send data - asynchronous
            producer.send(record, (recordMetadata, e) -> {
                // This method is executed on successful producing the message or if an exception is thrown
                if (e == null) {
                    logger.info("Topic is: " + recordMetadata.topic() + "\n" +
                            "partition to which message is published: " + recordMetadata.partition() + "\n" +
                            "offset value in that partition: " + recordMetadata.offset() + "\n" +
                            "Timestamp when published: " + recordMetadata.timestamp() + "\n" +
                            "key-sent " + record.key());
                } else {
                    logger.error("Exception occured while producing", e);
                }
            }); // use get() method to make a synchronized call; DO NOT USE IN PRODUCTION
        }
            //flush data
            producer.flush();

            // close producer
            producer.close();
        }

}
