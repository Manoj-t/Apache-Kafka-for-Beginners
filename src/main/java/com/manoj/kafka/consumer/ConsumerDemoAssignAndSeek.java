package com.manoj.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class.getName());

        String bootStrapServers = "127.0.0.1:9092";
        String groupId = "my-first-java-consumer-group";
        String topic = "First-topic";

        Properties consumerProperties = new Properties();


        // create consumer properties
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(consumerProperties);

      // Assign the Topic partition
        TopicPartition topicPartition = new TopicPartition("First-topic", 0);
        kafkaConsumer.assign(Arrays.asList(topicPartition));

     // Seek from particular topic, partition and offset
        kafkaConsumer.seek(topicPartition, 15L);

        int numberOfMessagesToRead = 5;
        int numberOfMessagesReadSoFar = 0;
        boolean keepOnReading = true;

        // poll for new data
        while (keepOnReading){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));


            for (ConsumerRecord record : records){
                logger.info("Key: " + record.key() + "\n"
                   + "Value: " + record.value() + "\n"
                   + "Partition: " + record.partition() + "\n"
                   + "Offset in this partition: " + record.offset()
                );
                numberOfMessagesReadSoFar += 1;
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead){
                    keepOnReading = false;
                    break;
                }
            }
        }
    }
}
