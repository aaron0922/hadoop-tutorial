package com.aaron.hadoop.tutorial;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Kafka {
    private static final String BOOTSTRAP_SERVER = "10.136.154.193:8089";
    private Properties config;
    private Producer<String, String> producer = null;
    private KafkaConsumer<String, String> kafkaConsumer = null;

    public void init() throws Exception {
        config = new Properties();
        config.put("bootstrap.servers", BOOTSTRAP_SERVER);
        config.put("acks", "all");
        config.put("retries", 0);
        config.put("batch.size", 16384);
        config.put("linger.ms", 1);
        config.put("buffer.memory", 33554432);
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

//        config.put("group.id", "group-1");
//        config.put("enable.auto.commit", "true");
        config.put("group.id", "group-2");
        config.put("enable.auto.commit", "false");
        config.put("auto.commit.interval.ms", "1000");
        config.put("auto.offset.reset", "earliest");
        config.put("session.timeout.ms", "30000");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }
    public void produce(String topic, String msg) throws Exception {
        try {
            producer = new KafkaProducer<String, String>(config);
            for (int i = 0; i < 3; i++) {
                String record = msg + " " + i;
                producer.send(new ProducerRecord<String, String>(topic, record));
                System.out.println("Sent:" + record);
            }
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            producer.close();
        }
    }
    public void consume(String topic) throws Exception {
        kafkaConsumer = new KafkaConsumer(config);
        kafkaConsumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, value = %s", record.offset(), record.value());
                System.out.println();
            }
        }
    }
}
