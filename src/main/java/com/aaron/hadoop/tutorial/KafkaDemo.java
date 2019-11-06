package com.aaron.hadoop.tutorial;

import com.aaron.hadoop.lib.Kafka;

public class KafkaDemo {
    public static void testKafka() {
        try {
            System.out.println("kafka demo");
            Kafka kafka = new Kafka();
            kafka.init();
            kafka.produce("kafka_test", "Record");
            kafka.consume("kafka_test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
