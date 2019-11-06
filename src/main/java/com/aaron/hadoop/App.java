package com.aaron.hadoop;

import com.aaron.hadoop.tutorial.HBaseDemo;
import com.aaron.hadoop.tutorial.HiveDemo;
import com.aaron.hadoop.tutorial.KafkaDemo;
import com.aaron.hadoop.tutorial.PhoenixDemo;

public class App {
    public static void main(String[] args) {
        HiveDemo.testHive();
        KafkaDemo.testKafka();
        HBaseDemo.testHBase();
        PhoenixDemo.testPhoenix();
        System.out.println("finish");
    }
}
