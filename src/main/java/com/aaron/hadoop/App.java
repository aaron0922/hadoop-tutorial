package com.aaron.hadoop;

import com.aaron.hadoop.tutorial.HBase;
import com.aaron.hadoop.tutorial.Hive;
import com.aaron.hadoop.tutorial.Kafka;
import com.aaron.hadoop.tutorial.Phoenix;

import java.sql.ResultSet;

public class App {
    public static void testHive() {
        try {
            // init hive connection
            Hive hive = new Hive();
            hive.init();
            hive.showDatabases();
            hive.selectDatabase("test");
//            hive.showTables();

            // write hive data
            String create_table_sql = "CREATE TABLE IF NOT EXISTS hive_test (\n" +
                    "id int,\n" +
                    "firstname string,\n" +
                    "lastname string\n" +
                    ")\n" +
                    "STORED AS ORC";
            hive.execute(create_table_sql);

            hive.execute("INSERT INTO table hive_test (id, firstname, lastname) VALUES (1, 'Tony', 'Stark')");
            hive.execute("INSERT INTO table hive_test (id, firstname, lastname) VALUES (2, 'Peter', 'Park')");
            hive.countData("hive_test");

            // read hive data
            String select_sql = "SELECT * from hive_test";
            ResultSet rs = hive.selectData(select_sql);
            System.out.println("id" + "\t\t" + "firstname" + "\t\t" + "lastname");
            while (rs.next()) {
                System.out.println(rs.getString("id") + "\t\t" + rs.getString("firstname") + "\t\t" + rs.getString("lastname"));
            }

            // close hive connection
            String truncate_sql = "TRUNCATE TABLE hive_test";
            hive.execute(truncate_sql);
            hive.destory();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void testKafka() {
        try {
            Kafka kafka = new Kafka();
            kafka.init();
            kafka.produce("kafka_test", "Record");
            kafka.consume("kafka_test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void testPhoenix() {
        try {
            Phoenix phoenix = new Phoenix();
            phoenix.init();
//            String create_table_sql = "create table phoenix_test2 (mykey integer not null primary key, mycolumn varchar )";
//            phoenix.createTable(create_table_sql);

//            ArrayList sql = new ArrayList();
//            sql.add("upsert into phoenix_test values(3, 'peter')");
//            sql.add("upsert into phoenix_test values(4, 'park')");
//            phoenix.upsert(sql);

            phoenix.query("select * from phoenix_test");
//            phoenix.delete("delete from phoenix_test where mykey = 3");
            phoenix.destory();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void testHBase() {
        try {
            HBase hbase = new HBase();
            hbase.init();
//            hbase.listTables();
            hbase.isExists("hbase_test");
//            hbase.isExists("atlas_janus");
//            hbase.createTable("hbase_test", "cf1");
//            hbase.listTables();
//            hbase.deleteTable("hbase_test");
//            hbase.putDatas("hbase_test", "cf1");
//
//            String[] column1 = { "name", "nickname" };
//            String[] value1 = { "nicholas", "lee" };
//            hbase.addData("rowkey1", "hbase_test", column1, value1);
//            hbase.addData("rowkey2", "hbase_test", column1, value1);
//            hbase.addData("rowkey3", "hbase_test", column1, value1);

//
//            hbase.scanTable("hbase_test", "cf1");
            hbase.getData("hbase_test", "cf1");
            hbase.destory();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
//        System.out.println("test hive");
//        testHive();
        testKafka();
//        testHBase();
//        testPhoenix();
        System.out.println("finish");
    }
}
