package com.aaron.hadoop.tutorial;

import com.aaron.hadoop.lib.HBase;

public class HBaseDemo {
    public static void testHBase() {
        System.out.println("hbase demo");
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
}
