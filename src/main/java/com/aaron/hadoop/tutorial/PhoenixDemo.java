package com.aaron.hadoop.tutorial;

import com.aaron.hadoop.lib.Phoenix;

import java.util.ArrayList;

public class PhoenixDemo {
    public static void testPhoenix() {
        System.out.println("phoenix demo");
        try {
            Phoenix phoenix = new Phoenix();
            phoenix.init();
//            String create_table_sql = "create table phoenix_test2 (mykey integer not null primary key, mycolumn varchar )";
//            phoenix.createTable(create_table_sql);

            ArrayList sql = new ArrayList();
            sql.add("upsert into phoenix_test values(3, 'peter')");
            sql.add("upsert into phoenix_test values(4, 'park')");
            phoenix.upsert(sql);

            phoenix.query("select * from phoenix_test");
//            phoenix.delete("delete from phoenix_test where mykey = 3");
            phoenix.destory();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
