package com.aaron.hadoop.tutorial;

import com.aaron.hadoop.lib.Hive;

import java.sql.ResultSet;

public class HiveDemo {
    public static void testHive() {
        System.out.println("hive demo");
        try {
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
}
