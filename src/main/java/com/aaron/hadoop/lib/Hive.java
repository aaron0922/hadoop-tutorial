package com.aaron.hadoop.lib;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;

public class Hive {
    private static final Logger LOG = Logger.getLogger("com.delta.drc.tutorial.Hive");
    private static final String DRIVER_NAME  = "org.apache.hive.jdbc.HiveDriver";
    private static String URL;
    private static String USER;
    private static String PASSWORD;
    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;

    public void init() throws Exception {
        Properties prop = new Properties();
        prop.load(getClass().getClassLoader().getResourceAsStream("config.properties"));
        URL = prop.getProperty("hive_url");
        USER = prop.getProperty("hive_user");
        PASSWORD = prop.getProperty("hive_password");
        System.out.println("init connection");
        Class.forName(DRIVER_NAME);
        conn = DriverManager.getConnection(URL, USER, PASSWORD);
        stmt = conn.createStatement();
    }

    public void showDatabases() throws Exception {
        String sql = "show databases";
        System.out.println("show databases");
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }
    public void showTables() throws Exception {
        String sql = "show tables";
        System.out.println("show tables:");
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }
    public void selectDatabase(String db) throws Exception {
        String sql = "use " + db;
        stmt.execute(sql);
    }

    public void descTable(String table) throws Exception {
        String sql = "desc " + table;
        System.out.println("describe " + table);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1) + "\t" + rs.getString(2));
        }
    }

    public void execute(String sql) throws Exception {
        stmt.execute(sql);
    }

    public void loadData(String sql) throws Exception {
//        String filePath = "/home/hadoop/data/emp.txt";
//        String sql = "load data local inpath '" + filePath + "' overwrite into table emp";
        System.out.println("load data");
        stmt.execute(sql);
    }

    public ResultSet selectData(String sql) throws Exception {
        System.out.println("select data");
        rs = stmt.executeQuery(sql);
        return rs;
    }

    public void countData(String table) throws Exception {
        String sql = "select count(1) from " + table;
        System.out.println("count data");
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getInt(1) );
        }
    }
    public void dropTable(String table) throws Exception {
        String sql = "drop table if exists " + table;
        System.out.println("drop table");
        stmt.execute(sql);
    }

    public void dropDatabase(String db) throws Exception {
        String sql = "drop database if exists " + db;
        System.out.println("drop database");
        stmt.execute(sql);
    }
    public void destory() throws Exception {
        System.out.println("connection close");
        if ( rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
