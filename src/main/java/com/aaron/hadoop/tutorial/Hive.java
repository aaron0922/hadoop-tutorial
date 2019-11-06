package com.aaron.hadoop.tutorial;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Logger;

public class Hive {
    private static final Logger LOG = Logger.getLogger("com.delta.drc.tutorial.Hive");
    private static final String DRIVER_NAME  = "org.apache.hive.jdbc.HiveDriver";
    private static final String URL = "jdbc:hive2://10.136.154.193:9996/test";
    private static final String USER = "d2map";
    private static final String PASSWORD = "d2map";

    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;

    public void init() throws Exception {
        LOG.info("init connection");
        Class.forName(DRIVER_NAME);
        conn = DriverManager.getConnection(URL, USER, PASSWORD);
        stmt = conn.createStatement();
    }

    public void showDatabases() throws Exception {
        String sql = "show databases";
        LOG.info("show databases");
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }
    public void showTables() throws Exception {
        String sql = "show tables";
        LOG.info("show tables");
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }
    public void selectDatabase(String db) throws Exception {
        String sql = "use " + db;
        LOG.info("use " + db);
        stmt.execute(sql);
    }
    public void createDatabase(String db) throws Exception {
        String sql = "create database " + db;
        LOG.info("Running: " + sql);
        stmt.execute(sql);
    }


    public void descTable(String table) throws Exception {
        String sql = "desc " + table;
        LOG.info("describe " + table);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1) + "\t" + rs.getString(2));
        }
    }

    public void execute(String sql) throws Exception {
        LOG.info("execute sql");
        stmt.execute(sql);
    }

    public void loadData(String sql) throws Exception {
//        String filePath = "/home/hadoop/data/emp.txt";
//        String sql = "load data local inpath '" + filePath + "' overwrite into table emp";
        LOG.info("load data");
        stmt.execute(sql);
    }

    public ResultSet selectData(String sql) throws Exception {
        LOG.info("select data");
        rs = stmt.executeQuery(sql);
        return rs;
    }

    public void countData(String table) throws Exception {
        String sql = "select count(1) from " + table;
        LOG.info("count data");
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getInt(1) );
        }
    }
    public void dropTable(String table) throws Exception {
        String sql = "drop table if exists " + table;
        LOG.info("drop table");
        stmt.execute(sql);
    }

    public void dropDatabase(String db) throws Exception {
        String sql = "drop database if exists " + db;
        LOG.info("drop database");
        stmt.execute(sql);
    }
    public void destory() throws Exception {
        LOG.info("connection close");
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
