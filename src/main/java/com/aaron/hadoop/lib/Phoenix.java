package com.aaron.hadoop.lib;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

public class Phoenix {
    private static final String DRIVER_NAME = "org.apache.phoenix.jdbc.PhoenixDriver";
    //    private static final String DRIVER_NAME = "org.apache.phoenix.queryserver.client.Driver"; //thin
    private static String URL;
    private static String USER;
    private static String PASSWORD;
    private Connection conn;
    private Statement stat;
    private ResultSet rs;

    public void init() throws Exception {
        Properties prop = new Properties();
        prop.load(getClass().getClassLoader().getResourceAsStream("config.properties"));
        URL = prop.getProperty("phoenix_url");
        USER = prop.getProperty("phoenix_user");
        PASSWORD = prop.getProperty("phoenix_password");
        Class.forName(DRIVER_NAME);
        conn = DriverManager.getConnection(URL, USER, PASSWORD);
        conn.setAutoCommit(true);
        stat = conn.createStatement();
    }
    public void createTable(String sql) throws SQLException {
        stat.executeUpdate(sql);
        conn.commit();
    }
    public void query(String sql) throws SQLException {
        rs = stat.executeQuery(sql);
        System.out.println("mykey" + "\t" + "mycolumn");
        while (rs.next()) {
            System.out.println(rs.getString("mykey") + "\t\t" + rs.getString("mycolumn"));
        }
    }
    public void upsert(ArrayList sqlList) throws SQLException {
        Iterator it = sqlList.iterator();
        String sql = null;
        while(it.hasNext()) {
            sql = it.next().toString();
            stat.executeUpdate(sql);
        }
        conn.commit();
    }
    public void delete(String sql) throws SQLException {
        stat.executeUpdate(sql);
        conn.commit();
    }
    public void destory() throws SQLException {
        if (rs != null) {
            rs.close();
        }
        if (stat != null) {
            stat.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
