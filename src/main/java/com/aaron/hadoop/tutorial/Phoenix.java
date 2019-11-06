package com.aaron.hadoop.tutorial;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;

public class Phoenix {
    private static final String DRIVER_NAME = "org.apache.phoenix.jdbc.PhoenixDriver";
    //    private static final String DRIVER_NAME = "org.apache.phoenix.queryserver.client.Driver"; //thin
    private static final String URL = "jdbc:phoenix:twtpedmp01.delta.corp,twtpedmp02.delta.corp,twtpedmp03.delta.corp";
    //    private static final String URL = "jdbc:phoenix:twtpedmp01.delta.corp,twtpedmp02.delta.corp,twtpedmp03.delta.corp:8083";
//    private static final String URL = "jdbc:phoenix:thin:url=http://twtpedmp01.delta.corp,twtpedmp02.delta.corp,twtpedmp03.delta.corp:8083";
    private static final String USER = "d2map";
    private static final String PASSWORD = "d2map";

    private Connection conn;
    private Statement stat;
    private ResultSet rs;

    public void init() throws Exception {
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
