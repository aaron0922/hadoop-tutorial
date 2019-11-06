package com.aaron.hadoop.lib;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

public class HBase {
    private static final Logger LOG = Logger.getLogger("com.delta.drc.tutorial.Hive");
    private static String ZK_QUORUM;
    private static String ZK_PORT;
    private static String ZK_MASTER;
    private static Connection connection = null;
    private static Admin admin = null;

    public void init() throws Exception {
        Properties prop = new Properties();
        prop.load(getClass().getClassLoader().getResourceAsStream("config.properties"));
        ZK_QUORUM = prop.getProperty("hbase_zk_quorum");
        ZK_PORT = prop.getProperty("hbase_zk_port");
        ZK_MASTER = prop.getProperty("hbase_zk_master");
//        HBaseConfiguration config = new HBaseConfiguration(); //deprecate
        Configuration config = HBaseConfiguration.create();
        config.clear();
        config.set("hbase.zookeeper.quorum", ZK_QUORUM);
        config.set("hbase.zookeeper.property.clientPort", ZK_PORT);
        config.set("hbase.master", ZK_MASTER);
//        config.set("hbase.rootdir", "hdfs://sandbox.hortonworks.com:8020/apps/hbase/data");
//        config.set("zookeeper.znode.parent", "/hbase-unsecure");
//        String path = this.getClass()
//                .getClassLoader()
//                .getResource("hbase-site.xml")
//                .getPath();
//        config.addResource(new Path(path));
        connection = ConnectionFactory.createConnection(config);
        admin = connection.getAdmin();
//        org.apache.hadoop.hbase.client.HBaseAdmin.checkHBaseAvailable(config);
    }

    public void listTables() throws IOException {
        TableName[] names = admin.listTableNames();
        System.out.println("list table: ");
        for (TableName tableName : names) {
            System.out.println(tableName.getNameAsString());
        }
    }

    public boolean isExists(String table) throws IOException {
        TableName tableName = TableName.valueOf(table);
        boolean exists = admin.tableExists(tableName);
        if (exists) {
            System.out.println("Table " + tableName.getNameAsString() + " already exists.");
        } else {
            System.out.println("Table " + tableName.getNameAsString() + " not exists.");
        }
        return exists;
    }

    public void createTable(String table, String columnFamily) throws IOException {
        TableName tableName = TableName.valueOf(table);
        boolean exists = admin.tableExists(tableName);
        if (!exists) {
            System.out.println("To create table named " + table);
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            HColumnDescriptor columnDesc = new HColumnDescriptor(columnFamily);
            tableDesc.addFamily(columnDesc);
            admin.createTable(tableDesc);
            System.out.println(table + " created");
        } else {
            System.out.println("table Exists!");
        }
    }

    public void deleteTable(String table) throws IOException {
        TableName tableName = TableName.valueOf(table);
        System.out.println("disable and then delete table named " + table);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        System.out.println(table + " deleted");
    }

    public void addData(String rowKey, String tableName,
                        String[] column1, String[] value1)
            throws IOException {
        TableName tn = TableName.valueOf(tableName);
        Table table = connection.getTable(tn);

        Put put = new Put(Bytes.toBytes(rowKey));
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();

        for (int i = 0; i < columnFamilies.length; i++) {
            String f = columnFamilies[i].getNameAsString();
            if (f.equals("article")) {
                for (int j = 0; j < column1.length; j++) {
                    put.addColumn(Bytes.toBytes(f), Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }
//            if (f.equals("author")) {
//                for (int j = 0; j < column2.length; j++) {
//                    put.addColumn(Bytes.toBytes(f), Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
//                }
//            }
        }

        table.put(put);
        System.out.println("add data Success!");
    }

    public void putDatas(String table, String columnFamily) throws IOException {
        String[] rows = {"eda_20191001_20201001", "cdmp_20191002_20201002"};
        String[] columns = {"owner", "ip", "server", "reg_date", "exp_date"};
        String[][] values = {
                {"eda", "220.181.57.217", "tpe", "2019-10-01", "2020-10-01"},
                {"cdmp.", "205.204.101.42", "tpe", "2019-10-02", "2020-10-02"}
        };
        TableName tableName = TableName.valueOf(table);
        byte[] family = Bytes.toBytes(columnFamily);
        Table tableObj = connection.getTable(tableName);
        for (int i = 0; i < rows.length; i++) {
            System.out.println("========================" + rows[i]);
            byte[] rowkey = Bytes.toBytes(rows[i]);
            Put put = new Put(rowkey);
            for (int j = 0; j < columns.length; j++) {
                byte[] qualifier = Bytes.toBytes(columns[j]);
                byte[] value = Bytes.toBytes(values[i][j]);
                put.addColumn(family, qualifier, value);
            }
            tableObj.put(put);
        }
        tableObj.close();
    }

    public void getData(String table, String columnFamily) throws IOException {
//        LOG.info("Get data from table " + TABLE_NAME + " by family.");
        System.out.println("Get data from table " + table + " by family.");
        TableName tableName = TableName.valueOf(table);
        byte[] family = Bytes.toBytes(columnFamily);
        byte[] row = Bytes.toBytes("r1");
        Table tableObj = connection.getTable(tableName);

        Get get = new Get(row);
        //via addFamily or addColumn to push down
        get.addFamily(family); //via addFamily or addColumn to push down
        Result result = tableObj.get(get);
        List<Cell> cells = result.listCells();
        System.out.println("rowkey\tfamily\tcolumn\tvalue\ttimest");
        for (Cell cell : cells) {
            System.out.println("------------------------------------");
            System.out.println(new String(CellUtil.cloneRow(cell)) + "\t\t" +
                    new String(CellUtil.cloneFamily(cell)) + "\t\t" +
                    new String(CellUtil.cloneQualifier(cell)) + "\t\t" +
                    new String(CellUtil.cloneValue(cell)) + "\t\t" +
                    cell.getTimestamp()
            );
        }

    }

    public void scanTable(String table, String columnFamily) throws IOException {
        System.out.println("Scan table " + table + " to browse all datas.");
        TableName tableName = TableName.valueOf(table);
        byte[] family = Bytes.toBytes(columnFamily);
        boolean exists = admin.tableExists(tableName);
        Scan scan = new Scan();
        scan.addFamily(family);

        Table tableObj = connection.getTable(tableName);
        ResultScanner resultScanner = tableObj.getScanner(scan);
        System.out.println("rowkey\tfamily\tcolumn\tvalue\ttimest");
        for (Iterator<Result> it = resultScanner.iterator(); it.hasNext(); ) {
            Result result = it.next();
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println("------------------------------------");
                System.out.println(new String(CellUtil.cloneRow(cell)) + "\t\t" +
                        new String(CellUtil.cloneFamily(cell)) + "\t\t" +
                        new String(CellUtil.cloneQualifier(cell)) + "\t\t" +
                        new String(CellUtil.cloneValue(cell)) + "\t\t" +
                        cell.getTimestamp()
                );
            }
        }
    }

    public void queryByFilter() {
        Filter filter = new PageFilter(15);
        int totalRows = 0;
        byte[] lastRow = null;

        Scan scan = new Scan();
        scan.setFilter(filter);

        // 略
    }

    private void deleteDatas(String table, String columnFamily) throws IOException {
        System.out.println("delete data from table " + table + " .");
        TableName tableName = TableName.valueOf(table);
        byte[] family = Bytes.toBytes(columnFamily);
        byte[] row = Bytes.toBytes("baidu.com_19991011_20151011");
        Delete delete = new Delete(row);

        // @deprecated Since hbase-1.0.0. Use {@link #addColumn(byte[], byte[])}
        // delete.deleteColumn(family, qualifier);			// delete specific version of specific column
        delete.addColumn(family, Bytes.toBytes("owner"));

        // @deprecated Since hbase-1.0.0. Use {@link #addColumns(byte[], byte[])}
        // delete.deleteColumns(family, qualifier)			// delete all version of specific column

        // @deprecated Since 1.0.0. Use {@link #(byte[])}
        // delete.addFamily(family);							// delete specific column family

        Table tableObj = connection.getTable(tableName);
        tableObj.delete(delete);
    }
    public void destory() throws SQLException {
        try {
            if (admin != null) {
                admin.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
