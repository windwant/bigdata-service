package org.windwant.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * hbase 基本操作
 * jar包置于hbase文件夹下 执行：java -cp lib/*:hadoop-test-1.0-SNAPSHOT.jar org.windwant.bigdata.hbase.HBaseOpt
 * Created by Administrator on 2017/12/7.
 */
public class HBaseOpt {
    private static final Logger logger = LoggerFactory.getLogger(HBaseOpt.class);

    public static Configuration conf;

    static {
        Properties properties = new Properties();
        properties.setProperty("log4j.rootLogger", "INFO,application,console");
        properties.setProperty("log4j.appender.console", "org.apache.log4j.ConsoleAppender");
        properties.setProperty("log4j.appender.console.layout", "org.apache.log4j.PatternLayout");
        properties.setProperty("log4j.appender.console.layout.ConversionPattern", "%d [%t] %-5p %C{6} (%F:%L) - %m%n");

        properties.setProperty("log4j.appender.application", "org.apache.log4j.DailyRollingFileAppender");
        properties.setProperty("log4j.appender.application.File", "hbase-opt.log");
        properties.setProperty("log4j.appender.application.Append", "true");
        properties.setProperty("log4j.appender.application.DatePattern", "'.'yyyy-MM-dd");
        properties.setProperty("log4j.appender.application.layout", "org.apache.log4j.PatternLayout");
        properties.setProperty("log4j.appender.application.layout.ConversionPattern", "%d [%t] %-5p %C{6} (%F:%L) - %m%n");
        PropertyConfigurator.configure(properties);

    }

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.addResource("hbase-site.xml");

    }

    /**
     * create table
     * @param table
     * @param columnFamily
     */
    public static void createTable(String table, String columnFamily) {
        logger.info("begin create table: {}, columnFamily: {} ...", table, columnFamily);
        Connection conn = null;
        HBaseAdmin admin = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = (HBaseAdmin) conn.getAdmin();
            TableName tableName = TableName.valueOf(table);
            HTableDescriptor descriptor = new HTableDescriptor(tableName);
            descriptor.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(descriptor);
            logger.info("table: {} created success.", table);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                conn.close();
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * hbase put data
     * @param tableName
     * @param row
     * @param columnFamily
     * @param column
     * @param data
     */
    public static void put(String tableName, String row, String columnFamily, String column, String data) {
        logger.info("begin put data...");
        Connection conn = null;
        Table table = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
            table.put(put);
            logger.info("begin put data, table: {}, row: {}, columnFamily: {}, column: {}, data: {}",
                    new Object[]{tableName, row, columnFamily, column, data});
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                conn.close();
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * delete table
     * @param tableName
     */
    public static void deleteTable(String tableName){
        logger.info("begin delete table: {} ...", tableName);
        Connection conn = null;
        HBaseAdmin admin = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = (HBaseAdmin) conn.getAdmin();
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            logger.info("table: {} deleted.", tableName);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try{
                conn.close();
                admin.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    /**
     * get table data
     * @param tableName
     * @param row
     * @param columnFamily
     * @param column
     */
    public static void get(String tableName, String row, String columnFamily, String column){
        logger.info("begin get table data, tableName: {}, row: {}, columnFamily: {}, column: {} ...",
                new Object[]{tableName, row, columnFamily, column});
        Connection conn = null;
        Table table = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(row));
            Result result = table.get(get);
            byte[] colv = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
            logger.info("get data: {} success.", new String(colv, "utf-8"));
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try {
                conn.close();
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * scan table
     * @param tableName
     */
    public static void scanTable(String tableName){
        logger.info("begin scan table data, tableName: {}", tableName);
        Connection conn = null;
        Table table = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf(tableName));
            ResultScanner resultScanner = table.getScanner(new Scan());
            for (Result result : resultScanner) {
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    String row = new String(result.getRow(), "UTF-8");
                    String family = new String(CellUtil.cloneFamily(cell), "UTF-8");
                    String qualifier = new String(CellUtil.cloneQualifier(cell), "UTF-8");
                    String value = new String(CellUtil.cloneValue(cell), "UTF-8");
                    logger.info("scan table {} data: row:{}, family: {}, qualifier:{}, value:{}, timestamp: {}",
                            new Object[]{tableName, row, family, qualifier, value, cell.getTimestamp()});
                }
            }
        } catch (IOException e){
            e.printStackTrace();
        }finally {
            try {
                conn.close();
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * delete table columnFamily
     * @param tableName
     * @param row
     * @param columnFamily
     */
    public static void delColFamily(String tableName, String row, String columnFamily) {
        logger.info("delete table columnFamily ...",
                new Object[]{tableName, row, columnFamily});
        Connection conn = null;
        Table table = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf(tableName));
            Delete del = new Delete(Bytes.toBytes(row));
            del.addFamily(Bytes.toBytes(columnFamily));
            table.delete(del);
            logger.info("delete table columnFamily, tableName: {}, row: {}, columnFamily: {} ...",
                    new Object[]{tableName, row, columnFamily});
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try {
                conn.close();
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * put table columnFamily
     * @param tableName
     * @param columnFamily
     */
    public static void putFamily(String tableName, String columnFamily){
        logger.info("begin delete table: {} ...", tableName);
        Connection conn = null;
        HBaseAdmin admin = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = (HBaseAdmin) conn.getAdmin();
            if(!admin.tableExists(tableName)){
                logger.warn("table: {} not exist!", tableName);
            }else {
                admin.disableTable(TableName.valueOf(tableName));
                HColumnDescriptor hcol = new HColumnDescriptor(columnFamily);
                admin.addColumn(TableName.valueOf(tableName), hcol);
                admin.enableTable(tableName);
                logger.info("put table columnFamily, table: {}, columnFamily: {}", tableName, columnFamily);
            }
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try{
                conn.close();
                admin.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    /**
     * delete table column
     * @param tableName
     * @param row
     * @param columnFamily
     */
    public static void delCol(String tableName, String row, String columnFamily, String column) {
        logger.info("delete table column ...",
                new Object[]{tableName, row, columnFamily});
        Connection conn = null;
        Table table = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf(tableName));
            Delete del = new Delete(Bytes.toBytes(row));
            del.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
            table.delete(del);
            logger.info("delete table column, tableName: {}, row: {}, columnFamily: {}, column: {}",
                    new Object[]{tableName, row, columnFamily});
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try {
                conn.close();
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * delete table row
     * @param tableName
     * @param row
     */
    public static void delRow(String tableName, String row){
        logger.info("delete table row ...", tableName, row);
        Connection conn = null;
        Table table = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf(tableName));
            Delete del = new Delete(Bytes.toBytes(row));
            table.delete(del);
            logger.info("delete table row, tableName: {}, row: {}", tableName, row);
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try {
                conn.close();
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        String tableName = "test-1";
        String colFamily1 = "cf1";
        String colFamily2 = "cf2";
        String col1 = "col1";
        String col2 = "col2";
        String row = "row1";
        deleteTable(tableName);
        createTable(tableName, colFamily1);
        put(tableName, row, colFamily1, col1, "value1");
        put(tableName, row, colFamily1, col2, "value2");
        get(tableName, row, colFamily1, col1);
        get(tableName, row, colFamily1, col2);
        scanTable(tableName);
        putFamily(tableName, colFamily2);
        put(tableName, row, colFamily2, col1, "value1");
        put(tableName, row, colFamily2, col2, "value2");
        scanTable(tableName);
        delColFamily(tableName, row, colFamily1);
        scanTable(tableName);
        delCol(tableName, row, colFamily1, col1);
        delCol(tableName, row, colFamily2, col2);
        scanTable(tableName);
        delRow(tableName, row);
        scanTable(tableName);

    }
}
