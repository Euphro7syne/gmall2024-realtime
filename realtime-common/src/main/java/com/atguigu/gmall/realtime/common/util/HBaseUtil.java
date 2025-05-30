package com.atguigu.gmall.realtime.common.util;
// todo 操作HBase的工具类

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class HBaseUtil {

    // todo 获取HBase连接
    public static Connection getHbaseConnection() throws IOException {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        Connection hbaseConn = ConnectionFactory.createConnection(conf);
        return hbaseConn;
    }

    // todo 关闭HBase连接
    public static void closeHbaseConnection(Connection hbaseConn) throws IOException{
        if(hbaseConn != null && !hbaseConn.isClosed()){
            hbaseConn.close();
        }

    }

    //todo 获取异步操作HBase的连接对象
    public static AsyncConnection getHBaseAsyncConnection(){
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        try {
            AsyncConnection asyncConnection = ConnectionFactory.createAsyncConnection(conf).get();
            return asyncConnection;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //todo 关闭异步操作HBase的连接对象
    public static void closeAsyncHbaseConnection(AsyncConnection asyncConn) {
        if (asyncConn != null && !asyncConn.isClosed()) {
            try {
                asyncConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    // todo 建表
    public static void createHBaseTable(Connection hbaseConn,String namespace,String tableName,String ... families){
        if(families.length <1){
            System.out.println("至少需要一个列族");
            return;
        }
        try (Admin admin = hbaseConn.getAdmin()){
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if(admin.tableExists(tableNameObj)){
                System.out.println("表空间"+namespace+"下的表"+tableName+"已存在");
                return;
            }
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for (String family : families) {
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("表空间"+namespace+"下的表"+tableName+"创建成功");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    // todo 删除表
    public static void dropHbaseTable(Connection hbaseConn,String namespace,String tableName)  {

        try (Admin admin = hbaseConn.getAdmin()){
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            // 要判断表是否存在
            if (!admin.tableExists(tableNameObj)){
                System.out.println("要删除的表空间"+namespace+"下的表"+tableName+"不存在");
                return;
            }
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
            System.out.println("删除的表空间"+namespace+"下的表"+tableName+"成功");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    //todo 向表中put数据
    // hbaseConn  连接对象 ； namespace  表空间 ； tableName 表名 ； rowKey rowkey ;  family 列族 ； jsonObj 要传输的数据
    public static void putRow(Connection hbaseConn, String namespace, String tableName, String rowKey, String family, JSONObject jsonObj){
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)){
            Put put = new Put(Bytes.toBytes(rowKey));
            Set<String> columns = jsonObj.keySet();
            for (String column : columns) {
                String value = jsonObj.getString(column);
                if(StringUtils.isNotEmpty(value)){
                    put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(value));
                }
            }
            table.put(put);
            System.out.println("向表空间"+namespace+"下的表"+tableName+"中put数据"+rowKey+"成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    //todo 向表中删除数据
    public static void delRow(Connection hbaseConn, String namespace, String tableName, String rowKey){
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)){
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println("向表空间"+namespace+"下的表"+tableName+"中删除数据"+rowKey+"成功");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
    }
    }

    //todo 根据rowkey从Hbase中查询一行数据
    public static <T>T getRow(Connection hbaseConn, String namespace, String tableName, String rowKey,  Class<T> clz, boolean... isUnderlineToCamel){
        boolean defaultIsToc=false; //默认不执行下划线转驼峰

        if(isUnderlineToCamel.length>0){
            defaultIsToc = isUnderlineToCamel[0];
        }
        // 这里的tablename不是真正的表名，而是一个对象
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)){
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            // listcell 方法可以拿到rowkey对应的所有单元格
            List<Cell> cells = result.listCells();
            if (cells != null && cells.size() > 0) {
                //定义一个对象，用于封装查询出来的一行数据
                T obj = clz.newInstance();
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    if(defaultIsToc){
                        columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    BeanUtils.setProperty(obj, columnName, columnValue);
                }
                return obj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * //todo 以异步的方式 从HBase维度表中查询维度数据
     * @param asyncConnection  异步操作HBase的连接
     * @param namespace         表空间
     * @param tableName         表名
     * @param rowKey            rowkey
     * @return
     */
    public static JSONObject readDimAsync(AsyncConnection asyncConnection,String namespace,String tableName,String rowKey){
        try {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            AsyncTable<AdvancedScanResultConsumer> asyncTable = asyncConnection.getTable(tableNameObj);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();
            if (cells != null && cells.size() > 0) {
                JSONObject jsonObject = new JSONObject();
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    jsonObject.put(columnName, columnValue);
                }
                return jsonObject;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;

    }



    public static void main(String[] args) throws Exception {
        Connection hbaseConnection = getHbaseConnection();
        JSONObject dimBaseTrademark = getRow(hbaseConnection, Constant.HBASE_NAMESPACE, "dim_base_trademark", "1", JSONObject.class, true);
        System.out.println(dimBaseTrademark);
        closeHbaseConnection(hbaseConnection);
    }
}
