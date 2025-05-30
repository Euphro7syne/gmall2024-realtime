package com.atguigu.gmall.realtime.dim.function;

/*
    将流中的数据同步到HBase表中
 */

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    private Connection hbaseConn;
    private Jedis jedis;
    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HBaseUtil.getHbaseConnection();
        jedis = RedisUtil.getJedis();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHbaseConnection(hbaseConn);
        RedisUtil.closeJedis(jedis);
    }

    // 将流中的数据写到hbase里面
    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> tup, Context context) throws Exception {
        JSONObject jsonObj = tup.f0;
        TableProcessDim tableProcessDim = tup.f1;
        String type = jsonObj.getString("type");
        jsonObj.remove("type");
        //获取hbase表的表名
        String sinkTable = tableProcessDim.getSinkTable();
        // 获取hbase的rowkey
        String rowKey = jsonObj.getString(tableProcessDim.getSinkRowKey());
        // 判断对业务数据库的维度表进行什么操作
        if("delete".equals(type)) {
            // 从业务数据库维度表中做了删除操作， 需要将hbase维度表将对应的记录也删除掉
            HBaseUtil.delRow(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, rowKey);
        }
        else {
            // 如果不是delete，可能的类型有insert ，update，bootstrap-insert,上述操作都是对hbase进行put操作
            String sinkFamily = tableProcessDim.getSinkFamily();
            HBaseUtil.putRow(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,rowKey,sinkFamily,jsonObj);
        }

        //如果维度表数据发生了变化，将Redis中缓存的数据清除掉
        if("update".equals(type) || "delete".equals(type)) {
            String key = RedisUtil.getKey(sinkTable, rowKey);
            jedis.del(key);
        }
    }
}
