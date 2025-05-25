package com.atguigu.gmall.realtime.dwd.db.split.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;

/*
    事实表动态分流 -- 处理关联后的数据
 */
public class BaseDbTableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject,TableProcessDwd>> {

    private MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor;

    private Map<String,TableProcessDwd> configMap = new HashMap<>();

    public BaseDbTableProcessFunction(MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //todo 将配置信息预加载到程序中
        //    避免主流数据先到 广播配置信息未到产生的阻塞
        Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcessDwd> tableProcessDwdList
                = JdbcUtil.queryList(mysqlConnection, "select * from gmall2024_config.table_process_dwd", TableProcessDwd.class, true);
        for (TableProcessDwd tableProcessDwd : tableProcessDwdList) {
            String sourceTable = tableProcessDwd.getSourceTable();
            String sourceType = tableProcessDwd.getSourceType();
            String key = getKey(sourceTable, sourceType);
            configMap.put(key,tableProcessDwd);
        }
        JdbcUtil.closeMysqlConnection(mysqlConnection);
    }

    private String getKey(String sourceTable, String sourceType) {
        String key = sourceTable + ":" + sourceType;
        return key;
    }

    //处理主流业务数据
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        //获取处理的业务数据库表的表名
        String table = jsonObject.getString("table");
        //获取操作类型
        String type = jsonObject.getString("type");
        //拼接key
        String key = getKey(table, type);
        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        //根据key到广播状态以及configMap中获取配置信息
        TableProcessDwd tp = null;
        if ((tp=broadcastState.get(key)) != null || (tp=configMap.get(key)) !=null ){
            //说明当前数据，是需要动态分流处理的事实表数据，将data部分传递到下游
            JSONObject dataJsonObj = jsonObject.getJSONObject("data");
            //在向下游传递数据之前，过滤掉不需要传递的字段
            String sinkColumns = tp.getSinkColumns();
            deleteNotNeedColumns(dataJsonObj, sinkColumns);
            //在向下游传递数据之前，将ts事件时间补充到data对象中
            Long ts = jsonObject.getLong("ts");
            dataJsonObj.put("ts",ts);

            collector.collect(Tuple2.of(dataJsonObj, tp));
        }
    }

    //处理广播流配置信息
    @Override
    public void processBroadcastElement(TableProcessDwd tp, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        //获取对配置信息进行操作的类型
        String op = tp.getOp();
        //获取广播状态
        BroadcastState<String, TableProcessDwd> broadcastState = context.getBroadcastState(mapStateDescriptor);
        //获取业务数据库的表的表明
        String sourceTable = tp.getSourceTable();
        //获取业务数据库的表对应的操作类型
        String sourceType = tp.getSourceType();
        //拼接key
        String key = getKey(sourceTable, sourceType);
        if ("d".equals(op)) {
            //从配置表中删除了一条数据，那么需要将广播状态以及configMap中对应的配置也删除掉
            broadcastState.remove(key);
            configMap.remove(key);
        }else {
            //从配置表中读取数据或者添加、更新了数据 ，那么需要将最新的这条配置信息放到广播状态以及configMap中
            broadcastState.put(key,tp);
            configMap.put(key,tp);
        }

    }
    // 过滤掉不需要传递的字段
    // dataJsonObj : {"tm_name":"Redmi","create_time":"2021-12-14 00:00:00","logo_url":"22222","id":1}
    // sinkColumns : id,tm_name
    // dataJsonObj : 底层使用了map来进行封装,即包含了K和V
    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();

        // 判断 dataJsonObj的字段是否在 sinkColumns 中存在，如果不存在就删除该字段，等同于去掉不需要的列，再往下继续传递数据
        // 这里调用的是集合的remove方法
        entrySet.removeIf(entry -> !columnList.contains(entry.getKey()));

    }
}
