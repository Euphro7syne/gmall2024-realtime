package com.atguigu.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;

/*
    处理主流业务数据和广播流配置数据关联后的逻辑
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {

    MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;

    // todo configMap的存在就是解决 主流业务数据 先到但是 广播流配置信息还没到而造成的 主流业务数据等待的问题
    //  把配置信息同步存储在configMap中就不用等待
    // String 代表着集合 ，TableProcessDim 代表着封装的对象
    private Map<String, TableProcessDim> configMap = new HashMap<>();

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 将配置表中的配置信息预加载到程序中
        // 使用MySQL
        Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(mysqlConnection, "select * from gmall2024_config.table_process_dim", TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDimList) {
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        JdbcUtil.closeMysqlConnection(mysqlConnection);
    }

    // 处理主流业务数据 ,根据维度表名到广播状态中读取配置信息，判断是否为维度
    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        // 获取数据表的表明
        String table = jsonObj.getString("table");
        // 获取广播状态
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 根据表名到广播状态中获取对应的配置信息 , 如果没有找到对应的配置，再尝试到 configMap 中获取
        TableProcessDim tableProcessDim = null;



        if ((tableProcessDim = broadcastState.get(table)) != null
                || (tableProcessDim = configMap.get(table)) != null) {
            // 如果根据表名获取到了对应的配置信息，说明当前处理的是维度数据，将维度数据继续向下游传递

            // 将维度数据向下游传递（只需要传递data属性）
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");

            //在向下游传递数据之前，应该提前过滤掉不需要的属性
            String sinkColumns = tableProcessDim.getSinkColumns();
            deleteNotNeedColumns(dataJsonObj, sinkColumns);

            // 在下游传递数据之前，补充对维度数据的操作作类型属性
            String type = jsonObj.getString("type");
            dataJsonObj.put("type", type);

            out.collect(Tuple2.of(dataJsonObj, tableProcessDim));
        }
    }

    // 处理广播流配置信息  将配置数据放到广播状态中或者删除对应的配置     k:维度表明 v:一个配置对象
    @Override
    public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        // 获取对配置表进行操作的类型
        String op = tp.getOp();
        // 获取广播状态
        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 获取维度表的名称
        String sourceTable = tp.getSourceTable();
        if ("d".equals(op)) {
            //从配置表中删除一条消息，将对应的配置信息也从广播状态中删除
            broadcastState.remove(sourceTable);
            configMap.remove(sourceTable);
        } else {
            //对配置表进行了读取、添加或者更新操作，将最新的配置信息放到广播状态中
            broadcastState.put(sourceTable, tp);
            configMap.put(sourceTable, tp);
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


