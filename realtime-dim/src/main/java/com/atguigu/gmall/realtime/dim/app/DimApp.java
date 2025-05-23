package com.atguigu.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.dim.function.HBaseSinkFunction;
import com.atguigu.gmall.realtime.dim.function.TableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

/*
   开发流程
        基本环境准备
        检查点相关设置
        从Kafka主题中读取数据
        对流中的数据进行类型转换并ETL  jsonStr -> jsonObj
        --------------------------------------------
        使用FlinkCDC读取配置表中的配置信息
        对读取到的配置流数据进行类型转换 jsonStr -> 实体类对象
        根据当前配置信息到HBase中执行建表或者删除操作
            op=d    删表
            op=c,r  建表
            op=u    先删表再建表
        对配置流数据进行广播-broadcast
        关联主流业务数据以及广播流配置数据--connect
        对关联后的数据进行处理 --- process
             new TableProcessFunction extends BroadcastProcessFunction{
             open:将配置信息厦加载到程序中，避免主流数据先到、广播流数据后到，丢失数的况
             processElement;对主流数据的处理
                获取操作的表的表名
                根据表名到广播状态中议及configHap中获取对应的配置信息，如果配置信息不为空，说明是维度，将维度数据发送到下游
                   Tuple2<dataJsonObj,配置对象>
                在向下游发送数据前，过滤掉了不需要传递的属性
                在向下游发送数据时，补充操作类型
            processBroadcastElement:对广播派数据进行处理
                op =d 将配置信息从广播状态以及configMap中删除
                op!=d 将配置信息放到广播状态以从configMap
             }
         将流中数据同步到HBase中
            class HBaseSinkFunction extends RichSinkFunction{
                invoke:
                    type="delete":从HBase表中删除数据
                    type!="delete":向HBase中put数据
            }
         优化:抽取FlinkSourceUtilL工具类
              抽取TableProcessFunction以及HBaseSinkFunction函数处理
              抽取方法
              抽取基类---模板方法设计模式
        执行流程(以修改了品牌维度表中的一条数据为例)
            当程序启动的时候，会将配置表中的配置信息加载到configMap以及广播状态中
            修改品牌维度
            binlog会将修改操作记录下来
            maxwell会从binlog中获取修改的信息，并封装为json字符串格式发送到kafka的topic_db主题中
            DimApp应用程序会从topic_db上中读取数据并对其进行处理
            根据当前处理内数拱府表名判断是否为维度
            如果是维度的话，将维度数据传递到下游
            将维度数据同步到HBase中

 */


public class DimApp extends BaseApp {
    // todo 维度层的处理
    // todo 需要启动的进程
    //  zookeeper。Kafka，hbase，maxwell，hdfs，DimApp
    public static void main(String[] args) throws Exception {
        new DimApp().start(10001,4,"dim_app",Constant.TOPIC_DB);
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {

        // todo 对流中数据进行转换并进行简单的ETL jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);

        // todo 使用FlinkCDC读取配置表中的配置信息
        SingleOutputStreamOperator<TableProcessDim> tpDS = readTableProcess(env);

        // todo 根据配置表中的配置信息到 Hbase 执行建表和删表操作
        tpDS = createHBaseTable(tpDS);
        //tpDS.print();

        // todo 过滤维度数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connect(tpDS, jsonObjDS);

        // todo 11.将维度数据写到HBase表中
        dimDS.print();
        writeToHBase(dimDS);
    }

    private static void writeToHBase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS) {
        dimDS.addSink(new HBaseSinkFunction());
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(SingleOutputStreamOperator<TableProcessDim> tpDS, SingleOutputStreamOperator<JSONObject> jsonObjDS) {
        // todo 将配置流中的配置信息进行广播--broadcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
                new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        // todo 9.将主流业务数据和广播配置信息进行关联--connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);

        // todo 10.处理关联后的数据（判断是否为维度）
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS.process(
                new TableProcessFunction(mapStateDescriptor)
        );
        return dimDS;
    }

    private static SingleOutputStreamOperator<TableProcessDim> createHBaseTable(SingleOutputStreamOperator<TableProcessDim> tpDS) {
        tpDS = tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHbaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHbaseConnection(hbaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tp) throws Exception {
                        //获取对配置表进行的操作类型
                        String op = tp.getOp();
                        //获取hbase中维度表的表名
                        String sinkTable = tp.getSinkTable();
                        // 获取hbase中建表的列族
                        String[] sinkFamilies = tp.getSinkFamily().split(",");
                        if ("d".equals(op)) {
                            //从配置表中删除了一条数据 将hbase中对应的表删除掉
                            HBaseUtil.dropHbaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                        } else if ("r".equals(op) || "c".equals(op)) {
                            //从配置表中读取了一条数据或者向配置表中添加了一条配置  在hbase中执行建表

                            HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        } else {
                            // 对配置表中的信息进行了修改，先从hbase中将对应的删除掉 在创建新表
                            HBaseUtil.dropHbaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                            HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        }
                        return tp;
                    }
                }
        ).setParallelism(1);
        return tpDS;
    }

    private static SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
        //5.1 创建MySQL source 对象
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("gmall2024_config", "table_process_dim");
        //5.2 读取数据 封装为流
        DataStreamSource<String> mysqlStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);
//        mysqlStrDS.print();

        // todo 6.对配置流中的数据进行类型转换 jsonStr -> 实体类对象
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) throws Exception {
                        // 为了处理方便，先将jsonStr转换为jsonObj
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim = null;
                        if ("d".equals(op)) {
                            // 对配置表进行了一次删除操作 ，就得从 before 属性中获取删除之前的配置信息
                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                        } else {
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                            // 对配置表进行了 读取、添加、修改 等操作之后，就从 after 属性中获取最新的配置信息

                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);
        //tpDS.print();
        return tpDS;
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject jsonObj = JSONObject.parseObject(jsonStr);
                        String db = jsonObj.getString("database");//拿到数据库名
                        String type = jsonObj.getString("type"); //获取操作类型
                        String data = jsonObj.getString("data");

                        if ("gmall2024".equals(db)
                                && ("insert".equals(type)
                                || "update".equals(type)
                                || "delete".equals(type)
                                || "bootstrap-insert".equals(type))
                                && data != null
                                && data.length() > 2
                        ) {
                            collector.collect(jsonObj);
                        }
                    }
                }
        );
//        jsonObjDS.print();
        return jsonObjDS;
    }
}




