package com.atguigu.gmall.realtime.dwd.log.split;
/*
    日志分流
    需要启动的进程：Hadoop，flume，zookeeper，Kafka，DwdBaseLog

 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;

public class DwdBaseLog extends BaseApp {
    private final String START = "start";
    private final String ERR = "err";
    private final String DISPLAY = "display";
    private final String ACTION = "action";
    private final String PAGE = "page";

    public static void main(String[] args) throws Exception {
        new DwdBaseLog().start(10002,4,"dwd_base_log", Constant.TOPIC_LOG);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //todo 对流中数据类型进行转换 并且作简单的ETL
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);

        //todo 对新老访客的标记进行修复
        SingleOutputStreamOperator<JSONObject> fixedDS = fixedNewAndOld(jsonObjDS);

        //todo 分流  错误日志-错误侧输出流 启动日志-启动侧输出流 曝光日志-曝光侧输出流 动作日志-动作侧输出流  页面日志-主流
        Map<String, DataStream<String>> streamMap = splitStream(fixedDS);

        //todo 将不同流的数据写到Kafka的不同主题中
        writeToKafka(streamMap);


    }

    private void writeToKafka(Map<String, DataStream<String>> streamMap) {
        streamMap.get(PAGE).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        streamMap.get(ERR).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        streamMap.get(START).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        streamMap.get(DISPLAY).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        streamMap.get(ACTION).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
    }

    private Map<String, DataStream<String>> splitStream(SingleOutputStreamOperator<JSONObject> fixedDS) {
        //定义侧输出流标签
        OutputTag<String> errTag = new OutputTag<String>("errTag"){};
        OutputTag<String> startTag = new OutputTag<String>("startTag"){};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag"){};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag"){};
        //分流
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
                new ProcessFunction<JSONObject, String>() {

                    @Override
                    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                        //----错误日志
                        JSONObject errJsonObj = jsonObject.getJSONObject("err");
                        if (errJsonObj != null) {
                            context.output(errTag, jsonObject.toJSONString());
                            jsonObject.remove("err");
                        }
                        JSONObject startJsonObj = jsonObject.getJSONObject("start");
                        if (startJsonObj != null) {
                            // ---启动日志
                            // 将启动日志写道启动测输出流
                            context.output(startTag, jsonObject.toJSONString());
                        } else {
                            // --页面日志
                            JSONObject commonJsonObj = jsonObject.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObject.getJSONObject("page");
                            Long ts = jsonObject.getLong("ts");

                            // -- 曝光日志
                            JSONArray displayArr = jsonObject.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                // 遍历当前页面的所有曝光信息
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject displayJsonObj = displayArr.getJSONObject(i);
                                    //定义一个新的json对象，用于封装遍历出来的曝光数据
                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    newDisplayJsonObj.put("common", commonJsonObj);
                                    newDisplayJsonObj.put("page", pageJsonObj);
                                    newDisplayJsonObj.put("display", displayJsonObj);
                                    newDisplayJsonObj.put("ts", ts);
                                    // 将曝光日志写到曝光侧输出流
                                    context.output(displayTag, newDisplayJsonObj.toJSONString());
                                }
                                jsonObject.remove("displays");
                            }
                            // -- 动作日志
                            JSONArray actionArr = jsonObject.getJSONArray("actions");
                            if (actionArr != null && actionArr.size() > 0) {
                                // 遍历每一个动作
                                for (int i = 0; i < actionArr.size(); i++) {
                                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
                                    // 定义一个新的json对象，用于封装动作信息
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common", commonJsonObj);
                                    newActionJsonObj.put("page", pageJsonObj);
                                    newActionJsonObj.put("action", actionJsonObj);

                                    //将动作日志写道动作侧输出流
                                    context.output(actionTag, newActionJsonObj.toJSONString());
                                }
                                jsonObject.remove("actions");
                            }
                            // 页面日志写到主流中
                            collector.collect(jsonObject.toJSONString());
                        }
                    }
                }
        );
        SideOutputDataStream<String> errDs = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDs = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDs = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDs = pageDS.getSideOutput(actionTag);
        pageDS.print("页面：");
        errDs.print("错误：");
        startDs.print("启动：");
        displayDs.print("曝光：");
        actionDs.print("动作：");
        Map<String, DataStream<String>> streamMap = new HashMap<>();
        streamMap.put(ERR, errDs);
        streamMap.put(START, startDs);
        streamMap.put(DISPLAY, displayDs);
        streamMap.put(ACTION, actionDs);
        streamMap.put(PAGE,pageDS);
        return streamMap;
    }

    private static SingleOutputStreamOperator<JSONObject> fixedNewAndOld(SingleOutputStreamOperator<JSONObject> jsonObjDS) {
        //按照设备id进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //使用flink的状态编程完成修复
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(
                // 创建复合函数使用open方法
                new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
                        // 获取is_new的值
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        // 从状态中获取首日访问日期
                        String lastVisitDate = lastVisitDateState.value();
                        // 获取当前访问日期
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);
                        if ("1".equals(isNew)) {
                            //如果is_new的值为1
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                //如果键控状态为null，认为本次是该访客首次访问 APP，将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改；
                                lastVisitDateState.update(curVisitDate);
                            } else {
                                //如果键控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0；
                                if (!lastVisitDate.equals(curVisitDate)) {
                                    isNew = "0";
                                    jsonObj.getJSONObject("common").put("is_new", isNew);
                                }
                            }
                        } else {
                            //如果 is_new 的值为 0
                            //如果键控状态为 null，说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。
                            //当前端新老访客状态标记丢失时，日志进入程序被判定为新访客，Flink 程序就可以纠正被误判的访客状态标记，
                            //只要将状态中的日期设置为今天之前即可。本程序选择将状态更新为昨日
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                String yesterDay = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                                lastVisitDateState.update(yesterDay);
                            }
                        }
                        return jsonObj;
                    }
                }
        );
        //fixedDS.print();
        return fixedDS;
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        // 定义侧输出流标签   注意泛型擦除的问题
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};
        //ETL
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            //如果转换的时候，没有发生异常，说明是标准json，将数据传递到下游
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            //如果转换的时候，发生异常，说明不是标准的json，属于脏数据。将其放到测输出流中
                            ctx.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );
        //jsonObjDS.print("标准的json：");
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        //dirtyDS.print("脏数据：");
        //将侧输出流中的脏数据写进Kafka主题中
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink("dirty_data");
        dirtyDS.sinkTo(kafkaSink);
        return jsonObjDS;
    }
}
