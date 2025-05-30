package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeProvinceOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/*
    省份粒度下单聚合过程的统计

    需要启动的进程：  zk,kafka,maxwell,hbase,redis,doris,DwdTradeOrderDetail,当前程序
 */
public class DwsTradeProvinceOrderWindow extends BaseApp {

    public static void main(String[] args) throws Exception {
        new DwsTradeProvinceOrderWindow().start(
                10020,
                4,
                "dws_trade_province_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //todo 1.过滤空消息  并对流中的数据进行类型转换 jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {

                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        if (jsonStr != null) {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            collector.collect(jsonObj);
                        }
                    }
                }
        );
        //{"create_time":"2025-05-30 18:24:01","sku_num":"1","split_original_amount":"39.0000","split_coupon_amount":"0.0","sku_id":"22","date_id":"2025-05-30",
        // "user_id":"5057","province_id":"20","sku_name":"十月稻田 长粒香大米 东北大米 东北香米 5kg","id":"30584","order_id":"21980","split_activity_amount":"0.0",
        // "split_total_amount":"39.0","ts":1748600641}

//        jsonObjDS.print();

        //todo 2.按照唯一键（订单明细的id）进行分组
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObject -> jsonObject.getString("id"));

        //todo 3.去重   状态+抵消器
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor
                                = new ValueStateDescriptor<JSONObject>("lastJsonObjState", JSONObject.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        //  从状态中获取上次接收到的json对象
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj != null) {
                            //说明数据重复，将已经发送到下游的数据（在状态里面）影响到度量值的字段进行 取反操作 再传递到下游
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            collector.collect(lastJsonObj);
                        }
                        lastJsonObjState.update(jsonObject);
                        collector.collect(jsonObject);
                    }
                }
        );
//        distinctDS.print();

        //todo 4.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObject, long l) {
                                        return jsonObject.getLong("ts") * 1000;
                                    }
                                }
                        )
        );

        //todo 5.再次对流中的数据进行类型转换 jsonObj ->  统计的实体类对象
        SingleOutputStreamOperator<TradeProvinceOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean map(JSONObject jsonObject) throws Exception {
                        //{"create_time":"2025-05-30 18:24:01","sku_num":"1","split_original_amount":"39.0000","split_coupon_amount":"0.0","sku_id":"22","date_id":"2025-05-30",
                        // "user_id":"5057","province_id":"20","sku_name":"十月稻田 长粒香大米 东北大米 东北香米 5kg","id":"30584","order_id":"21980","split_activity_amount":"0.0",
                        // "split_total_amount":"39.0","ts":1748600641}\
                        String provinceId = jsonObject.getString("province_id");
                        BigDecimal splitTotalAmount = jsonObject.getBigDecimal("split_total_amount");
                        Long ts = jsonObject.getLong("ts");
                        String orderId = jsonObject.getString("order_id");

                        TradeProvinceOrderBean orderBean = TradeProvinceOrderBean.builder()
                                .provinceId(provinceId)
                                .orderAmount(splitTotalAmount)
                                .orderIdSet(new HashSet<>(Collections.singleton(orderId)))
                                .ts(ts)
                                .build();
                        return orderBean;
                    }
                }
        );
//        beanDS.print();

        //todo 6.分组
        KeyedStream<TradeProvinceOrderBean, String> provinceIdKeyedDS = beanDS.keyBy(TradeProvinceOrderBean::getProvinceId);

        //todo 7.开窗
        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowDS
                = provinceIdKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //todo 8.聚合  reduce 属于增量聚合
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<TradeProvinceOrderBean> iterable, Collector<TradeProvinceOrderBean> collector) throws Exception {
                        TradeProvinceOrderBean orderBean = iterable.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String edt = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String curDate = DateFormatUtil.tsToDate(timeWindow.getStart());
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());
                        collector.collect(orderBean);
                    }
                }
        );
//        reduceDS.print();

        //todo 9.关联省份维度
        SingleOutputStreamOperator<TradeProvinceOrderBean> withProvinceDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeProvinceOrderBean>() {
                    @Override
                    public void addDims(TradeProvinceOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setProvinceName(dimJsonObj.getString("name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_province";
                    }

                    @Override
                    public String getRowKey(TradeProvinceOrderBean orderBean) {
                        return orderBean.getProvinceId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //withProvinceDS.print();
        //todo 9.将关联后的结果写道 Doris 中去
        withProvinceDS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_province_order_window"));
    }
}
