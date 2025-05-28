package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/*
    首页、详情页独立访客聚合统计
 */
public class DwsTrafficHomeDetailPageViewWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTrafficHomeDetailPageViewWindow().start(
                10023,
                4,
                "dws_traffic_home_detail_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //todo 1.对流中的数据类型进行转换 jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        //todo 2.过滤首页以及详情页
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");
                        return "home".equals(pageId) || "good_detail".equals(pageId);
                    }
                }
        );
//        filterDS.print();
        //todo  3.指定Watermark的生成策略以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = filterDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObject, long l) {
                                        return jsonObject.getLong("ts");
                                    }
                                }
                        )

        );
        //todo  4.按照mid分组
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        
        //todo  5.使用flink的状态编程 判断是否为首页以及详情页的独立访客， 并将结果封装为统计的实体类对象
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
                    // flink 状态编程
                    //存放首页的上次访问日期
                    private ValueState<String> homeLastVisitDateAState;
                    //存放详情页的上次访问日期
                    private ValueState<String> detailLastVisitDateAState;

                    // 在open方法里面做初始化操作

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> homeValueStateDescriptor = new ValueStateDescriptor<String>("homeLastVisitDateAState", String.class);
                        homeValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        homeLastVisitDateAState = getRuntimeContext().getState(homeValueStateDescriptor);

                        ValueStateDescriptor<String> detailValueStateDescriptor = new ValueStateDescriptor<String>("detailLastVisitDateAState", String.class);
                        homeValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        detailLastVisitDateAState = getRuntimeContext().getState(homeValueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context context, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");

                        Long ts = jsonObject.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);
                        Long homeUvCt = 0L;
                        Long detailUvCt = 0L;
                        if ("home".equals(pageId)) {
                            //获取首页的上次访问日期
                            String homeLastVisitDate = homeLastVisitDateAState.value();
                            if (StringUtils.isEmpty(homeLastVisitDate) || !homeLastVisitDate.equals(curVisitDate)) {
                                homeUvCt = 1L;
                                homeLastVisitDateAState.update(curVisitDate);
                            }
                        } else {
                            //获取详情页的上次访问日期
                            String detailLastVisitDate = detailLastVisitDateAState.value();
                            if (StringUtils.isEmpty(detailLastVisitDate) || !detailLastVisitDate.equals(curVisitDate)) {
                                detailUvCt = 1L;
                                detailLastVisitDateAState.update(curVisitDate);
                            }
                        }
                        if (homeUvCt != 0L || detailUvCt != 0L) {
                            collector.collect(new TrafficHomeDetailPageViewBean(
                                    "", "", "", homeUvCt, detailUvCt, ts
                            ));
                        }
                    }
                }
        );
//        beanDS.print();
        //todo  6.开窗 滚动事件窗口
        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowDS
                = beanDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //todo  7.聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceDS = windowDS.reduce(

                new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                        value1.setHomeUvCt(value1.getHomeUvCt()+value2.getHomeUvCt());
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt()+value2.getGoodDetailUvCt());
                        return value1;
                    }
                },
                new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<TrafficHomeDetailPageViewBean> iterable, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                        TrafficHomeDetailPageViewBean viewBean = iterable.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String edt = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String curDate = DateFormatUtil.tsToDate(timeWindow.getStart());
                        viewBean.setStt(stt);
                        viewBean.setEdt(edt);
                        viewBean.setCurDate(curDate);
                        collector.collect(viewBean);
                    }
                }
        );

        //todo  8.将聚合的结果写道doris 转化成json格式字符串
        reduceDS.print();
        reduceDS
                .map(new BeanToJsonStrMapFunction<TrafficHomeDetailPageViewBean>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_home_detail_page_view_window"));
    }
}
