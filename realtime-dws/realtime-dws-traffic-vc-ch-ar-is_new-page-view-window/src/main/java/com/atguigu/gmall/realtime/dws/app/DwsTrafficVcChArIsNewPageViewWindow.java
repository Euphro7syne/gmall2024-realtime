package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/*
    按照版本、地区、渠道、新老访客对pv、uv、sv、dur进行聚合统计
    需要启动的进程
        zk kafka flume doris,DwdBaseLog 当前程序
 */
public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTrafficVcChArIsNewPageViewWindow().start(
                10022,
                4,
                "dws_traffic_vc_ch_ar_is_new_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );

    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //todo 1.对流中数据进行类型转换  jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        //todo 2.按照mid对流中数据进行分组
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //todo 3.再次对流中的数据进行类型转换  jsonStr -> 统计的实体类对象
        SingleOutputStreamOperator<TrafficPageViewBean> beanDS = midKeyedDS.map(
                new RichMapFunction<JSONObject, TrafficPageViewBean>() {
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public TrafficPageViewBean map(JSONObject jsonObj) throws Exception {
                        JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                        // 从状态中获取当前位置上次访问日期
                        String lastVisitDate = lastVisitDateState.value();
                        // 获取当前访问日期
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);
                        Long uvCt = 0L;
                        if (StringUtils.isEmpty(lastVisitDate) || !lastVisitDate.equals(curVisitDate)){
                            uvCt=1L;
                            lastVisitDateState.update(curVisitDate);
                        }
                        String lastPageId = pageJsonObj.getString("last_page_id");
                        Long svCt = StringUtils.isEmpty(lastPageId) ? 1L :0L;
                        return new TrafficPageViewBean(
                                "",
                                "",
                                "",
                                commonJsonObj.getString("vc"),
                                commonJsonObj.getString("ch"),
                                commonJsonObj.getString("ar"),
                                commonJsonObj.getString("is_new"),
                                uvCt,
                                svCt,
                                1L,
                                pageJsonObj.getLong("during_time"),
                                ts
                        );
                    }
                }
        );
//        beanDS.print();
        //todo 4.指定WaterMark以及提取事件时间字段
        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkDS = beanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TrafficPageViewBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TrafficPageViewBean>() {
                                    @Override
                                    public long extractTimestamp(TrafficPageViewBean bean, long l) {
                                        return bean.getTs();
                                    }
                                }
                        )
        );
        //todo 5.分组--按照统计的维度进行分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> dimKeyedDS = withWatermarkDS.keyBy(
                new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficPageViewBean bean) throws Exception {
                        return Tuple4.of(
                                bean.getVc(),
                                bean.getCh(),
                                bean.getAr(),
                                bean.getIsNew()
                        );
                    }
                }
        );
        //todo 6.开窗
        // 以滚动事件时间窗口为例，分析以下几个问题
        //  窗口对象创建时间 : 当属于这个窗口的第一个元素到来的时候创建窗口对象
        //  窗口的起始结束时间 （窗口为什么是左闭右开的）
        //      向下取整：long start = TimeWindow.getWindowStartWithOffset(timestamp, (this.globalOffset + this.staggerOffset) % this.size, this.size);
        //  窗口计算什么时候触发  水位线到了窗口的最大时间（事件时间-乱序程度时间-1ms ）的时候 才会触发计算
        //  窗口什么时候关闭    水位线时间 >=窗口最大时间+允许迟到时间
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windosDS = dimKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //todo 7.聚合计算
        SingleOutputStreamOperator<TrafficPageViewBean> reduceDS = windosDS.reduce(
                new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        return value1;
                    }
                },
                new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow timeWindow, Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> collector) throws Exception {
                        TrafficPageViewBean pageViewBean = iterable.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String edt = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String curDate = DateFormatUtil.tsToDate(timeWindow.getStart());

                        pageViewBean.setStt(stt);
                        pageViewBean.setEdt(edt);
                        pageViewBean.setCur_date(curDate);

                        collector.collect(pageViewBean);
                    }
                }
        );
        reduceDS.print();
        //todo 8.将聚合的结果写道doris中
        reduceDS
                // 在向doris写数据前，将流中的实体类对象转换成json格式字符串
                .map(new BeanToJsonStrMapFunction<TrafficPageViewBean>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_vc_ch_ar_is_new_page_view_window"));
    }
}
