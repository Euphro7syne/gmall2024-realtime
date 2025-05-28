package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.UserLoginBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/*
    独立用户以及回流用户的聚合统计
 */
public class DwsUserUserLoginWindow extends BaseApp {

    public static void main(String[] args) throws Exception {
        new DwsUserUserLoginWindow().start(
                10024,
                4,
                "dws_user_user_login_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );

    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //todo 1.对流中的数据进行类型转换  jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        //todo 2.过滤出登录行为
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String uid = jsonObject.getJSONObject("common").getString("uid");
                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                        return StringUtils.isNotEmpty(uid)
                                && (("login".equals(lastPageId)) || StringUtils.isEmpty(lastPageId));
                    }
                }
        );
//        filterDS.print();
        //todo 3.指定watermark
        SingleOutputStreamOperator<JSONObject> withWaterMarkDS = filterDS.assignTimestampsAndWatermarks(
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
        //todo 4.按照uid进行分组
        KeyedStream<JSONObject, String> keyedDS = withWaterMarkDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("uid"));
        //todo 5.使用flink的状态编程 判断是否为独立用户以及回流用户
        SingleOutputStreamOperator<UserLoginBean> beanDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
                    //定义一个状态
                    private ValueState<String> lastLoginDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>("lastLoginState", String.class);
                        lastLoginDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context context, Collector<UserLoginBean> collector) throws Exception {
                        //获取上次登录日期
                        String lastLoginDate = lastLoginDateState.value();
                        //获取当前的登陆日期
                        Long ts = jsonObject.getLong("ts");
                        String curLoginDate = DateFormatUtil.tsToDate(ts);

                        Long uuCt = 0L;
                        Long backCt = 0L;

                        if (StringUtils.isNotEmpty(lastLoginDate)) {
                            //若状态中的末次登录日期不为 null，进一步判断。
                            if (!lastLoginDate.equals(curLoginDate)) {
                                //如果末次登录日期不等于当天日期则独立用户数 uuCt 记为 1，并将状态中的末次登录日期更新为当日，进一步判断。
                                uuCt = 1L;
                                lastLoginDateState.update(curLoginDate);

                                //如果当天日期与末次登录日期之差大于等于8天则回流用户数backCt置为1。
                                //否则 backCt 置为 0。
                                Long day = (ts - DateFormatUtil.dateToTs(lastLoginDate)) / 1000 / 60 / 60 / 24;
                                if (day >= 8) {
                                    backCt = 1L;
                                }
                            }

                        } else {
                            //	如果状态中的末次登录日期为 null，将 uuCt 置为 1，backCt 置为 0，并将状态中的末次登录日期更新为当日。
                            uuCt = 1L;
                            lastLoginDateState.update(curLoginDate);
                        }
                        if (uuCt != 0L || backCt != 0L) {
                            collector.collect(new UserLoginBean("", "", "", backCt, uuCt, ts));
                        }

                    }
                }
        );
//      beanDS.print();
        //todo 6.开窗
        AllWindowedStream<UserLoginBean, TimeWindow> windowDS = beanDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
        //todo 7.聚合
        SingleOutputStreamOperator<UserLoginBean> reduceDS = windowDS.reduce(
                new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        value1.setUuCt(value1.getUuCt()+value2.getUuCt());
                        value1.setBackCt(value1.getBackCt()+value2.getBackCt());
                        return value1;
                    }
                },
                new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<UserLoginBean> iterable, Collector<UserLoginBean> collector) throws Exception {
                        UserLoginBean bean = iterable.iterator().next();
                        bean.setStt(DateFormatUtil.tsToDateTime(timeWindow.getStart()));
                        bean.setEdt(DateFormatUtil.tsToDateTime(timeWindow.getEnd()));
                        bean.setCurDate(DateFormatUtil.tsToDate(timeWindow.getStart()));
                        collector.collect(bean);
                    }
                }
        );
        //todo 8.将聚合结果写到doris
        reduceDS.print();
        reduceDS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_user_user_login_window"));
    }
}
