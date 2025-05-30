package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.common.function.DimMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import com.codahale.metrics.JvmAttributeGaugeSet;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;
import sun.reflect.generics.visitor.Reifier;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/*
    sku粒度下单业务过程聚合统计
        维度：sku
        度量：原始金额，优惠券减免金额，活动减免金额，实付金额


    zk , kf,maxwell,hdfs,hbase,redis,doris,DwdTradeOrderDetail
 */
public class DwsTradeSkuOrderWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeSkuOrderWindow().start(
                10029,
                4,
                "dws_trade_sku_order_window",
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
                            collector.collect(JSON.parseObject(jsonStr));
                        }
                    }
                }
        );
        //4> {"create_time":"2025-05-28 09:56:22","sku_num":"1","split_original_amount":"129.0000","split_coupon_amount":"30.0",
        // "sku_id":"26","date_id":"2025-05-28","coupon_id":"1","user_id":"5093","province_id":"22","sku_name":"索芙特i-Softto 口红不掉色唇膏保湿滋润 璀璨金钻哑光唇膏 Y01复古红 百搭气质 璀璨金钻哑光唇膏 ",
        // "id":"26712","order_id":"19197","split_activity_amount":"0.0","split_total_amount":"99.0","ts":1748483782}

//        jsonObjDS.print();
        //todo 2.按照唯一键（订单明细的id）进行分组
        KeyedStream<JSONObject, String> orderDetailKeyedDS = jsonObjDS.keyBy(jsonObject -> jsonObject.getString("id"));

        //todo 3.去重
        //去重方式1：状态 + 定时器  缺点：时效性差（因为要等5s）  优点：如果出现重复，只会向下游发送一条数据
        /*
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    // 状态编程 定义一个状态
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<JSONObject>("lastJsonObjState", JSONObject.class);
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        //  从状态中获取上次接收到的json对象
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj == null) {
                            //说明没有重复，将当前接受到的这条json数据放到状态中，并注册5s后执行的定时器
                            lastJsonObjState.update(jsonObject);
                            long currentProcessingTime = context.timerService().currentProcessingTime();
                            context.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                        } else {
                            //说明没有重复， 用当前数据的聚合时间和状态中的数据聚合时间进行比较 将时间大的放到状态中
                            String lastTs = lastJsonObj.getString("聚合事件戳");
                            String curTs = jsonObject.getString("聚合事件戳");
                            if (curTs.compareTo(lastTs) >= 0) {
                                lastJsonObjState.update(jsonObject);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        //当定时器被触发执行的时候，将状态中的数据发送到下游，并清除状态
                        JSONObject jsonObj = lastJsonObjState.value();
                        out.collect(jsonObj);
                        lastJsonObjState.clear();
                    }
                }
        );
        distinctDS.print();
         */

        //  去重方式2：状态 + 抵消  优点：时效性好   缺点：如果出现重复，需要向下游传递三条数据，属于数据膨胀
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailKeyedDS.process(
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
                            //4> {"create_time":"2025-05-28 09:56:22","sku_num":"1","split_original_amount":"129.0000","split_coupon_amount":"30.0",
                            // "sku_id":"26","date_id":"2025-05-28","coupon_id":"1","user_id":"5093","province_id":"22","sku_name":"索芙特i-Softto  ",
                            // "id":"26712","order_id":"19197","split_activity_amount":"0.0","split_total_amount":"99.0","ts":1748483782}

                            String splitOriginalAmount = lastJsonObj.getString("split_original_amount");
                            String splitCouponAmount = lastJsonObj.getString("split_coupon_amount");
                            String splitActivityAmount = lastJsonObj.getString("split_activity_amount");
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");

                            lastJsonObj.put("split_original_amount", "-" + splitOriginalAmount);
                            lastJsonObj.put("split_coupon_amount", "-" + splitCouponAmount);
                            lastJsonObj.put("split_activity_amount", "-" + splitActivityAmount);
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
        SingleOutputStreamOperator<TradeSkuOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TradeSkuOrderBean>() {

                    @Override
                    public TradeSkuOrderBean map(JSONObject jsonObject) throws Exception {
                        //4> {"create_time":"2025-05-28 09:56:22","sku_num":"1","split_original_amount":"129.0000","split_coupon_amount":"30.0",
                        // "sku_id":"26","date_id":"2025-05-28","coupon_id":"1","user_id":"5093","province_id":"22","sku_name":"索芙特i-Softto  ",
                        // "id":"26712","order_id":"19197","split_activity_amount":"0.0","split_total_amount":"99.0","ts":1748483782}
                        String skuId = jsonObject.getString("sku_id");
                        BigDecimal splitOriginalAmount = jsonObject.getBigDecimal("split_original_amount");
                        BigDecimal splitCouponAmount = jsonObject.getBigDecimal("split_coupon_amount");
                        BigDecimal splitActivityAmount = jsonObject.getBigDecimal("split_activity_amount");
                        BigDecimal splitTotalAmount = jsonObject.getBigDecimal("split_total_amount");
                        Long ts = jsonObject.getLong("ts") * 1000;
                        TradeSkuOrderBean orderBean = TradeSkuOrderBean.builder()
                                .skuId(skuId)
                                .originalAmount(splitOriginalAmount)
                                .couponReduceAmount(splitCouponAmount)
                                .activityReduceAmount(splitActivityAmount)
                                .orderAmount(splitTotalAmount)
                                .ts(ts)
                                .build();

                        return orderBean;
                    }
                }
        );
//        beanDS.print();

        //todo 6.分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS = beanDS.keyBy(TradeSkuOrderBean::getSkuId);

        //todo 7.开窗
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS = skuIdKeyedDS.window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        //todo 8.聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                        value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> iterable, Collector<TradeSkuOrderBean> collector) throws Exception {
                        TradeSkuOrderBean orderBean = iterable.iterator().next();
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        collector.collect(orderBean);
                    }
                }
        );
//        reduceDS.print();

        //todo 9.关联sku维度
        /*
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
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
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        //根据流中的对象获取要关联维度的主键
                        String skuId = orderBean.getSkuId();
                        //根据维度的主键到hbase维度表中获取对应的唯独对象
                        //id,spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,is_sale,create_time
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId, JSONObject.class);
                        //将维度对象相关的维度属性补充到流中对象上
                        orderBean.setSkuName(skuInfoJsonObj.getString("sku_name"));
                        orderBean.setSpuId(skuInfoJsonObj.getString("spu_id"));
                        orderBean.setCategory3Id(skuInfoJsonObj.getString("category3_id"));
                        orderBean.setTrademarkId(skuInfoJsonObj.getString("tm_id"));
                        return orderBean;
                    }
                }
        );
        withSkuInfoDS.print();

         */

        /*
        // 优化1：旁路缓存
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
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

                    @Override
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        //根据流中对象获取要关联的主键
                        String skuId = orderBean.getSkuId();

                        //根据维度的主键，先到Redis中查询维度
                        JSONObject dimJsonObj = RedisUtil.readDim(jedis, "dim_sku_info", skuId);
                        if (dimJsonObj != null) {
                            //如果在redis中找到了对应的维度数据(缓存命中)，直接作为查询结果返回
                            System.out.println("=====从Redis中查询数据======");
                        }else {
                            //如果在redis中没有找到对应的维度数据，发送请求到Hbase中查询对应的维度，
                            dimJsonObj=HBaseUtil.getRow(hbaseConn,Constant.HBASE_NAMESPACE,"dim_sku_info",skuId,JSONObject.class);
                            if (dimJsonObj != null) {
                                //并将查出来的维度放到redis中缓存起来
                                System.out.println("=====从HBase中查询数据======");
                                RedisUtil.writeDim(jedis,"dim_sku_info",skuId,dimJsonObj);
                            }else {
                                System.out.println("=====没有找到要关联的维度======");
                            }
                        }
                        //将维度对象相关的维度属性补充到流中的对象里
                        if(dimJsonObj!=null){
                            orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                            orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                            orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                            orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                        }
                        return orderBean;
                    }
                }
        );
//        withSkuInfoDS.print();

         */

        /*
        // ========== 使用 旁路缓存模板  去关联维度=================
        //todo 9.关联sku维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(
                new DimMapFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                        orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                        orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getSkuId();
                    }
                }
        );
        withSkuInfoDS.print();
         */


        //优化2：异步IO
        //  将异步IO操作应用于DataStream作为DataStream的一次转换操作
        /*
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                reduceDS,
                //如何发送异步请求： 实现分发请求的AsyncFunction
                new RichAsyncFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private AsyncConnection hbaseAsyncConn;
                    private StatefulRedisConnection<String,String> redisAsyncConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseAsyncConn = HBaseUtil.getHBaseAsyncConnection();
                        redisAsyncConn = RedisUtil.getRedisAsyncConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeAsyncHbaseConnection(hbaseAsyncConn);
                        RedisUtil.closeRedisAsyncConnection(redisAsyncConn);
                    }

                    @Override
                    public void asyncInvoke(TradeSkuOrderBean orderBean, ResultFuture<TradeSkuOrderBean> resultFuture) throws Exception {
                        //根据流中对象获取要关联的主键
                        String skuId = orderBean.getSkuId();
                        //根据维度的主键，先到Redis中查询维度
                        JSONObject dimJsonObj = RedisUtil.readDimAsync(redisAsyncConn, "dim_sku_info", skuId);
                        if (dimJsonObj != null) {
                            //如果在redis中找到了对应的维度数据(缓存命中)，直接作为查询结果返回
                            System.out.println("=====从Redis中获取维度数据======");
                        }else {
                            //如果在redis中没有找到对应的维度数据，发送请求到Hbase中查询对应的维度，
                            dimJsonObj=HBaseUtil.readDimAsync(hbaseAsyncConn,Constant.HBASE_NAMESPACE,"dim_sku_info",skuId);
                            if (dimJsonObj != null) {
                                System.out.println("=====从HBase中获取维度数据======");
                                //并将查出来的维度放到redis中缓存起来
                                RedisUtil.writeDimAsync(redisAsyncConn,"dim_sku_info",skuId,dimJsonObj);
                            }else {
                                System.out.println("=====维度数据没有找到======");
                            }
                        }



                        //将维度对象相关的维度属性补充到流中的对象里
                        if (dimJsonObj != null) {
                            orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                            orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                            orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                            orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                        }
                        //获取数据库交互的结果并发生给ResultFuture的回调函数，将关联后的数据传递到下游
                        resultFuture.complete(Collections.singleton(orderBean));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
//        withSkuInfoDS.print();

         */
        //异步IO + 模板
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                        orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                        orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getSkuId();
                    }
                },
                60,
                TimeUnit.SECONDS

        );
//        withSkuInfoDS.print();

        //todo 10.关联spu维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDS = AsyncDataStream.unorderedWait(
                withSkuInfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setSpuName(dimJsonObj.getString("spu_name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_spu_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getSpuId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //todo 11.关联tm维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withTmDS = AsyncDataStream.unorderedWait(
                withSpuInfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {

                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setTrademarkName(dimJsonObj.getString("tm_name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getTrademarkId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //todo 12.关联category3维度
        SingleOutputStreamOperator<TradeSkuOrderBean> c3Stream = AsyncDataStream.unorderedWait(
                withTmDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getCategory3Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category3";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setCategory3Name(dim.getString("name"));
                        bean.setCategory2Id(dim.getString("category2_id"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );


        //todo 13.关联category2维度
        SingleOutputStreamOperator<TradeSkuOrderBean> c2Stream = AsyncDataStream.unorderedWait(
                c3Stream,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getCategory2Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category2";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setCategory2Name(dim.getString("name"));
                        bean.setCategory1Id(dim.getString("category1_id"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );


        //todo 14.关联category1维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withC1DS = AsyncDataStream.unorderedWait(
                c2Stream,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getCategory1Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category1";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setCategory1Name(dim.getString("name"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

//        withC1DS.print();

        //todo  15.将关联的结果写道Doris中 将流中的数据转换成Json字符串格式
        withC1DS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_sku_order_window"));

    }
}
