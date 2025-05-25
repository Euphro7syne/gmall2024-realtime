package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/*
    获取相关的sink的工具类
 */
public class FlinkSinkUtil {

    //获取Kafka的sink
    public static KafkaSink<String> getKafkaSink(String topic){
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                // 当前配置决定是否开启事务，保证写道Kafka数据的精准一次
                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 设置事务ID的前缀
                //.setTransactionalIdPrefix("dwd_base_log_")
                // 设置事务的超时时间  检查点超时时间 < 事务的超时时间 <= 事务最大超时时间
                //.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"")
                .build();
        return kafkaSink;
    }

    public static KafkaSink<Tuple2<JSONObject, TableProcessDwd>> getKafkaSink(){
        KafkaSink<Tuple2<JSONObject, TableProcessDwd>> kafkaSink = KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> jsonObjectTableProcessDwdTuple2, KafkaSinkContext kafkaSinkContext, Long aLong) {
                        JSONObject jsonObject = jsonObjectTableProcessDwdTuple2.f0;
                        TableProcessDwd tableProcessDwd = jsonObjectTableProcessDwdTuple2.f1;
                        String topic = tableProcessDwd.getSinkTable();

                        return new ProducerRecord<byte[], byte[]>(topic, jsonObject.toJSONString().getBytes());
                    }
                })
                // 当前配置决定是否开启事务，保证写道Kafka数据的精准一次
                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 设置事务ID的前缀
                //.setTransactionalIdPrefix("dwd_base_log_")
                // 设置事务的超时时间  检查点超时时间 < 事务的超时时间 <= 事务最大超时时间
                //.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"")
                .build();
        return kafkaSink;
    }

    // 扩展
    //如果流中的数据类型不确定，如何将数据写到Kafka

    public static <T>KafkaSink<T> getKafkaSink(KafkaRecordSerializationSchema<T> ksr){
        KafkaSink<T> kafkaSink = KafkaSink.<T>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(ksr)
                // 当前配置决定是否开启事务，保证写道Kafka数据的精准一次
                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 设置事务ID的前缀
                //.setTransactionalIdPrefix("dwd_base_log_")
                // 设置事务的超时时间  检查点超时时间 < 事务的超时时间 <= 事务最大超时时间
                //.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"")
                .build();
        return kafkaSink;
    }

}
