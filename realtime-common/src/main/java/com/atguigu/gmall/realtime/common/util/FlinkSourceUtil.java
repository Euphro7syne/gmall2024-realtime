package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.util.Properties;

public class FlinkSourceUtil {
    // todo 获取KafkaSource
    public static KafkaSource<String> getKafkaSource(String topic, String groupId){
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(groupId)
                // 在生产环境中，一般为了保证消费的精准一次性，需要手动去维护偏移量，KafkaSource -> kafkasourcereader -> 存储偏移量变量
                //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setStartingOffsets(OffsetsInitializer.latest()) //从最末尾位点开始消费
                //注意：如果使用Flink提供的SimpleStreamSchema对String类型的消息进行反序列化，如果消息为空，底层会报错
                //.setValueOnlyDeserializer(new SimpleStringSchema())
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] message) throws IOException {
                                if (message != null) {
                                    return new String(message);
                                }
                                return null;
                            }

                            @Override
                            public boolean isEndOfStream(String string) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        }
                )
                .build();
        return kafkaSource;
    }

    // todo 获取MySQLSource
    public static MySqlSource<String> getMySqlSource(String database,String tableName){
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT).
                databaseList(database).
                tableList(database+"."+tableName).
                username(Constant.MYSQL_USER_NAME).
                password(Constant.MYSQL_PASSWORD).
                deserializer(new JsonDebeziumDeserializationSchema())//反序列化
                .startupOptions(StartupOptions.initial()) //读取历史数据
                .jdbcProperties(props)
                .build();
        return mySqlSource;
    }
}
