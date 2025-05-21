package com.atguigu.gmall.realtime.dim.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test01_FlinkCDC {
    // todo 该案例演示FlinkCDC的使用
    public static void main(String[] args) throws Exception {

        // todo 1. 准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // todo 2. 设置并行度
        env.setParallelism(1);
        // todo 开启检查点
        env.enableCheckpointing(3000);

        // todo 3.使用FlinkCDC读取MySQL表中的数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall2024_config") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList("gmall2024_config.test") // set captured table
                .username("root")
                .password("000000")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        // todo 4.打印输出
        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                .setParallelism(4)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");
        // {"before":null,"after":{"id":1,"name":"zs","age":33},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"test","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},
        // "op":"r","ts_ms":1747825336026,"transaction":null}

        //{"before":null,"after":{"id":3,"name":"ww","age":55},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1747825499000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"test","server_id":1,"gtid":null,"file":"mysql-bin.000006","pos":392,"row":0,"thread":16,"query":null},
        // "op":"c","ts_ms":1747825492812,"transaction":null}

        //{"before":{"id":3,"name":"ww","age":55},"after":{"id":3,"name":"wwww","age":55},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1747825522000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"test","server_id":1,"gtid":null,"file":"mysql-bin.000006","pos":714,"row":0,"thread":16,"query":null},
        // "op":"u","ts_ms":1747825515876,"transaction":null}

        //{"before":{"id":3,"name":"wwww","age":55},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1747825555000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"test","server_id":1,"gtid":null,"file":"mysql-bin.000006","pos":1042,"row":0,"thread":16,"query":null},
        // "op":"d","ts_ms":1747825548988,"transaction":null}
    }
}
