import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Test01 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(5000L);
//        tableEnv.executeSql("CREATE TABLE flink_doris (  " +
//                "    siteid INT,  " +
//                "    citycode SMALLINT,  " +
//                "    username STRING,  " +
//                "    pv BIGINT  " +
//                "    )   " +
//                "    WITH (  " +
//                "      'connector' = 'doris',  " +
//                "      'fenodes' = 'hadoop102:7030',  " +
//                "      'table.identifier' = 'test.table1',  " +
//                "      'username' = 'root',  " +
//                "      'password' = '000000'  " +
//                ")  "
//        );



        tableEnv.executeSql("CREATE TABLE flink_doris (  " +
                "    siteid INT,  " +
                "    citycode INT,  " +
                "    username STRING,  " +
                "    pv BIGINT  " +
                ")WITH (" +
                "  'connector' = 'doris', " +
                "  'fenodes' = 'hadoop102:7030', " +
                "  'table.identifier' = 'test.table1', " +
                "  'username' = 'root', " +
                "  'password' = '000000', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.buffer-count' = '4', " +
                "  'sink.buffer-size' = '4086'," +
                "  'sink.enable-2pc' = 'false'" + // 测试阶段可以关闭两阶段提交,方便测试
                ")  ");

        tableEnv.executeSql("insert into flink_doris values(33, 3, '深圳', 3333)");
//        tableEnv.sqlQuery("select * from flink_doris").execute().print();

    }
}
