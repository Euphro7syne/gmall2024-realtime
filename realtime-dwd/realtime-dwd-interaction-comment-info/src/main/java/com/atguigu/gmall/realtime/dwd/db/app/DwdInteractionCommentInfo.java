package com.atguigu.gmall.realtime.dwd.db.app;

/*
    评论事实表
    流程序需要设置手动提交作业
    sql程序不需要手动设置，会自动提交
 */

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import com.sun.org.apache.bcel.internal.Const;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
      new DwdInteractionCommentInfo().start(10012,4,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //todo 从Kafka的topic_db主题中读取数据 创建动态表    ---kafka 连接器
        readOdsDb(tableEnv,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

        //todo 过滤出评论数据                              --where table='comment_info' type='insert'
        Table commentInfo = tableEnv.sqlQuery("select\n" +
                "\t`data`['id'] id,\n" +
                "\t`data`['user_id'] user_id,\n" +
                "\t`data`['sku_id'] sku_id,\n" +
                "\t`data`['appraise'] appraise,\n" +
                "\t`data`['comment_txt'] comment_txt,\n" +
                "\tts,\n" +
                "\tproc_time\n" +
                "from topic_db where `table`='comment_info' and `type`='insert'");
//        commentInfo.execute().print();
        //将表对象注册到表执行环境中
        tableEnv.createTemporaryView("comment_info", commentInfo);

        //todo 从HBase中读取字典数据 创建动态表                --hbase 连接器
        readHBaseDic(tableEnv);


        //todo 将评论表和字典表进行关联                      ---lookup join
        Table joinedTable = tableEnv.sqlQuery("SELECT \n" +
                "\tid,\n" +
                "\tuser_id,\n" +
                "\tsku_id,\n" +
                "\tappraise,\n" +
                "\tdic.dic_name appraise_name,\n" +
                "\tcomment_txt,\n" +
                "\tts\n" +
                "FROM comment_info AS c\n" +
                "  JOIN base_dic FOR SYSTEM_TIME AS OF c.proc_time AS dic\n" +
                "    ON c.appraise = dic.dic_code;");
//        joinedTable.execute().print();
        //todo 将关联的数据写到Kafka主题中                   -- upsert kafka连接器
        //创建动态表和要写入的的主题进行映射
        tableEnv.executeSql("CREATE TABLE "+ Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO +" (\n" +
                "  id string,\n" +
                "  user_id string,\n" +
                "  sku_id string,\n" +
                "  appraise string,\n" +
                "  appraise_name string,\n" +
                "  comment_txt string,\n" +
                "  ts bigint,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") " + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        //写入
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

    }



}
