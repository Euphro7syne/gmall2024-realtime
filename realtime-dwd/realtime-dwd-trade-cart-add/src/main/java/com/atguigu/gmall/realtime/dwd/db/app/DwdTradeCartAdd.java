package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/*
    加购事实表
 */
public class DwdTradeCartAdd extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeCartAdd().start(
                10013,
                4,
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );

    }
    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //todo 从Kafka的topic_db主题中读取数据 创建动态表
        readOdsDb(tableEnv,Constant.TOPIC_DWD_TRADE_CART_ADD);
        //todo 过滤出加购数据 table= 'cart_info' type='insert', type = 'update' 并且修改的是加购商品的数量，修改后的值比修改前的大
        Table cartInfo = tableEnv.sqlQuery("select\n" +
                "\t`data`['id'] id,\n" +
                "\t`data`['user_id'] user_id,\n" +
                "\t`data`['sku_id'] sku_id,\n" +
                "\t if(type='insert',`data`['sku_num'], CAST((CAST(data['sku_num'] AS INT) - CAST(`old`['sku_num'] AS INT)) AS STRING) ) sku_num,\n" +
                "\tts\n" +
                "from topic_db where `table`='cart_info' \n" +
                "and (\n" +
                "\ttype = 'insert'\n" +
                "\tor\n" +
                "\t(type ='update' and `old`['sku_num'] is not null and (CAST(data['sku_num'] AS INT) > CAST(`old`['sku_num'] AS INT)))\n" +
                ")");
//        cartInfo.execute().print();
        //todo 将过滤出来的加购数据写道kafka主题中
        //创建动态表和要写入的主题进行映射
        tableEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_CART_ADD+"(\n" +
                "\tid string,\n" +
                "\tuser_id string,\n" +
                "\tsku_id string,\n" +
                "\tsku_num string,\n" +
                "\tts bigint,\n" +
                "\tPRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));
        // 写入
        cartInfo.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
}
