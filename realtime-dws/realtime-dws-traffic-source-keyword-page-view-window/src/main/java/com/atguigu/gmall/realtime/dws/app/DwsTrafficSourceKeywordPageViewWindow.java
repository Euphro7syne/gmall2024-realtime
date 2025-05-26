package com.atguigu.gmall.realtime.dws.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import com.atguigu.gmall.realtime.dws.function.KeywordUDTF;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/*
    搜索关键词的聚合统计

    需要启动的进程：
    DwdBaseLog,
    DwsTrafficSourceKeywordPageViewWindow,
    zk,
    kafka,
    flume,


 */
public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(
                10021,
                4,
                "dws_traffic_source_keyword_page_view_window"
        );
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //todo 注册自定义函数到表执行环境中
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        //todo  从页面日志事实表中读取数据 创建动态表  并指定Watermark的生成策略以及提取事件时间字段
        tableEnv.executeSql("create table page_log(\n" +
                "\tcommon map<string,string>,\n" +
                "\tpage map<string,string>,\n" +
                "\tts bigint,\n" +
                "\tet as TO_TIMESTAMP_LTZ(ts,3),\n" +
                "\tWATERMARK FOR et AS et \n" +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE,"dws_traffic_source_keyword_page_view_window"));
//        tableEnv.executeSql("select * from page_log").print();

        //todo 过滤搜索行为
        Table searchTable = tableEnv.sqlQuery("select \n" +
                "\tpage['item'] fullword,\n" +
                "\tet\n" +
                "from page_log\n" +
                "where page['last_page_id'] = 'search' and page['item_type'] = 'keyword' and page['item'] is not null");
//        searchTable.execute().print();
        tableEnv.createTemporaryView("search_table",searchTable);

        //todo 分词 调用自定义分词函数  并和原表的其他字段进行join
        Table splitTable = tableEnv.sqlQuery("SELECT keyword,et FROM search_table,\n" +
                "LATERAL TABLE(ik_analyze(fullword)) t(keyword)");

        tableEnv.createTemporaryView("split_table", splitTable);
//        tableEnv.executeSql("select * from split_table").print();

        //todo 分组、开窗、聚合
        Table resTable = tableEnv.sqlQuery("select\n" +
                "\tdate_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "\tdate_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "\tdate_format(window_end, 'yyyy-MM-dd') cur_date,\n" +
                "\tkeyword,\n" +
                "\tcount(*) keyword_count\n" +
                "from TABLE(\n" +
                "\tTUMBLE(TABLE split_table,DESCRIPTOR(et), INTERVAL '10' second))\n" +
                "group by window_start,window_end,keyword;");
//        resTable.execute().print();
        //todo 将聚合的结果写到doris 中
        tableEnv.executeSql("create table dws_traffic_source_keyword_page_view_window(" +
                "  stt string, " +  // 2023-07-11 14:14:14
                "  edt string, " +
                "  cur_date string, " +
                "  keyword string, " +
                "  keyword_count bigint " +
                ")with(" +
                " 'connector' = 'doris'," +
                " 'fenodes' = '" + Constant.DORIS_FE_NODES + "'," +
                "  'table.identifier' = '" + Constant.DORIS_DATABASE + ".dws_traffic_source_keyword_page_view_window'," +
                "  'username' = 'root'," +
                "  'password' = '000000', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.buffer-count' = '4', " +
                "  'sink.buffer-size' = '4086'," +
                "  'sink.enable-2pc' = 'false', " + // 测试阶段可以关闭两阶段提交,方便测试
                "  'sink.properties.read_json_by_line' = 'true' " +
                ")");
        resTable.executeInsert("dws_traffic_source_keyword_page_view_window");

    }
}
