package com.atguigu.gmall.realtime.dws.function;

import com.atguigu.gmall.realtime.dws.util.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/*
    自定义UDTF函数
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String text) {
        for (String keyword : KeyWordUtil.analyze(text)) {
            collect(Row.of(keyword));
        }
    }
}
