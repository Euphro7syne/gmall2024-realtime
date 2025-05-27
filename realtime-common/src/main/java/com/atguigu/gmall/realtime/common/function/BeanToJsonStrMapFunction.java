package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.gmall.realtime.common.bean.TrafficPageViewBean;
import org.apache.flink.api.common.functions.MapFunction;

/*
    将流中的对象转换成字符串格式
 */

public class BeanToJsonStrMapFunction<T> implements MapFunction<T, String> {

    @Override
    public String map(T bean) throws Exception {
        SerializeConfig config = new SerializeConfig();
        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSON.toJSONString(bean, config);
    }
}


