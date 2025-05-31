package com.atguigu.gmall.service;

import com.atguigu.gmall.bean.TrafficUvCt;

import java.util.List;

/*
    流量域统计Service接口
 */
public interface TrafficStatsService {
    //获取某天各个渠道的独立访客
    List<TrafficUvCt> getChUvCt(Integer date, Integer limit);
}
