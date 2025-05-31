package com.atguigu.gmall.service;

import com.atguigu.gmall.bean.TradeProvinceOrderAmount;

import java.math.BigDecimal;
import java.util.List;

/*
    交易域统计service接口
 */
public interface TradeStatsService {
    //获取某天总交易额
    BigDecimal getGMV(Integer date);

    //获取某天各个省份的交易额
    List<TradeProvinceOrderAmount> getProvinceAmount(Integer date);
}
