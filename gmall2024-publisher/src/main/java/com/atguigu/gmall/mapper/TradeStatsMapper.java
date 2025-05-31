package com.atguigu.gmall.mapper;

import com.atguigu.gmall.bean.TradeProvinceOrderAmount;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

/*
    交易域统计接口
 */
public interface TradeStatsMapper {
    // 获取某天总交易额
    @Select("select sum(order_amount) order_amount from dws_trade_province_order_window partition par#{date}")
    BigDecimal selectGMV(Integer date);

    // 获取某天各个省份的交易额
    @Select("select province_name,sum(order_amount) order_amount from dws_trade_province_order_window partition par#{date}\n" +
            "group by province_name")
    List<TradeProvinceOrderAmount> selectProvinceAmount(Integer date);
}
