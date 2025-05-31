package com.atguigu.gmall.service.impl;

import com.atguigu.gmall.bean.TrafficUvCt;
import com.atguigu.gmall.mapper.TrafficStatsMapper;
import com.atguigu.gmall.service.TrafficStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

/*
    流量域统计Service接口的实现类
 */
@Service
public class TrafficStatsServiceImpl implements TrafficStatsService {
    @Autowired
    private TrafficStatsMapper trafficStatsMapper;

    @Override
    public List<TrafficUvCt> getChUvCt(Integer date, Integer limit) {
        return trafficStatsMapper.selectChUvCt(date, limit);
    }
}
