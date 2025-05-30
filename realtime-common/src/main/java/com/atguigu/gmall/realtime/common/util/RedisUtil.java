package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import jdk.nashorn.internal.runtime.JSErrorType;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.concurrent.ExecutionException;

/*
    操作Redis的工具类
 */
public class RedisUtil {

    private static JedisPool jedisPool;
    static {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMinIdle(5);
        poolConfig.setMaxTotal(100);
        poolConfig.setMaxIdle(5);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxWaitMillis(2000);
        poolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(poolConfig,"hadoop103",6379,10000);
    }

    //todo 获取Jedis
    public static Jedis getJedis(){
        System.out.println("=====获取jedis客户端===");
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }

    //todo 关闭Jedis
    public static void closeJedis(Jedis jedis){
        System.out.println("=====获取jedis客户端===");
        if(jedis!=null){
            jedis.close();
        }
    }


    // todo 获取异步操作Redis的连接对象
    public static StatefulRedisConnection<String, String> getRedisAsyncConnection(){
        System.out.println("=====获取异步操作Redis的客户端===");
        RedisClient redisClient = RedisClient.create("redis://hadoop103:6379/0");
        return redisClient.connect();
    }


    // todo 关闭异步操作Redis的连接对象
    public static void   closeRedisAsyncConnection(StatefulRedisConnection<String, String> asyncRedisConnection) {
        if (asyncRedisConnection != null && asyncRedisConnection.isOpen()) {
            asyncRedisConnection.close();
            System.out.println("=====已关闭异步操作Redis的客户端===");
        }
    }
    //todo 以异步的方式从Redis中取数据
    public static JSONObject readDimAsync(StatefulRedisConnection<String, String> asyncRedisConnection,String tableName,String id){
        RedisAsyncCommands<String, String> asyncCommands = asyncRedisConnection.async();
        String key = getKey(tableName, id);
        try {
            String dimJsonStr = asyncCommands.get(key).get();
            if(StringUtils.isNotEmpty(dimJsonStr)){
                JSONObject dimJsonObj = JSON.parseObject(dimJsonStr);
                return dimJsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }


    //todo 以异步的方式从Redis中放数据
    public static void writeDimAsync(StatefulRedisConnection<String, String> asyncRedisConnection,String tableName,String id,JSONObject dimJsonObj){
        RedisAsyncCommands<String, String> asyncCommands = asyncRedisConnection.async();
        String key = getKey(tableName, id);
        asyncCommands.setex(key,24*60*60,dimJsonObj.toJSONString());
    }



    //todo 从Redis中取数据
    public static JSONObject readDim(Jedis jedis,String tableName,String id){
        // 拼接key
        String key = getKey(tableName, id);
        //根据Key到redis中获取维度数据
        String dimJsonStr = jedis.get(key);
        if (StringUtils.isNotEmpty(dimJsonStr)) {
            JSONObject dimJsonObj = JSON.parseObject(dimJsonStr);
            return dimJsonObj;
        }
        return null;
    }

    public static String getKey(String tableName, String id) {
        String key = tableName +":"+ id;
        return key;
    }
    //todo 从Redis中放数据
    public static void writeDim(Jedis jedis,String tableName,String id,JSONObject dimJsonObj){
        String key = getKey(tableName, id);
        //setex 在放数据的同时设定冷数据的释放时间，避免占内存
        jedis.setex(key,24*60*60,dimJsonObj.toJSONString());

    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();
        String pong = jedis.ping();
        System.out.println(pong);
        closeJedis(jedis);
    }

}
