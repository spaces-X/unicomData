package com.test.common.redis;

import com.common.redis.RedisConnection;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;


public class RedisConnectionTest {
    private RedisConnection redisConnection;


    @Before
    public void before(){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        // 设置最大连接数
        jedisPoolConfig.setMaxTotal(50);
        jedisPoolConfig.setMaxTotal(50);
        //设置 redis 连接池最大空闲连接数量
        jedisPoolConfig.setMaxIdle(10);
        //设置 redis 连接池最小空闲连接数量
        jedisPoolConfig.setMinIdle(1);
        redisConnection = new RedisConnection();
        redisConnection.setIp("");
        redisConnection.setPort(52981);
        redisConnection.setPwd("");
        redisConnection.setClientName(Thread.currentThread().getName());
        redisConnection.setTimeOut(600);
        redisConnection.setJedisPoolConfig(jedisPoolConfig);

    }
    @Test
    public void testPutGet() {
        Jedis jedis = redisConnection.getJedis();

    }

}
