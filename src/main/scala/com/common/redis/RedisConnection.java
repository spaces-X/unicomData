package com.common.redis;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;



@Data
public class RedisConnection {

    /**
     *  redis 连接池配置信息
     */
    private JedisPoolConfig jedisPoolConfig;

    /**
     *  redis 服务器地址
     */
    private String ip;

    /**
     *  redis 端口
     */
    private Integer port;

    /**
     * redis 服务密码
     */

    private String pwd;

    /**
     * redis 连接超时
     */
    private Integer timeOut;
    /**
     * redis 客户端名称
     */
    private String clientName;

    private JedisPool jedisPool;

    private void buildConnection() {
        if (jedisPool == null) {
            if (jedisPoolConfig == null) {
                jedisPool = new JedisPool(new JedisPoolConfig(), ip, port, timeOut, pwd, 0, clientName);
            } else {
                jedisPool = new JedisPool(jedisPoolConfig, ip, port, timeOut, pwd, 0, clientName);
            }
        }
    }
    public Jedis getJedis() {
        buildConnection();
        if (jedisPool != null) {
            return jedisPool.getResource();
        }
        return null;
    }




}
