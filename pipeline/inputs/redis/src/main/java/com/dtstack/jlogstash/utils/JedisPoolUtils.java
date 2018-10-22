package com.dtstack.jlogstash.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import java.net.URI;


public class JedisPoolUtils {
    private static JedisPool pool;

    private static URI uri;

    private static int maxIdle = 30;

    private static int maxTotal = 50;

    private static long maxWaitMills = 1000;

    private static String password = null;

    private static int timeout = Protocol.DEFAULT_TIMEOUT;

    public static int getMaxIdle() {
        return maxIdle;
    }

    public static void setMaxIdle(int maxIdle) {
        JedisPoolUtils.maxIdle = maxIdle;
    }

    public static int getMaxTotal() {
        return maxTotal;
    }

    public static void setMaxTotal(int maxTotal) {
        JedisPoolUtils.maxTotal = maxTotal;
    }

    public static long getMaxWaitMills() {
        return maxWaitMills;
    }

    public static void setMaxWaitMills(long maxWaitMills) {
        JedisPoolUtils.maxWaitMills = maxWaitMills;
    }

    public static String getPassword() {
        return password;
    }

    public static void setPassword(String password) {
        JedisPoolUtils.password = password;
    }

    public static int getTimeout() {
        return timeout;
    }

    public static void setTimeout(int timeout) {
        JedisPoolUtils.timeout = timeout;
    }

    /**
     * 建立连接池 真实环境，一般把配置参数缺抽取出来。
     *
     */
    private static void createJedisPool() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(30);
        config.setMaxTotal(50);
        config.setMaxWaitMillis(1000);

        pool = new JedisPool(config,uri);
    }

    public static void init(String host, int port, int database) {

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(maxIdle);
        config.setMaxTotal(maxTotal);
        config.setMaxWaitMillis(maxWaitMills);
        pool = new JedisPool(config, host, port, timeout, password, database, null);
    }

    /**
     * 在多线程环境同步初始化
     */
    public static synchronized void poolInit() {
        if (pool == null)
            createJedisPool();
    }

    /**
     * 获取一个jedis 对象
     *
     * @return
     */
    public static Jedis getJedis() {
        if (pool == null)poolInit();
        return pool.getResource();
    }


    public static URI getUri() {
        return uri;
    }

    public static void setUri(URI uri) {
        JedisPoolUtils.uri = uri;
    }

    public static JedisPool getJedisPool(){
        return pool;
    }

}
