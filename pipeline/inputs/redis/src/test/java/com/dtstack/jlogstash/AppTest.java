package com.dtstack.jlogstash;

import com.dtstack.jlogstash.inputs.ChannelListener;
import com.dtstack.jlogstash.utils.JedisPoolUtils;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.*;

/**
 * Unit test for simple App.
 */
public class AppTest {

    private Jedis jedis;
    private String host = "localhost";
    private int port = 6379;
    private int db = 1;
    private boolean isStop = false;
    private static String script_sha;

    private void initData() {
        jedis.lpush("list1", "one", "two", "three", "four");
    }

    public static void main(String args[]) {
        AppTest appTest = new AppTest();
        appTest.testApp();
    }


    public void testApp() {
        initJedis();
        testConnection();
        //stringRunner("str1");
        //setRunner("set1");
        //initData();
        //setRunner("set1");
        //sortedSetRunner("set2",0);
        //hashRunner("h1");
        //channel("channel1");
        channelPatternRunner("ch");
    }

    private void channelPatternRunner(String channel){
        TestPubSub listener = new TestPubSub();
        jedis.psubscribe(listener,channel);
    }

    private void channel(String channel) {
        TestPubSub listener = new TestPubSub();
        jedis.subscribe(listener, channel);
        /*while(true){
            jedis.publish("channel1","haha");
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }*/
    }

    private void hashRunner(String key) {
        //while (!isStop) {
        Map<String, String> data = jedis.hgetAll(key);
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(key, data);
        System.out.println(map.toString());
        //}
    }

    private void sortedSetRunner(String key, int data_size) {
        //while (!isStop) {
        Set<String> data = jedis.zrange(key, 0, data_size - 1);
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(key, data);
        System.out.println(map.toString());
        //}
    }

    /**
     *
     */
    private void getList(String key, int data_size) {
        List<String> data = jedis.lrange(key, 0, data_size - 1);
        jedis.ltrim(key, data_size + 1, -1);
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(key, data);
        System.out.println(map.toString());
    }

    private void stringRunner(String key) {
        while (!isStop) {
            String data = jedis.get(key);
            Map<String, Object> map = new HashMap<String, Object>();
            map.put(key, data);
            System.out.println(map.toString());
        }
    }

    private void setRunner(String key) {
        //while (!isStop) {
        Set<String> data = jedis.smembers(key);
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(key, data);
        System.out.println(map.toString());
        //}
    }

    public void initJedis() {
        JedisPoolUtils.init(host, port, 0);
        jedis = JedisPoolUtils.getJedis();
    }

    public void testConnection() {
        if (jedis.ping().equals("PONG")) {
            System.out.println("---------链接成功---------");
        } else {
            System.out.println("---------链接失败---------");
            System.exit(-1);
        }
    }
}
