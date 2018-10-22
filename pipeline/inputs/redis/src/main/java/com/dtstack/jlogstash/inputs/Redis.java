package com.dtstack.jlogstash.inputs;

import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.utils.DataType;
import com.dtstack.jlogstash.utils.JedisPoolUtils;
import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * Date: 2017年6月15日
 * Company: www.dtstack.com
 *
 * @author jiangbo
 */

@SuppressWarnings("serial")
public class Redis extends BaseInput {

    @Required(required = true)
    private String host;

    private int port = 6379;

    private int default_db = 0;

    @Required(required = true)
    private String key;

    @Required(required = true)
    private String data_type;

    @Required(required = true)
    private int data_size = 10;

    private boolean isStop = false;

    private Jedis jedis;

    private int sleep_time = 500;

    public Redis(Map config) {
        super(config);
    }

    public void initJedis() {
        JedisPoolUtils.init(host, port, default_db);
        jedis = JedisPoolUtils.getJedis();
    }

    public void prepare() {
        initJedis();
    }

    public void emit() {
        if (data_type.equals(DataType.LIST.getDataType())) {
            listRunner();
        } else if (data_type.equals(DataType.STRING.getDataType())) {
            stringRunner();
        } else if (data_type.equals(DataType.HASH.getDataType())) {
            hashRunner();
        } else if (data_type.equals(DataType.SORTEDSET.getDataType())) {
            sortedSetRunner();
        } else if (data_type.equals(DataType.CHANNEL.getDataType())) {
            channelRunner();
        } else if (data_type.equals(DataType.CHANNEL_PATTERN.getDataType())) {
            channelPatternRunner();
        } else {
            setRunner();
        }
    }

    public void release() {
        if (jedis != null) {
            isStop = true;
            jedis.close();
        }
    }

    /**
     *
     */
    private void hashRunner() {
        while (!isStop) {
            Map<String, String> data = jedis.hgetAll(key);
            Map<String, Object> map = new HashMap<String, Object>();
            map.put(key, data);
            process(map);
            try {
                Thread.sleep(sleep_time);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     */
    private void sortedSetRunner() {
        if (data_size < 0) {
            data_size = 0;
        }
        while (!isStop) {
            Set<String> data = jedis.zrange(key, 0, data_size - 1);
            Map<String, Object> map = new HashMap<String, Object>();
            map.put(key, data);
            process(map);
            try {
                Thread.sleep(sleep_time);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     */
    private void setRunner() {
        while (!isStop) {
            Set<String> data = jedis.smembers(key);
            Map<String, Object> map = new HashMap<String, Object>();
            map.put(key, data);
            process(map);
            try {
                Thread.sleep(sleep_time);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     */
    private void stringRunner() {
        while (!isStop) {
            String data = jedis.get(key);
            Map<String, Object> map = new HashMap<String, Object>();
            map.put(key, data);
            process(map);
            try {
                Thread.sleep(sleep_time);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     */
    private void listRunner() {
        if (data_size < 0) {
            data_size = 0;
        }
        while (!isStop) {
            List<String> data = jedis.lrange(key, 0, data_size - 1);
            jedis.ltrim(key, data_size + 1, -1);
            Map<String, Object> map = new HashMap<String, Object>();
            map.put(key, data);
            process(map);
            try {
                Thread.sleep(sleep_time);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     */
    private void channelRunner() {
        ChannelListener channelListener = new ChannelListener(this);
        jedis.subscribe(channelListener, key);
    }

    /**
     *
     */
    private void channelPatternRunner() {
        ChannelListener channelListener = new ChannelListener(this);
        jedis.psubscribe(channelListener, key);
    }
}
