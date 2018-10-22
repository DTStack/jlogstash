package com.dtstack.jlogstash.inputs;

import redis.clients.jedis.JedisPubSub;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017/6/19.
 */
public class ChannelListener extends JedisPubSub {
    private Redis redis;

    public ChannelListener(Redis redis){
        this.redis = redis;
    }

    @Override
    public void onMessage(String channel, String message) {
        Map<String,Object> map = new HashMap<String, Object>();
        map.put(channel,message);
        redis.process(map);
    }

    @Override
    public void onPMessage(String pattern, String channel, String message) {
        Map<String,Object> map = new HashMap<String, Object>();
        map.put(channel,message);
        redis.process(map);
    }
}
