package com.dtstack.jlogstash.outputs.kafka;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author: haisi
 * @date 2019-06-25 11:09
 */
public class MonitorCluster implements Runnable{

    private static final Logger logger = LoggerFactory.getLogger(MonitorCluster.class);

    private String zkAddress;

    private ZkClient zkClient;

    private volatile AtomicBoolean isKafkaHealth;

    public MonitorCluster(String zkAddress, AtomicBoolean isKafkaHealth) {
        this.zkAddress = zkAddress;
        this.zkClient = new ZkClient(this.zkAddress);
        this.isKafkaHealth = isKafkaHealth;
    }

    @Override
    public void run() {
        if (healthcheck()){
            isKafkaHealth.set(true);
        } else {
            isKafkaHealth.set(false);
        }
    }

    public boolean healthcheck(){
        try{
            String brokersIdsPath = getBrokersIds();
            List<String> brockersList = zkClient.getChildren(brokersIdsPath);
            if (brockersList == null || brockersList.size() == 0){
                logger.warn("kafka11 healthcheck, brokersList is empty");
                return false;
            }
            logger.debug("kafka11 healthcheck, brokersList is not empty");;
            return true;
        }catch (Exception e){
            logger.error("kafka11 healthcheck", e);
            return false;
        }
    }

    public String getBrokersIds(){
        return "/brokers/ids";
    }
}
