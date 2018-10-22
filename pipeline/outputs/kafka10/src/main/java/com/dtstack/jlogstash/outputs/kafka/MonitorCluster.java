package com.dtstack.jlogstash.outputs.kafka;

import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitorCluster implements Runnable {
	
	private static final Logger logger = LoggerFactory.getLogger(MonitorCluster.class);
		
	private String zkAddress = "localhost:2181";
	
	private ZkClient zkClient;
	
	private volatile AtomicBoolean isKafkaHealth;

	public MonitorCluster(String zkAddress, AtomicBoolean isKafkaHealth){
		this.zkAddress = zkAddress;
		this.zkClient = new ZkClient(this.zkAddress);
		this.isKafkaHealth = isKafkaHealth;
	}
	
	
	@Override
	public void run() {
		
		if(healthcheck()) {
			isKafkaHealth.set(true);
		} else {
			isKafkaHealth.set(false);
		}
	}
	
	public boolean healthcheck(){
		
		try {
			String brokersIdsPath = getBrokersIds(); 
			List<String> brokersList = zkClient.getChildren(brokersIdsPath);
			if(brokersList == null || brokersList.size() == 0) {
				logger.warn("kafka healthcheck, brokersList is empty");
				return false;
			}
			logger.debug("kafka healthcheck, brokersList is not empty");
			return true;
		} catch(Exception e) {
			logger.error("kafka healthcheck", e);
			return false;
		}
	}
	
	public String getBrokersIds(){
		return "/brokers/ids";
	}

	
}