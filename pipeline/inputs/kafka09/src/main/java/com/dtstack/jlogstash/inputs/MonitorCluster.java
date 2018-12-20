/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.jlogstash.inputs;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Maps;

/**
 * 监控kafka consumer变化,partitions变化,brokers变化
 * @author xuchao
 *
 */
public class MonitorCluster implements Runnable{
	
	private static final Logger logger = LoggerFactory.getLogger(MonitorCluster.class);
		
	private String zkAddress = "localhost:2181";
	
	private String consumerGroup = "";
	
	private Set<String> topicNameSet;
	
	private ZkClient zkClient;
	
	/** 对consumer数量的监控周期 ms,可以根据实际情况自己设置 */
	private int consumer_moni_period = 3600 * 1000;
	
	/** 对partitions 数量的监控周期ms,可以根据实际情况自己设置 */
	private int partitions_moni_period = 10 * 1000;
	
	/** 上次consumer 信息pull时间 */
	private long consumer_last_check = System.currentTimeMillis();
	
	/** 上次partition 信息pull时间 */
	private long partitions_last_check = System.currentTimeMillis();
	
	private IKafkaChg kafkaChg;
	
	/**当前指定的consumer_group 下的consumer数*/
	private int currConsumers = 0;
	
	private Map<String, Integer> currTopicPartitions = Maps.newHashMap();
	
	private Map<String, Boolean> infoChg = Maps.newHashMap();

	/**是否开启在consumer,partition出现变化的时候做动态平衡*/
	private boolean openBalance = false;
	
	public MonitorCluster(String zkAddress, Set<String> topicNameSet, String consumerGroup, IKafkaChg kafkaChg,
			int consumerMoniPeriod, int partitionsMoniPeriod, boolean openBalance){
		this.zkAddress = zkAddress;
		this.topicNameSet = topicNameSet;
		this.consumerGroup = consumerGroup;
		this.kafkaChg = kafkaChg;
		this.zkClient = new ZkClient(this.zkAddress);
		this.consumer_moni_period = consumerMoniPeriod;
		this.partitions_moni_period = partitionsMoniPeriod;
		this.openBalance = openBalance;

		init();
	}
	
	private void init(){
	}

	@Override
	public void run() {
		try {
			resetChgInfo();
			long currTime = System.currentTimeMillis();
			if(currTime - consumer_last_check > consumer_moni_period){
				pullConsumerInfo();
			}
			
			if(currTime - partitions_last_check > partitions_moni_period){
				pullPartitionInfo();
			}
			
			for(Entry<String, Boolean> tmp : infoChg.entrySet()){
				if(tmp.getValue()){
					Integer topicPartitions = currTopicPartitions.get(tmp.getKey());
					if(topicPartitions == null || topicPartitions == 0){
						logger.error("monitor get an invalid partitions of topic:{}.", tmp.getKey());
						continue;
					}
					
					if(currConsumers == 0){
						logger.info("monitor get consumers num:0,it may be not correct.");
						continue;
					}

					if(openBalance){
						kafkaChg.onInfoChgTrigger(tmp.getKey(), currConsumers, topicPartitions);
					}
				}
			}
			
			healthcheck();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void pullConsumerInfo(){
		
		String idsPath = getidsPath(consumerGroup);
		if(!checkPath(zkClient, idsPath)) return;
		
		List<String> idsList = zkClient.getChildren(idsPath);
		logger.debug("consumer group current have consumer:{}.", idsList);
		
		consumer_last_check = System.currentTimeMillis();
		if(idsList.size() != currConsumers){
			for(String topicName : topicNameSet){
				infoChg.put(topicName, true);
			}
			
			currConsumers = idsList.size();
		}
	}
	
	public void pullPartitionInfo(){
		
		for(String topicName : topicNameSet){
			String partitionsPath = getPartitionsPath(topicName);
			if(!checkPath(zkClient, partitionsPath)) continue;
			
			List<String> partitionList = zkClient.getChildren(partitionsPath);
			logger.info("topic:{} current have partition num:{}.", topicName, partitionList.size());
			partitions_last_check = System.currentTimeMillis();
			
			Integer currPartitions = currTopicPartitions.get(topicName);
			currPartitions = currPartitions == null ? 0 : currPartitions;
			
			if(partitionList.size() != currPartitions){
				infoChg.put(topicName, true);
			}
			
			currTopicPartitions.put(topicName, partitionList.size());
		}
	}
	
	public void healthcheck(){
		String brokersIdsPath = getBrokersIds(); 
		List<String> brokersList = zkClient.getChildren(brokersIdsPath);
		if(brokersList.size() == 0){
			kafkaChg.onClusterShutDown();
		}
	}
	
	public boolean checkPath(ZkClient zkClient, String path){
		boolean isExists = zkClient.exists(path);
		if(!isExists){
			logger.error("invalid zk path:{}", path);
			return false;
		}
		
		return true;
	}
	
	/**
	 * 重置配置，默认都为false。
	 */
	private void resetChgInfo(){
		for(Entry<String, Boolean> tmp : infoChg.entrySet()){
			tmp.setValue(false);
		}
	}
	
	public String getidsPath(String consumerGroup){
		String formatPath = "/consumers/%s/ids";
		formatPath = String.format(formatPath, consumerGroup);
		return formatPath;
	}
	
	public String getPartitionsPath(String topic){
		String formatPath = "/brokers/topics/%s/partitions";
		formatPath = String.format(formatPath, topic);
		return formatPath;
	}
	
	public String getBrokersIds(){
		return "/brokers/ids";
	}
	
	public void setConsumer_moni_period(int consumer_moni_period) {
		this.consumer_moni_period = consumer_moni_period;
	}

	public void setPartitions_moni_period(int partitions_moni_period) {
		this.partitions_moni_period = partitions_moni_period;
	}
}
