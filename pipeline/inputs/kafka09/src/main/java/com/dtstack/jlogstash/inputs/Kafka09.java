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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.decoder.IDecode;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:16:06
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class Kafka09 extends BaseInput implements IKafkaChg{
	private static final Logger logger = LoggerFactory.getLogger(Kafka09.class);

	private Map<String, ConsumerConnector> consumerConnMap = new HashMap<>();
	
	private Map<String, ExecutorService> executorMap = new HashMap<>();
	
	private static String encoding="UTF8";
	
	@Required(required=true)
	private static Map<String, Integer> topic;
	
	@Required(required=true)
	private static Map<String, String> consumerSettings;
	
	private ScheduledExecutorService scheduleExecutor;
	
	private static boolean openBalance = false;
	
	private static int consumerMoniPeriod =  3600 * 1000;
	
	private static int partitionsMoniPeriod = 10 * 1000;
	
	private ReentrantLock lock = new ReentrantLock();

	private class Consumer implements Runnable {
		private KafkaStream<byte[], byte[]> m_stream;
		private Kafka09 kafkaInput;
		private IDecode decoder;

		public Consumer(KafkaStream<byte[], byte[]> a_stream, Kafka09 kafkaInput) {
			this.m_stream = a_stream;
			this.kafkaInput = kafkaInput;
			this.decoder = kafkaInput.getDecoder();
		}

		@Override
		public void run() {
			try {
				while(true){
					ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
					while (it.hasNext()) {
						String m = null;
						try {
							m = new String(it.next().message(),
									Kafka09.encoding);
							Map<String, Object> event = this.decoder
									.decode(m);
							if (event!=null&&event.size()>0){
								this.kafkaInput.process(event);
							} 
						} catch (Exception e) {
							logger.error("process event:{} failed:{}",m,e.getCause());
						}
					}
				}
			} catch (Exception t) {
				logger.error("kakfa Consumer fetch is error:{}",t.getCause());
			}
		}
	}

	public Kafka09(Map<String, Object> config){
		super(config);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void prepare() {
		Properties props = geneConsumerProp();
		
		for(String topicName : topic.keySet()){
			ConsumerConnector consumer = kafka.consumer.Consumer
					.createJavaConsumerConnector(new ConsumerConfig(props));
			
			consumerConnMap.put(topicName, consumer);
		}
	}
	
	private Properties geneConsumerProp(){
		Properties props = new Properties();

		Iterator<Entry<String, String>> consumerSetting = consumerSettings
				.entrySet().iterator();

		while (consumerSetting.hasNext()) {
			Map.Entry<String, String> entry = consumerSetting.next();
			String k = entry.getKey();
			String v = entry.getValue();
			props.put(k, v);
		}
		
		return props;
	}

	@Override
	public void emit() {
		
		Iterator<Entry<String, Integer>> topicIT = topic.entrySet().iterator();
		while (topicIT.hasNext()) {
			
			Map.Entry<String, Integer> entry = topicIT.next();
			String topic = entry.getKey();
			Integer threads = entry.getValue();
			addNewConsumer(topic, threads);
		}
		
		//-----监控-----
		startMonitor();
	}
	
	public void addNewConsumer(String topic, Integer threads){
		ConsumerConnector consumer = consumerConnMap.get(topic);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = null;
		
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, threads);
		consumerMap = consumer.createMessageStreams(topicCountMap);
		
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
		ExecutorService executor = Executors.newFixedThreadPool(threads);

		for (final KafkaStream<byte[], byte[]> stream : streams) {
			executor.submit(new Consumer(stream, this));
		}
		
		executorMap.put(topic, executor);
	}

	@Override
	public void release() {
		
		for(ConsumerConnector consumer : consumerConnMap.values()){
			consumer.commitOffsets(true);
			consumer.shutdown();
		}
		
		for(ExecutorService executor : executorMap.values()){
			executor.shutdownNow();
		}
		
		scheduleExecutor.shutdownNow();
	}
	
	public void startMonitor(){
		
		logger.info("-----monitor kafka info(consumers,partitions,brokers)--------");
		String zookeeperConn = consumerSettings.get("zookeeper.connect");
		String groupId = consumerSettings.get("group.id");
		scheduleExecutor = Executors.newScheduledThreadPool(1);
		
		MonitorCluster monitor = new MonitorCluster(zookeeperConn, topic.keySet(), groupId,
				this, consumerMoniPeriod, partitionsMoniPeriod, openBalance);
		scheduleExecutor.scheduleAtFixedRate(monitor, 10, 10, TimeUnit.SECONDS);
	}

	@Override
	public void onInfoChgTrigger(String topicName, int consumers, int partitions) {
		
		lock.lock();
		logger.info("topic:{} consumer or partitions change, curr consumer num:{} partitions num:{}",
				new String[]{topicName, consumers+"", partitions+""});
		
		try{
			int expectNum = partitions/consumers;
			if(partitions%consumers > 0){
				expectNum++;
			}
			
			Integer threadNum = topic.get(topicName);
			if(threadNum == null){
				logger.error("invaid topic:{}.", topicName);
				return;
			}
			
			if(threadNum != expectNum){
				logger.warn("need chg thread num, curr threadNum:{}, expect threadNum:{}.", threadNum, expectNum);
				topic.put(topicName, expectNum);
				//停止,重启客户端
				reconnConsumer(topicName);
			}
		}catch(Exception e){
			logger.error("", e);
		}finally{
			lock.unlock();
		}
	}

	@Override
	public void onClusterShutDown(){
		logger.error("---- kafka cluster shutdown!!!");
	}
	
	public void reconnConsumer(String topicName){
		
		//停止topic 对应的conn
		ConsumerConnector consumerConn = consumerConnMap.get(topicName);
		consumerConn.commitOffsets(true);
		consumerConn.shutdown();
		consumerConnMap.remove(topicName);
		
		//停止topic 对应的stream消耗线程
		ExecutorService es = executorMap.get(topicName);
		es.shutdownNow();	
		executorMap.remove(topicName);
		
		Properties prop = geneConsumerProp();
		ConsumerConnector newConsumerConn = kafka.consumer.Consumer
				.createJavaConsumerConnector(new ConsumerConfig(prop));
		consumerConnMap.put(topicName, newConsumerConn);
		
		addNewConsumer(topicName, topic.get(topicName));
	}
}
