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
package com.dtstack.jlogstash.outputs;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.outputs.BaseOutput;
import com.dtstack.jlogstash.render.Formatter;
import com.google.common.collect.Maps;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:35:11
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class Kafka extends BaseOutput {
	
	private static final Logger logger = LoggerFactory.getLogger(Kafka.class);

	private static ObjectMapper objectMapper = new ObjectMapper();
	
	private Properties props;
	
	private ProducerConfig pconfig;
	
	private Producer<String, byte[]> producer;
	
	private static String encoding = "utf-8";
	
	private static String timezone;
	
	@Required(required=true)
	private static String topic;
	
	private static Map<String,Map<String,String>> topicSelect;
	
	private Set<Map.Entry<String,Map<String,String>>> entryTopicSelect;

	@Required(required=true)
	private static String brokerList;
	
	private static Map<String,String> producerSettings;

	static{
		Thread.currentThread().setContextClassLoader(null);
	}


	@SuppressWarnings("rawtypes")
	public Kafka(Map config) {
		super(config);
	}
	
	/**
	 * default
	 * props.put("key.serializer.class", "kafka.serializer.StringEncoder");
	 * props.put("value.serializer.class", "kafka.serializer.StringEncoder");
	 * props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
	 * props.put("producer.type", "sync");
	 * props.put("compression.codec", "none");
	 * props.put("request.required.acks", "1");
	 * props.put("batch.num.messages", "1024");
	 * props.put("client.id", "");			
	 */
	@Override
	public void prepare() {
		try{
			if(topicSelect != null){
				entryTopicSelect = topicSelect.entrySet();
			}
			
			if(props == null){
				props = new Properties();
                addDefaultKafkaSetting();
			}

			if(producerSettings != null){
				props.putAll(producerSettings);
			}

			if (!brokerList.trim().equals("")){
				props.put("metadata.broker.list",brokerList);
			} else {
				throw new Exception("brokerList can not be empty!");
			}

			if(pconfig == null){
				pconfig = new ProducerConfig(props);
			}
			if(producer == null){
				producer= new Producer<String, byte[]>(pconfig);
			}
		}catch(Exception e){
			logger.error("", e);
			System.exit(1);
		}
	}

	private void addDefaultKafkaSetting(){
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("value.serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
        props.put("producer.type", "sync");
        props.put("compression.codec", "none");
        props.put("request.required.acks", "1");
        props.put("batch.num.messages", "1024");
        props.put("client.id", "");
    }

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void emit(Map event) {
		try {
			String tp = null;
			if(entryTopicSelect != null){
				for(Map.Entry<String,Map<String,String>> entry : entryTopicSelect){
					String key = entry.getKey();
					Map<String,String> value = entry.getValue();
					Set<Map.Entry<String,String>> sets = value.entrySet();
					for(Map.Entry<String,String> ey:sets){
						if(ey.getKey().equals(event.get(key))){
							tp = Formatter.format(event, ey.getValue(), timezone);
							break;
						}
					}
				}
			}
			if(tp==null){
				tp = Formatter.format(event, topic, timezone);
			}
			producer.send(new KeyedMessage<>(tp, event.toString(), objectMapper.writeValueAsString(event).getBytes(encoding)));
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	 public static void main(String[] args){
		 Map<String,String> ss = Maps.newConcurrentMap();
		 ss.put("ysq1", "tysq1");
		 ss.put("ysq2", "tysq2");
		 Map<String,Map<String,String>> topic = Maps.newConcurrentMap();
		 topic.put("path", ss);
		 
		 Kafka.topic="oggoggogg";
		 Kafka.brokerList = "172.16.8.107:9092";
		 Kafka.topicSelect=topic;
		 
		 Kafka.producerSettings= Maps.newConcurrentMap();
			 
		 Kafka.producerSettings.put("producer.type", "async");
		 Kafka.producerSettings.put("key.serializer.class", "kafka.serializer.StringEncoder");
		 Kafka.producerSettings.put("value.serializer.class", "kafka.serializer.StringEncoder");
		 
		 Kafka kafka = new Kafka(Maps.newConcurrentMap());
		 kafka.prepare();
		 
		 for(int i=0;i<10;i++){
			 Map dd = Maps.newConcurrentMap();
			 dd.put("path", "ysq"+i);
			 kafka.emit(dd);
		 }
		 
	 }
}
