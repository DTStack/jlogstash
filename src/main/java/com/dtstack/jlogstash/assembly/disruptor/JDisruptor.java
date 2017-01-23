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
package com.dtstack.jlogstash.assembly.disruptor;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * 
 * Reason: TODO ADD REASON(可选) Date: 2017年01月20日 下午09:25:18
 * Company:www.dtstack.com
 * 
 * @author sishu.yss
 *
 */
public class JDisruptor {

	private static Logger logger = LoggerFactory.getLogger(JDisruptor.class);

	
	private WorkHandler<MapEvent>[] processors;

	private int ringBufferSize = 1 << 6;

	private WaitStrategy waitStrategy = null;
	
	private Disruptor<MapEvent> disruptor = null;
	
	private RingBuffer<MapEvent> ringBuffer = null;

	
	public JDisruptor(WorkHandler<MapEvent>[] processors, int ringBufferSize) {
		this(processors);
		this.ringBufferSize = ringBufferSize;
		init();
	}
	
	public JDisruptor(WorkHandler<MapEvent>[] processors,
			String waitType) {
		this(processors);
		this.waitStrategy = WaitStrategyEnum.getWaitStrategy(waitType);
		init();
	}
	
	public JDisruptor(WorkHandler<MapEvent>[] processors, int ringBufferSize,String waitType) {
		this(processors);
		this.ringBufferSize = ringBufferSize;
		this.waitStrategy =  WaitStrategyEnum.getWaitStrategy(waitType);
		init();
	}

	private JDisruptor(WorkHandler<MapEvent>[] processors) {
		this.processors = processors;
	}

	public void init() {
	    disruptor = new Disruptor<MapEvent>(new MapEventFactory(), ringBufferSize,
				new MapEventThreadFactory(), ProducerType.MULTI, waitStrategy); 
		disruptor.handleEventsWithWorkerPool(processors);
		disruptor.setDefaultExceptionHandler(new MapEventExceptionHandler());
		ringBuffer = disruptor.getRingBuffer();
	}
	
	public void put(Map<String, Object> event) {
		if (event != null && event.size() > 0){
			long sequence = ringBuffer.next();
			try {
				MapEvent mapEvent = ringBuffer.get(sequence);
			    mapEvent.setEvent(event);
			} finally {
				ringBuffer.publish(sequence);
			}
		}
	}
	

	public void release() {
		// TODO Auto-generated method stub
		if(disruptor!=null){
			if(disruptor!=null)disruptor.shutdown();
		}
	
	}
	
	public void start(){
		if(disruptor!=null){
			if(disruptor!=null)disruptor.start();
		}
	}
}
