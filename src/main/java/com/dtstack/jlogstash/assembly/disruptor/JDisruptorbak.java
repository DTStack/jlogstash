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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.lmax.disruptor.BlockingWaitStrategy;
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
public class JDisruptorbak {

	private static Logger logger = LoggerFactory.getLogger(JDisruptorbak.class);

	
	private WorkHandler<MapEvent>[] processors;

	private int ringBufferSize = 1 << 6;

	private WaitStrategy waitStrategy = new BlockingWaitStrategy();
	
	private List<Disruptor<MapEvent>> disruptors = Lists.newArrayList();
	
	private List<RingBuffer<MapEvent>> ringBuffers = Lists.newArrayList();

	private Map<Long,Integer> threadIds = Maps.newConcurrentMap();
	
	private Map<Integer,List<Long>> ringBufferIndexThreadId = Maps.newLinkedHashMap();
	
//	private final AtomicInteger pIndex = new AtomicInteger(0);

//	private ExecutorService executor = Executors.newFixedThreadPool(1);

	
	public JDisruptorbak(WorkHandler<MapEvent>[] processors, int ringBufferSize) {
		this(processors);
		this.ringBufferSize = ringBufferSize;
		init();
	}
	
	public JDisruptorbak(WorkHandler<MapEvent>[] processors,
			String waitType) {
		this(processors);
		this.waitStrategy = WaitStrategyEnum.getWaitStrategy(waitType);
		init();
	}
	
	public JDisruptorbak(WorkHandler<MapEvent>[] processors, int ringBufferSize,String waitType) {
		this(processors);
		this.ringBufferSize = ringBufferSize;
		this.waitStrategy =  WaitStrategyEnum.getWaitStrategy(waitType);
		init();
	}

	private JDisruptorbak(WorkHandler<MapEvent>[] processors) {
		this.processors = processors;
	}

	@SuppressWarnings("unchecked")
	public void init() {
		int length = processors.length;
		for(int i=0;i<length;i++){
			Disruptor<MapEvent> disruptor = new Disruptor<MapEvent>(new MapEventFactory(), ringBufferSize,
					new MapEventThreadFactory(), ProducerType.MULTI, waitStrategy); 
			disruptor.handleEventsWithWorkerPool(processors[i]);
			disruptor.setDefaultExceptionHandler(new MapEventExceptionHandler());
			disruptors.add(disruptor);
			ringBuffers.add(disruptor.getRingBuffer());
			ringBufferIndexThreadId.put(i, Lists.newArrayList());
		}
	}
	
	public void put(Map<String, Object> event) {
		if (event != null && event.size() > 0){
			Long threadId = Thread.currentThread().getId();
			Integer index = threadIds.get(threadId);
			if(index==null){
				synchronized(this){
					index = threadIds.get(threadId);
					if(index==null){
						index = electionRingBuffer();
						threadIds.put(threadId, index);
						ringBufferIndexThreadId.get(index).add(threadId);
					}
					System.out.println(ringBufferIndexThreadId.toString());
				}
			}
			RingBuffer<MapEvent> ringBuffer = ringBuffers.get(index);
			long sequence = ringBuffer.next();
			try {
				MapEvent mapEvent = ringBuffer.get(sequence);
			    mapEvent.setEvent(event);
			} finally {
				ringBuffer.publish(sequence);
			}
		}
	}
	
	private int electionRingBuffer(){
		int id = 0;
		int sz = Integer.MAX_VALUE;
		Set<Map.Entry<Integer, List<Long>>> entrys =ringBufferIndexThreadId.entrySet();
		for (Map.Entry<Integer, List<Long>> entry:entrys) {
			int ssz = entry.getValue().size();
			if (ssz < sz) {
				sz = ssz;
				id = entry.getKey();
			}
		}
		return id;
	}

	public void release() {
		// TODO Auto-generated method stub
		if(disruptors!=null){
			for(Disruptor<MapEvent> disruptor:disruptors){
				if(disruptor!=null)disruptor.shutdown();
			}
		}
	
	}
	
	public void start(){
		if(disruptors!=null){
			for(Disruptor<MapEvent> disruptor:disruptors){
				if(disruptor!=null)disruptor.start();
			}
		}
	}
}
