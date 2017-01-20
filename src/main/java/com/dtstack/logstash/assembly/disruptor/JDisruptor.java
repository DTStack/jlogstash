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
package com.dtstack.logstash.assembly.disruptor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.Lists;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.WorkerPool;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * 
 * Reason: TODO ADD REASON(可选) Date: 2017年01月20日 下午09:25:18
 * Company:www.dtstack.com
 * 
 * @author sishu.yss
 *
 */
public class JDisruptor {

	private WorkHandler<MapEvent>[] processors;

	private int ringBufferSize = 1 << 6;

//	private RingBuffer<MapEvent> ringBuffer;

	private WaitStrategy waitStrategy = new BlockingWaitStrategy();

	private int works;
	
	private Disruptor<MapEvent> disruptor;
	
	private  ExecutorService executor = Executors.newCachedThreadPool();

	public JDisruptor(WorkHandler<MapEvent>[] processors, int ringBufferSize,
			int works) {
		this(processors, works);
		this.ringBufferSize = ringBufferSize;
	}
	
	public JDisruptor(WorkHandler<MapEvent>[] processors,
			String waitType,int works) {
		this(processors, works);
		this.waitStrategy = WaitStrategyEnum.getWaitStrategy(waitType);
	}
	
	public JDisruptor(WorkHandler<MapEvent>[] processors, int ringBufferSize,String waitType,
			int works) {
		this(processors, works);
		this.ringBufferSize = ringBufferSize;
		this.waitStrategy =  WaitStrategyEnum.getWaitStrategy(waitType);;
	}

	public JDisruptor(WorkHandler<MapEvent>[] processors, int works) {
		this.processors = processors;
		this.works = works;
	}

	@SuppressWarnings("unchecked")
	public void init() {
//		ringBuffer = RingBuffer.createMultiProducer(new MapEventFactory(),
//				ringBufferSize, waitStrategy);
//		SequenceBarrier sequenceBarrier = ringBuffer.newBarrier();
//		List<Sequence> gatingSequences = Lists.newArrayList();
//		List<WorkerPool<MapEvent>> workerPools = Lists.newArrayList();
//		for (int i = 0; i < works; i++) {
//			WorkerPool<MapEvent> workPool = new WorkerPool<MapEvent>(ringBuffer,
//					sequenceBarrier, new MapEventExceptionHandler(),
//					new WorkHandler[] { processor });
//			for (Sequence s : workPool.getWorkerSequences()) {
//				gatingSequences.add(s);
//			}
//			workerPools.add(workPool);
//		}
//		ringBuffer.addGatingSequences(gatingSequences.toArray(new Sequence[gatingSequences.size()]));
//		workerPoolStart(workerPools);
	}
	
//	private void workerPoolStart(List<WorkerPool<MapEvent>> workerPools){
//		for(WorkerPool<MapEvent> wp:workerPools){
//			wp.start(executor);
//		}
//	}
	
	public void put(Map<String, Object> event) {
		RingBuffer<MapEvent> ringBuffer = disruptor.getRingBuffer();
		long sequence = ringBuffer.next();
		try {
			MapEvent mapEvent = ringBuffer.get(sequence);
			if (event != null && event.size() > 0)
				mapEvent.setEvent(event);
		} finally {
			ringBuffer.publish(sequence);
		}
	}

	public void release() {
		// TODO Auto-generated method stub	
		if(disruptor!=null)disruptor.shutdown();
	}
	
	public void start(){
		if(disruptor!=null)disruptor.start();
	}
}
