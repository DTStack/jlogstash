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
package com.dtstack.logstash.assembly.qlist;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:25:23
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class InputQueueList extends QueueList{

	private static Logger logger = LoggerFactory.getLogger(InputQueueList.class);
    
	private static ExecutorService executor = Executors.newFixedThreadPool(2);

	private final AtomicInteger pIndex = new AtomicInteger(0);
	
	private static int SLEEP = 1;//queue选取的间隔时间
	
	private static InputQueueList inputQueueList;
	
    private static int releaseSleep =1000;

	protected AtomicBoolean ato = new AtomicBoolean(false);

	protected ReentrantLock lock = new ReentrantLock();
	
	public static InputQueueList getInputQueueListInstance(int queueNumber,int queueSize){
		if(inputQueueList!=null)return inputQueueList;
		inputQueueList = new InputQueueList();
        for(int i=0;i<queueNumber;i++){
        	inputQueueList.queueList.add(new ArrayBlockingQueue<Map<String,Object>>(queueSize));
        }
		return inputQueueList;
	}
	

	/**
	 * 
	 * @param message
	 */
	@Override
	public void put(Map<String, Object> message) {
		try {
			if (ato.get()) {
				try {
					lock.lockInterruptibly();;
					queueList.get(pIndex.get()).put(message);
				} finally {
					lock.unlock();
				}
			} else {
				queueList.get(pIndex.get()).put(message);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			logger.error("put message error:", e);
		}finally{
			if(ato.get()){
				lock.unlock();
			}
		}
	}
		
	@Override
	public void startElectionIdleQueue(){
		executor.submit(new ElectionIdleQueue());
	}
	
	@Override
	public void startLogQueueSize(){
		executor.submit(new LogQueueSize());
	}
	
	
	@Override
	public void queueRelease(){
			try{
                lock.lockInterruptibly();
				ato.getAndSet(true);
				Thread.sleep(releaseSleep);
				boolean empty =allQueueEmpty();
				while(!empty){
					empty =allQueueEmpty();
				}
				logger.warn("queue size=="+allQueueSize());
			    logger.warn("inputQueueRelease success ...");
			}catch(Exception e){
			    logger.error("inputQueueRelease error:{}",e.getMessage());
			}
			finally{
				try{
					lock.unlock();
				}catch(Exception e){
				    logger.error("inputQueueRelease error:{}",e.getMessage());
				}
			}
	}
	
	
	class LogQueueSize implements Runnable{

		@Override
		public void run() {
			// TODO Auto-generated method stub
			try{
				while(true){
					Thread.sleep(1000);
					int size = queueList.size();
					for(int i = 0; i < size; i++){
						logger.debug("inputqueue:"+i+"--->"+queueList.get(i).size());
					}
				}
			}catch(Exception e){
				logger.error(e.getMessage());
			}
		}	
	}
	
	class ElectionIdleQueue implements Runnable {

		@Override
		public void run() {
			// TODO Auto-generated method stub
			int size = queueList.size();
			while (true) {
				try {
					if (size > 0) {
						int id = 0;
						int sz = Integer.MAX_VALUE;
						for (int i = 0; i < size; i++) {
							int ssz = queueList.get(i).size();
							if (ssz <= sz) {
								sz = ssz;
								id = i;
							}
						}
						pIndex.getAndSet(id);
					}
					Thread.sleep(SLEEP);
				} catch (Exception e) {
					logger.error("input electionIdleQueue is error:{}",e.getCause());
				}
			}
		}
	}
}
