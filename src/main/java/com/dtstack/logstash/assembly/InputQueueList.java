package com.dtstack.logstash.assembly;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:25:23
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class InputQueueList {

	private static Logger logger = LoggerFactory.getLogger(InputQueueList.class);
    
	private static ExecutorService executor = Executors.newFixedThreadPool(2);

	private static List<LinkedBlockingQueue<Map<String, Object>>> queueList = Lists.newArrayList();

	private final AtomicInteger index = new AtomicInteger(0);

	private static int SLEEP = 2;//queue选取的间隔时间

	private AtomicBoolean ato = new AtomicBoolean(false);

	private ReentrantLock lock = new ReentrantLock();
	
	public InputQueueList() {}

	/**
	 * 
	 * @param message
	 */
	public void put(Map<String, Object> message) {
		if (queueList.size() == 0) {
			logger.error("InputQueueList is not Initialize");
			System.exit(1);
		}
		try {
			if (ato.get()) {
				try {
					lock.lock();
					queueList.get(index.get()).put(message);
				} finally {
					lock.unlock();
				}
			} else {
				queueList.get(index.get()).put(message);
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

	public List<LinkedBlockingQueue<Map<String, Object>>> getQueueList() {
		return queueList;
	}
	
	
	public void startElectionIdleQueue(){
		executor.submit(new ElectionIdleQueue());
	}
	
	
	public void startLogQueueSize(){
		executor.submit(new LogQueueSize());
	}
	
	
	class LogQueueSize implements Runnable{

		@Override
		public void run() {
			// TODO Auto-generated method stub
			try{
				Thread.sleep(1000);
				int size = queueList.size();
				for(int i = 0; i < size; i++){
					System.out.println(i+"--->"+queueList.get(i).size());
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
						index.getAndSet(id);
					}
					Thread.sleep(SLEEP);
				} catch (Exception e) {
					logger.error("electionIdleQueue is error:", e);
				}
			}
		}
	}

	public boolean allQueueEmpty() {
		boolean result = true;
		for (LinkedBlockingQueue<Map<String, Object>> queue : queueList) {
			result = result && queue.isEmpty();
		}
		return result;
	}
	
	public int allQueueSize(){
		int size=0;
		for (LinkedBlockingQueue<Map<String, Object>> queue : queueList) {
			size = size+queue.size();
		}
		return size;
	} 

	public AtomicBoolean getAto() {
		return ato;
	}

	public ReentrantLock getLock() {
		return lock;
	}
}
