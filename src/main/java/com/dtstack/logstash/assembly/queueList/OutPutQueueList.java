package com.dtstack.logstash.assembly.queueList;

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
 * Date: 2016年11月29日 下午1:25:23
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class OutPutQueueList implements QueueList{

	private static Logger logger = LoggerFactory.getLogger(OutPutQueueList.class);
    
	private static ExecutorService executor = Executors.newFixedThreadPool(2);

	private static List<LinkedBlockingQueue<Map<String, Object>>> outPutQueueList = Lists.newArrayList();

	private final AtomicInteger pIndex = new AtomicInteger(0);
	
	private final AtomicInteger gIndex = new AtomicInteger(0);

	private static int SLEEP = 1;//queue选取的间隔时间

	private AtomicBoolean ato = new AtomicBoolean(false);

	private ReentrantLock lock = new ReentrantLock();
	
	/**
	 * 
	 * @param message
	 */
	@Override
	public void put(Map<String, Object> message) {
		if (outPutQueueList.size() == 0) {
			logger.error("OutputQueueList is not Initialize");
			System.exit(1);
		}
		try {
			if (ato.get()) {
				try {
					lock.lockInterruptibly();
					outPutQueueList.get(pIndex.get()).put(message);
				} finally {
					lock.unlock();
				}
			} else {
				outPutQueueList.get(pIndex.get()).put(message);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			logger.error("put output queue message error:{}",e.getCause());
		}finally{
			if(ato.get()){
				lock.unlock();
			}
		}
	}

	@Override
	public Map<String,Object> get(){
		if (outPutQueueList.size() == 0) {
			logger.error("OutputQueueList is not Initialize");
			System.exit(1);
		}
		try{
			return outPutQueueList.get(gIndex.get()).take();
		}catch(Exception e){
			logger.error("OutputQueueList get error:{}",e.getCause());
		}
	    return null;
	}
	
	
	
	public List<LinkedBlockingQueue<Map<String, Object>>> getQueueList() {
		return outPutQueueList;
	}
	
	@Override
	public void startElectionIdleQueue(){
		executor.submit(new ElectionIdleQueue());
	}
	
	@Override
	public void startLogQueueSize(){
		executor.submit(new LogQueueSize());
	}
	
	
	class LogQueueSize implements Runnable{

		@Override
		public void run() {
			// TODO Auto-generated method stub
			try{
				Thread.sleep(1000);
				int size = outPutQueueList.size();
				for(int i = 0; i < size; i++){
					System.out.println(i+"--->"+outPutQueueList.get(i).size());
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
			int size = outPutQueueList.size();
			while (true) {
				try {
					if (size > 0) {
						int id = 0;
						int gId =0;
						int sz = Integer.MAX_VALUE;
						int mz  = Integer.MIN_VALUE;
						for (int i = 0; i < size; i++) {
							int ssz = outPutQueueList.get(i).size();
							if (ssz <= sz) {
								sz = ssz;
								id = i;
							}
							if(ssz>=mz){
								mz = ssz;
								gId = i;
							}
						}
						pIndex.getAndSet(id);
						gIndex.getAndSet(gId);
					}
					Thread.sleep(SLEEP);
				} catch (Exception e) {
					logger.error("electionIdleQueue is error:{}",e.getCause());
				}
			}
		}
	}

	@Override
	public boolean allQueueEmpty() {
		boolean result = true;
		for (LinkedBlockingQueue<Map<String, Object>> queue : outPutQueueList) {
			result = result && queue.isEmpty();
		}
		return result;
	}
	
	@Override
	public int allQueueSize(){
		int size=0;
		for (LinkedBlockingQueue<Map<String, Object>> queue : outPutQueueList) {
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
