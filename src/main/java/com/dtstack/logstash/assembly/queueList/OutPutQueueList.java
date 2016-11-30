package com.dtstack.logstash.assembly.queueList;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年11月29日 下午1:25:23
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class OutPutQueueList extends QueueList{

	private static Logger logger = LoggerFactory.getLogger(OutPutQueueList.class);
    
	private static ExecutorService executor = Executors.newFixedThreadPool(2);

	private final AtomicInteger pIndex = new AtomicInteger(0);
	
	private final AtomicInteger gIndex = new AtomicInteger(0);

	private static int SLEEP = 1;//queue选取的间隔时间

	
	/**
	 * 
	 * @param message
	 */
	@Override
	public void put(Map<String, Object> message) {
		if (queueList.size() == 0) {
			logger.error("queueList is not Initialize");
			System.exit(1);
		}
		try {
			if (ato.get()) {
				try {
					lock.lockInterruptibly();
					queueList.get(pIndex.get()).put(message);
				} finally {
					lock.unlock();
				}
			} else {
				queueList.get(pIndex.get()).put(message);
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
		if (queueList.size() == 0) {
			logger.error("queueList is not Initialize");
			System.exit(1);
		}
		try{
			return queueList.get(gIndex.get()).take();
		}catch(Exception e){
			logger.error("queueList get error:{}",e.getCause());
		}
	    return null;
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
						int gId =0;
						int sz = Integer.MAX_VALUE;
						int mz  = Integer.MIN_VALUE;
						for (int i = 0; i < size; i++) {
							int ssz = queueList.get(i).size();
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
}
