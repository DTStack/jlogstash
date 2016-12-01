package com.dtstack.logstash.assembly.qlist;

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
	
	private static int SLEEP = 1;//queue选取的间隔时间
	
	private static OutPutQueueList outPutQueueList;
		
	public static OutPutQueueList getOutPutQueueListInstance(int queueNumber,int queueSize){
		if(outPutQueueList!=null)return outPutQueueList;
		outPutQueueList = new OutPutQueueList();
        for(int i=0;i<queueNumber;i++){
        	outPutQueueList.queueList.add(new LinkedBlockingQueue<Map<String,Object>>(queueSize));
        }
		return outPutQueueList;
	}

	
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
			queueList.get(pIndex.get()).put(message);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			logger.error("put output queue message error:{}",e.getCause());
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
				boolean empty =allQueueEmpty();
				while(!empty){
					empty =allQueueEmpty();
				}
				logger.warn("out queue size=="+allQueueSize());
			    logger.warn("outputQueueRelease success ...");
			}catch(Exception e){
			    logger.error("outputQueueRelease error:{}",e.getCause());
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
						logger.debug("outputqueue:"+i+"--->"+queueList.get(i).size());
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
					logger.error("electionIdleQueue is error:{}",e.getCause());
				}
			}
		}
	}
}
