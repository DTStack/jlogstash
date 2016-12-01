package com.dtstack.logstash.assembly.qlist;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import com.google.common.collect.Lists;



/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年11月30日 下午1:25:11
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public abstract class QueueList {
	
	protected List<LinkedBlockingQueue<Map<String, Object>>> queueList = Lists.newArrayList();

	public abstract void put(Map<String, Object> message);

	public abstract void startElectionIdleQueue();
	
	public abstract void startLogQueueSize();
	
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
	
	public abstract void queueRelease();

	public List<LinkedBlockingQueue<Map<String, Object>>> getQueueList() {
		return queueList;
	}
}
