package com.dtstack.logstash.assembly.qlist;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

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

	protected AtomicBoolean ato = new AtomicBoolean(false);

	protected ReentrantLock lock = new ReentrantLock();

	public void put(Map<String, Object> message) {
	}

	public Map<String, Object> get() {
		return null;
	}

	public void startElectionIdleQueue() {
	}

	public void startLogQueueSize() {
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
	
    public List<LinkedBlockingQueue<Map<String, Object>>> getQueueList() {
	    return queueList;
    }
}
