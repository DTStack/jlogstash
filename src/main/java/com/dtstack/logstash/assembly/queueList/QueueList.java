package com.dtstack.logstash.assembly.queueList;

import java.util.Map;


public interface QueueList {

	public void put(Map<String, Object> message);
	
	public Map<String,Object> get();
	
	public boolean allQueueEmpty();
	
	public int allQueueSize();
	
	public void startElectionIdleQueue();
	
	public void startLogQueueSize();
}
