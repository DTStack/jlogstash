package com.dtstack.logstash.assembly.disruptor;

import java.util.concurrent.ThreadFactory;

public class MapEventThreadFactory implements ThreadFactory{

	@Override
	public Thread newThread(Runnable r) {
		// TODO Auto-generated method stub
		return new Thread(r,"disruptor");
	}

}
