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
package com.dtstack.jlogstash.assembly.pthread;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import com.dtstack.jlogstash.factory.LogstashThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.jlogstash.assembly.qlist.InputQueueList;
import com.dtstack.jlogstash.assembly.qlist.OutPutQueueList;
import com.dtstack.jlogstash.exception.ExceptionUtil;
import com.dtstack.jlogstash.factory.FilterFactory;
import com.dtstack.jlogstash.filters.BaseFilter;

/**
 * 
 * Reason: TODO ADD REASON(可选) 
 * Date: 2016年11月29日 下午15:30:18 
 * Company:www.dtstack.com
 * @author sishu.yss
 *
 */
public class FilterThread implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(FilterThread.class);
	
	private BlockingQueue<Map<String, Object>> inputQueue;

	private static OutPutQueueList outPutQueueList;

	private List<BaseFilter> filterProcessors;
	
	private static ExecutorService filterExecutor;
	
	public FilterThread(List<BaseFilter> filterProcessors,BlockingQueue<Map<String, Object>> inputQueue){
		this.filterProcessors = filterProcessors;
		this.inputQueue = inputQueue;
	}
	
	@SuppressWarnings("rawtypes")
	public static void initFilterThread(List<Map> filters,InputQueueList inPutQueueList,OutPutQueueList outPutQueueList) throws Exception{
		if(filterExecutor==null){
			int size = inPutQueueList.getQueueList().size();
			filterExecutor = new ThreadPoolExecutor(size,size,
					0L, TimeUnit.MILLISECONDS,
					new LinkedBlockingQueue<Runnable>(),new LogstashThreadFactory(FilterThread.class.getName()));
		}
		FilterThread.outPutQueueList = outPutQueueList;
		for(BlockingQueue<Map<String, Object>> queueList:inPutQueueList.getQueueList()){
			List<BaseFilter> baseFilters = FilterFactory.getBatchInstance(filters);	
			filterExecutor.submit(new FilterThread(baseFilters,queueList));
		}
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		A: while (true) {
			Map<String, Object> event = null;
			try {
				event = this.inputQueue.take();
				if (filterProcessors != null) {
					for (BaseFilter bf : filterProcessors) {
						if (event == null || event.size() == 0){
							continue A;
						}
						bf.process(event);
					}
				}
				if(event!=null){outPutQueueList.put(event);}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("{}:filter event failed:{}", event, ExceptionUtil.getErrorMessage(e));
			}
		}
	}
}
