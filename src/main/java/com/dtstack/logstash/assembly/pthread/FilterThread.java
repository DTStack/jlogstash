package com.dtstack.logstash.assembly.pthread;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.logstash.assembly.qlist.InputQueueList;
import com.dtstack.logstash.assembly.qlist.OutPutQueueList;
import com.dtstack.logstash.exception.ExceptionUtil;
import com.dtstack.logstash.factory.FilterFactory;
import com.dtstack.logstash.filters.BaseFilter;

/**
 * 
 * Reason: TODO ADD REASON(可选) Date: 2016年11月29日 下午15:30:18 Company:
 * www.dtstack.com
 * 
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
	
	/**
	 * 
	 * @param filters
	 * @param works
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	public static void initFilterThread(List<Map> filters,InputQueueList inPutQueueList,OutPutQueueList outPutQueueList) throws Exception{
		if(filterExecutor==null)filterExecutor= Executors.newFixedThreadPool(inPutQueueList.getQueueList().size());
		FilterThread.outPutQueueList = outPutQueueList;
		for(int i=0;i<inPutQueueList.getQueueList().size();i++){
			List<BaseFilter> baseFilters = FilterFactory.getBatchInstance(filters);	
			filterExecutor.submit(new FilterThread(baseFilters,inPutQueueList.getQueueList().get(i)));
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
						if (event == null || event.size() == 0)
							continue A;
						bf.process(event);
					}
				}
				if(event!=null)outPutQueueList.put(event);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("{}:filter event failed:{}", event, ExceptionUtil.getErrorMessage(e));
			}
		}
	}
}
