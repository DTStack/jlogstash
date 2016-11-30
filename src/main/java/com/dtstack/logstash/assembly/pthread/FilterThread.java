package com.dtstack.logstash.assembly.pthread;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.logstash.assembly.qlist.InputQueueList;
import com.dtstack.logstash.assembly.qlist.OutPutQueueList;
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

	private static InputQueueList inPutQueueList;

	private static OutPutQueueList outPutQueueList;

	private List<BaseFilter> filterProcessors;
	
	private static ExecutorService filterExecutor;
	
	public FilterThread(List<BaseFilter> filterProcessors){
		this.filterProcessors = filterProcessors;
	}
	
	/**
	 * 
	 * @param filters
	 * @param works
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	public static void initFilterThread(List<Map> filters,int works,InputQueueList inPutQueueList,OutPutQueueList outPutQueueList) throws Exception{
		if(filterExecutor==null)filterExecutor= Executors.newFixedThreadPool(works);
		FilterThread.inPutQueueList=inPutQueueList;
		FilterThread.outPutQueueList = outPutQueueList;
		for(int i=0;i<works;i++){
			List<BaseFilter> baseFilters = FilterFactory.getBatchInstance(filters);	
			filterExecutor.submit(new FilterThread(baseFilters));
		}
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		A: while (true) {
			Map<String, Object> event = null;
			try {
				event = inPutQueueList.get();
				if (filterProcessors != null) {
					for (BaseFilter bf : filterProcessors) {
						if (event == null || event.size() == 0)
							continue A;
						bf.process(event);
					}
				}
				outPutQueueList.put(event);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("{}:filter event failed:{}", event, e.getCause());
			}
		}
	}
}
