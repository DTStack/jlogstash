package com.dtstack.logstash.assembly.pluginThread;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.logstash.assembly.queueList.InputQueueList;
import com.dtstack.logstash.assembly.queueList.OutPutQueueList;
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
	
	public FilterThread(List<BaseFilter> filterProcessors){
		this.filterProcessors = filterProcessors;
	}

	public static void setInPutQueueList(InputQueueList inPutQueueList) {
		FilterThread.inPutQueueList = inPutQueueList;
	}

	public static void setOutPutQueueList(OutPutQueueList outPutQueueList) {
		FilterThread.outPutQueueList = outPutQueueList;
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
