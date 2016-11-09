package com.dtstack.logstash.assembly;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.logstash.filters.BaseFilter;
import com.dtstack.logstash.outputs.BaseOutput;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:25:18
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class FilterAndOutputThread implements Runnable {

	private static Logger logger = LoggerFactory
			.getLogger(FilterAndOutputThread.class);
	private LinkedBlockingQueue<Map<String, Object>> inputQueue;
	private List<BaseFilter> filterProcessors;
	private List<BaseOutput> outputProcessors;
	private int batchSize;

	public FilterAndOutputThread(
			LinkedBlockingQueue<Map<String, Object>> inputQueue,
			List<BaseFilter> filters, List<BaseOutput> outputs, int batchSize) {
		this.inputQueue = inputQueue;
		this.filterProcessors = filters;
		this.outputProcessors = outputs;
		this.batchSize = batchSize;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		A: while (true) {
			Map<String, Object> event = null;
			try {
				
				//优先处理失败信息
				boolean dealFailMsg = false;
				for (BaseOutput bo : outputProcessors) {
					if (bo.isConsistency()) {
						dealFailMsg = dealFailMsg || bo.dealFailedMsg();
					}
				}
				
				if(dealFailMsg){
					continue A;
				}
				
				event = inputQueue.take();
				if (this.filterProcessors != null) {
					for (BaseFilter bf : filterProcessors) {
						if (event == null || event.size() == 0)continue A;
						bf.process(event);
					}
				}
				if (event != null && event.size() > 0) {
					for (BaseOutput bo : outputProcessors) {
						bo.process(event);
					}
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("process event failed:" + event, e.getCause());
			}
		}
	}

}
