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
	private List<Map<String, Object>> batchEvent = Lists
			.newCopyOnWriteArrayList();

	private Map<Long, Integer> repeatOffest = Maps.newLinkedHashMap();

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
				event = inputQueue.take();
				if (this.filterProcessors != null) {
					for (BaseFilter bf : filterProcessors) {
						if (event == null || event.size() == 0)continue A;
						bf.process(event);
					}
				}
				if (event != null && event.size() > 0) {
					for (BaseOutput bo : outputProcessors) {
						if (!bo.isConsistency()) {
							bo.process(event);
						} else {
							repeatEvent(bo, event, true, 0);
							repeatOffest.clear();
						}
					}
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("process event failed:" + event, e.getCause());
			}
		}
	}

	public void repeatEvent(BaseOutput bo, Map<String, Object> event,
			boolean source, int index) {
		if (bo.getAto().get() == 0) {
			bo.process(event);
			if (source) {
				batchEvent.add(event);
			}
		} else if (bo.getAto().get() == 1) {
			bo.getAto().getAndSet(0);
			bo.process(event);
			if (source) {
				batchEvent.clear();
				batchEvent.add(event);
			} else {
				long size = repeatOffest.size();
				repeatOffest.put(size, index);
			}
		} else if (bo.getAto().get() == 2) {
			bo.getAto().getAndSet(0);
			long nested = repeatOffest.size() + 1;
			int lastTimeIndex = nested == 1 ? 0 : repeatOffest.get(nested - 1);
			repeatOffest.put(nested, lastTimeIndex);
			int eventSize = batchEvent.size();
			for (int i = lastTimeIndex; i < eventSize; i++) {
				if (nested < repeatOffest.size()) {
					break;
				}
				repeatEvent(bo, batchEvent.get(i), false, i);
			}
			logger.warn(bo.getClass().getName() + "--->repeat event size:"
					+ batchEvent.size());
		}
	}
}
