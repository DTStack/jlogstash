package com.dtstack.logstash.assembly;

import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.logstash.outputs.BaseOutput;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年11月29日 下午15:30:18
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class OutputThread implements Runnable{
	
	private static Logger logger = LoggerFactory.getLogger(OutputThread.class);
	
	private static OutPutQueueList outPutQueueList;
	
	private List<BaseOutput> outputProcessors;

	public static void setOutPutQueueList(OutPutQueueList outPutQueueList) {
		OutputThread.outPutQueueList = outPutQueueList;
	}

    public OutputThread(List<BaseOutput> outputProcessors){
    	this.outputProcessors  = outputProcessors;
    }


	@Override
	public void run() {
		// TODO Auto-generated method stub
	    while (true) {
			Map<String, Object> event = null;
			try {
				if(!priorityFail()){
					event = outPutQueueList.get();
					if (event != null && event.size() > 0) {
						for (BaseOutput bo : outputProcessors) {
							bo.process(event);
						}
					}	
				}		
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("{}:output event failed:{}",event, e.getCause());
			}
		}	
	}
	
	private boolean priorityFail(){
		//优先处理失败信息
		boolean dealFailMsg = false;
		for (BaseOutput bo : outputProcessors) {
			if (bo.isConsistency()) {
				dealFailMsg = dealFailMsg || bo.dealFailedMsg();
			}
		}
		return dealFailMsg;
	}
}
