package com.dtstack.logstash.assembly.pthread;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.logstash.assembly.qlist.OutPutQueueList;
import com.dtstack.logstash.factory.OutputFactory;
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
	
	private static ExecutorService outputExecutor;

    public OutputThread(List<BaseOutput> outputProcessors){
    	this.outputProcessors  = outputProcessors;
    }
    
	/**
	 * 
	 * @param outputs
	 * @param works
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	public static  void initOutPutThread(List<Map> outputs,int works,OutPutQueueList outPutQueueList,List<BaseOutput> allBaseOutputs) throws Exception{
		OutputThread.outPutQueueList = outPutQueueList;
		if(outputExecutor==null)outputExecutor= Executors.newFixedThreadPool(works);
		for(int i=0;i<works;i++){
			List<BaseOutput> baseOutputs = OutputFactory.getBatchInstance(outputs);
			allBaseOutputs.addAll(baseOutputs);
			outputExecutor.submit(new OutputThread(baseOutputs));
		}
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
