package com.dtstack.logstash.assembly.pthread;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.logstash.assembly.qlist.OutPutQueueList;
import com.dtstack.logstash.exception.ExceptionUtil;
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
	
	
	private List<BaseOutput> outputProcessors;
	
	private static ExecutorService outputExecutor;
	
	private BlockingQueue<Map<String, Object>> outputQueue;


    public OutputThread(List<BaseOutput> outputProcessors,BlockingQueue<Map<String, Object>> outputQueue){
    	this.outputProcessors  = outputProcessors;
    	this.outputQueue = outputQueue;
    }
    
	/**
	 * 
	 * @param outputs
	 * @param works
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	public static  void initOutPutThread(List<Map> outputs,OutPutQueueList outPutQueueList,List<BaseOutput> allBaseOutputs) throws Exception{
		if(outputExecutor==null)outputExecutor= Executors.newFixedThreadPool(outPutQueueList.getQueueList().size());
		for(int i=0;i<outPutQueueList.getQueueList().size();i++){
			List<BaseOutput> baseOutputs = OutputFactory.getBatchInstance(outputs);
			allBaseOutputs.addAll(baseOutputs);
			outputExecutor.submit(new OutputThread(baseOutputs,outPutQueueList.getQueueList().get(i)));
		}
	}


	@Override
	public void run() {
		// TODO Auto-generated method stub
	 Map<String, Object> event = null;
	 try {
		Thread.sleep(2000); 
	    while (true) {
				if(!priorityFail()){
					event = this.outputQueue.take();
					if (event != null) {
						for (BaseOutput bo : outputProcessors) {
							bo.process(event);
						}
					}	
				}		
			} 
	    }catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("{}:output event failed:{}",event, ExceptionUtil.getErrorMessage(e));
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
