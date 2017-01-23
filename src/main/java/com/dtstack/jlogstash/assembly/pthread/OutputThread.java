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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.jlogstash.assembly.qlist.OutPutQueueList;
import com.dtstack.jlogstash.exception.ExceptionUtil;
import com.dtstack.jlogstash.factory.OutputFactory;
import com.dtstack.jlogstash.outputs.BaseOutput;

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
