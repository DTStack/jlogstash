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
package com.dtstack.logstash.assembly.pthread;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.logstash.assembly.disruptor.MapEvent;
import com.dtstack.logstash.factory.OutputFactory;
import com.dtstack.logstash.outputs.BaseOutput;
import com.lmax.disruptor.WorkHandler;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年11月29日 下午15:30:18
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class OutputHandler implements WorkHandler<MapEvent>{
	
	private static Logger logger = LoggerFactory.getLogger(OutputHandler.class);
	
	private List<BaseOutput> outputProcessors;
	
	public OutputHandler(List<BaseOutput> outputProcessors){
    	this.outputProcessors  = outputProcessors;
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

	@Override
	public void onEvent(MapEvent mapEvent) throws Exception {
		// TODO Auto-generated method stub
		if(!priorityFail()){
			Map<String,Object> event = mapEvent.getEvent();
			if (event != null&&event.size()>0) {
				for (BaseOutput bo : outputProcessors) {
					bo.process(event);
				}
			}	
		}
	}
	
	public static WorkHandler<MapEvent> getHandlerInstance(List<BaseOutput> outputProcessors){
		return new OutputHandler(outputProcessors);
	}


	@SuppressWarnings("rawtypes")
	public static WorkHandler<MapEvent>[] getArrayHandlerInstance(
			List<Map> outputs, int outputWorks,List<List<BaseOutput>> allOutputs) throws Exception {
		// TODO Auto-generated method stub
		WorkHandler<MapEvent>[] handlers = new FilterHandler[outputWorks];
		for(int i=0;i<outputWorks;i++){
			List<BaseOutput> os =OutputFactory.getBatchInstance(outputs);
			handlers[i] = getHandlerInstance(os);
			allOutputs.add(os);
		}
		return handlers;
	}
}
