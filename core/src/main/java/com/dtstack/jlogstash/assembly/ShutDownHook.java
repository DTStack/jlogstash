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
package com.dtstack.jlogstash.assembly;

import java.util.List;

import com.dtstack.jlogstash.metrics.MetricRegistryImpl;
import com.dtstack.jlogstash.metrics.groups.JobMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.jlogstash.assembly.qlist.QueueList;
import com.dtstack.jlogstash.inputs.BaseInput;
import com.dtstack.jlogstash.outputs.BaseOutput;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:25:34
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class ShutDownHook {
	
	private Logger logger = LoggerFactory.getLogger(ShutDownHook.class);
	
    private QueueList initFilterQueueList;
    
    private QueueList initOutputQueueList;

    private List<BaseInput> baseInputs; 
    
    private List<BaseOutput> baseOutputs;

    private MetricRegistryImpl metricRegistry;

    private JobMetricGroup jobMetricGroup;

    public ShutDownHook(QueueList initFilterQueueList,
						QueueList initOutputQueueList,
						List<BaseInput> baseInputs,
						List<BaseOutput> baseOutputs,
						MetricRegistryImpl metricRegistry,
						JobMetricGroup jobMetricGroup){
    	this.initFilterQueueList = initFilterQueueList;
    	this.initOutputQueueList = initOutputQueueList;
    	this.baseInputs  = baseInputs;
    	this.baseOutputs = baseOutputs;
    	this.metricRegistry = metricRegistry;
    	this.jobMetricGroup = jobMetricGroup;
    }
	
	public void addShutDownHook(){
	   Thread shut =new Thread(new ShutDownHookThread());
	   shut.setDaemon(true);
	   Runtime.getRuntime().addShutdownHook(shut);
	   logger.debug("addShutDownHook success ...");
	}
	
	class ShutDownHookThread implements Runnable{
		private void inputRelease(){
			try{
				if(baseInputs!=null){
					for(BaseInput input:baseInputs){
						input.release();
					}
				}
				logger.warn("inputRelease success...");
			}catch(Exception e){
				logger.error("inputRelease error:{}",e.getMessage());
			}
		}
		
		private void outPutRelease(){
			try{
				if(baseOutputs!=null){
					for(BaseOutput outPut:baseOutputs){
						outPut.release();
					}
				}
				logger.warn("outPutRelease success...");
			}catch(Exception e){
				logger.error("outPutRelease error:{}",e.getMessage());
			}
		}

		private void metricsRelease(){
			if (jobMetricGroup != null) {
				jobMetricGroup.close();
			}
			// metrics shutdown
			if (metricRegistry != null) {
				metricRegistry.shutdown();
				metricRegistry = null;
			}
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			inputRelease();
			if(initFilterQueueList!=null){initFilterQueueList.queueRelease();}
			if(initOutputQueueList!=null){initOutputQueueList.queueRelease();}
			outPutRelease();
			metricsRelease();
		}
	}
}
