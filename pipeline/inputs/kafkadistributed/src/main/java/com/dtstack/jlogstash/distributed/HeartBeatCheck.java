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
package com.dtstack.jlogstash.distributed;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.jlogstash.distributed.util.CountUtil;
import com.dtstack.jlogstash.exception.ExceptionUtil;
import com.google.common.collect.Maps;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年12月28日 下午1:16:37
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class HeartBeatCheck implements Runnable{
	
	private static final Logger logger = LoggerFactory.getLogger(HeartBeatCheck.class);
	
	private ZkDistributed zkDistributed;
	
	private final static int HEATBEATCHECK = 2000;
	
	private final static int EXCEEDCOUNT = 3;
	
	private MasterCheck masterCheck;

	public HeartBeatCheck(ZkDistributed zkDistributed,MasterCheck masterCheck){
		this.zkDistributed = zkDistributed;
		this.masterCheck = masterCheck;
	}
	
	public Map<String,BrokerNodeCount> brokerNodeCounts =  Maps.newConcurrentMap();
	
	
	
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			int index = 0;
			while(true){
				++index;
				if(this.masterCheck.isMaster()){
					healthCheck();
				}
				if(CountUtil.count(index, 5))logger.warn("HeartBeatCheck start again...");
				Thread.sleep(HEATBEATCHECK);
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error(ExceptionUtil.getErrorMessage(e));
		}
	}
	
	private void healthCheck(){
		List<String> childrens = zkDistributed.getBrokersChildren();
		if(childrens!=null){
			for(String node:childrens){
				BrokerNode brokerNode = zkDistributed.getBrokerNodeData(node);
				if(brokerNode!=null&&brokerNode.isAlive()){
					BrokerNodeCount brokerNodeCount = brokerNodeCounts.get(node);
					if(brokerNodeCount==null){
						brokerNodeCount = new BrokerNodeCount(0,brokerNode);
					}
					if(brokerNodeCount.getBrokerNode().getSeq().longValue()==brokerNode.getSeq().longValue()){
						brokerNodeCount.setCount(brokerNodeCount.getCount()+1);
					}else{
						brokerNodeCount.setCount(0);
					}
					if(brokerNodeCount.getCount() > EXCEEDCOUNT){//node died
					    this.zkDistributed.disableLocalNode(node);
						brokerNodeCounts.remove(node);
					}else{
						brokerNodeCount.setBrokerNode(brokerNode);
						brokerNodeCounts.put(node, brokerNodeCount);
					}
				}else{
					brokerNodeCounts.remove(node);
				}
			}
		}
	}
	
	static class BrokerNodeCount{
		
		private int count;
		
		private BrokerNode brokerNode;
		
		public BrokerNodeCount(int count,BrokerNode brokerNode){
			this.count = count;
			this.brokerNode  = brokerNode;
		}

		public int getCount() {
			return count;
		}
		public void setCount(int count) {
			this.count = count;
		}
		public BrokerNode getBrokerNode() {
			return brokerNode;
		}
		public void setBrokerNode(BrokerNode brokerNode) {
			this.brokerNode = brokerNode;
		}
	}
}
