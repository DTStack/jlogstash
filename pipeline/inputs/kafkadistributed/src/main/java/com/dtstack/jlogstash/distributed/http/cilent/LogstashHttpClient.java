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
package com.dtstack.jlogstash.distributed.http.cilent;

import java.util.Set;

import com.dtstack.jlogstash.distributed.ZkDistributed;
import com.dtstack.jlogstash.distributed.http.common.HttpCommon;
import com.dtstack.jlogstash.distributed.http.common.Urls;


/**
 * 
 * @author sishu.yss
 *
 */
public class LogstashHttpClient {
	
	private ZkDistributed zkDistributed;
	
	private static String tempalteUrl = "http://%s:%d%s";
	
	private String localAddress;
	
	public LogstashHttpClient(ZkDistributed zkDistributed,String localAddress){
		this.zkDistributed = zkDistributed;
		this.localAddress = localAddress;
	}
	
    public void sendImmediatelyLoadNodeData(){
    	Set<String> nodes = this.zkDistributed.getNodeDatas().keySet();
    	for(String node:nodes){
    		if(!node.equals(this.localAddress)){
    			Object[] obj = HttpCommon.getUrlPort(node);
    			HttpClient.post(String.format(tempalteUrl, obj[0],obj[1],Urls.LOADNODEDATA),null);
    		}
    	}
    }
    
    public void sendImmediatelyLogPoolData(){
    	Set<String> nodes = this.zkDistributed.getNodeDatas().keySet();
    	for(String node:nodes){
    		if(!node.equals(this.localAddress)){
    			Object[] obj = HttpCommon.getUrlPort(node);
    			HttpClient.post(String.format(tempalteUrl, obj[0],obj[1],Urls.LOGPOOLDATA),null);
    		}
    	}
    }
}
