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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年12月28日 下午1:16:37
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class BrokerNode {
	
	private Long seq;
	
	private  Boolean alive;
	
	private List<String> metas;
	
	public Boolean isAlive() {
		return alive;
	}

	public void setAlive(boolean alive) {
		this.alive = alive;
	}

	public Long getSeq() {
		return seq;
	}

	public void setSeq(long seq) {
		this.seq = seq;
	}

	public List<String> getMetas() {
		return metas;
	}

	public void setMetas(List<String> metas) {
		this.metas = metas;
	}
	
	public static BrokerNode initBrokerNode(){
		BrokerNode brokerNode = new BrokerNode();
		brokerNode.setMetas(new ArrayList<String>());
		brokerNode.setAlive(true);
		brokerNode.setSeq(0);
		return brokerNode;
	}
	
	public static BrokerNode initNullBrokerNode(){
		BrokerNode brokerNode = new BrokerNode();
		return brokerNode;
	}
	
	public static void copy(BrokerNode source,BrokerNode target){
    	if(source.getSeq()!=null){
    		target.setSeq(source.getSeq()+target.getSeq());
    	}
    	if(source.getMetas()!=null){
    		target.setMetas(source.getMetas());
    	}
    	if(source.isAlive()!=null){
    		target.setAlive(source.isAlive());
    	}
	}
}
