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
package com.dtstack.logstash.assembly.qlist;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import com.google.common.collect.Lists;



/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年11月30日 下午1:25:11
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public abstract class QueueList {
	
	protected List<BlockingQueue<Map<String, Object>>> queueList = Lists.newArrayList();

	public abstract void put(Map<String, Object> message);

	public abstract void startElectionIdleQueue();
	
	public abstract void startLogQueueSize();
	
	public boolean allQueueEmpty() {
		boolean result = true;
		for (BlockingQueue<Map<String, Object>> queue : queueList) {
			result = result && queue.isEmpty();
		}
		return result;
	}
	
	public int allQueueSize(){
		int size=0;
		for (BlockingQueue<Map<String, Object>> queue : queueList) {
			size = size+queue.size();
		}
		return size;
	} 
	
	public abstract void queueRelease();

	public List<BlockingQueue<Map<String, Object>>> getQueueList() {
		return queueList;
	}
}
