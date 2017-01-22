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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.jlogstash.assembly.disruptor.JDisruptor;
import com.dtstack.jlogstash.assembly.disruptor.MapEvent;
import com.dtstack.jlogstash.factory.FilterFactory;
import com.dtstack.jlogstash.filters.BaseFilter;
import com.lmax.disruptor.WorkHandler;

/**
 * 
 * Reason: TODO ADD REASON(可选) 
 * Date: 2016年11月29日 下午15:30:18 
 * Company:www.dtstack.com
 * @author sishu.yss
 *
 */
public class FilterHandler implements WorkHandler<MapEvent> {

	private static Logger logger = LoggerFactory.getLogger(FilterHandler.class);
	
	private List<BaseFilter> filterProcessors;
	
	private static JDisruptor filterToOutputDisruptor;
	
	public FilterHandler(List<BaseFilter> filterProcessors){
		this.filterProcessors = filterProcessors;
	}

	@Override
	public void onEvent(MapEvent mapEvent) throws Exception {
		// TODO Auto-generated method stub
		if (filterProcessors != null) {
			Map<String,Object> event =mapEvent.getEvent();
			for (BaseFilter bf : filterProcessors) {
				if (event == null || event.size() == 0)break;
				bf.process(event);
			}
			if(event!=null&&event.size()>0)filterToOutputDisruptor.put(event);
		}
	}
	
	public static WorkHandler<MapEvent> getHandlerInstance(List<BaseFilter> filterProcessors){
		return new FilterHandler(filterProcessors);
	}

	@SuppressWarnings("rawtypes")
	public static WorkHandler<MapEvent>[] getArrayHandlerInstance(
			List<Map> filters,int works) throws Exception {
		// TODO Auto-generated method stub
		WorkHandler<MapEvent>[] handlers = new FilterHandler[works];
		for(int i=0;i<works;i++){
			handlers[i] = getHandlerInstance((FilterFactory.getBatchInstance(filters)));
		}
		return handlers;
	}

	public static JDisruptor getFilterToOutputDisruptor() {
		return filterToOutputDisruptor;
	}

	public static void setFilterToOutputDisruptor(JDisruptor filterToOutputDisruptor) {
		FilterHandler.filterToOutputDisruptor = filterToOutputDisruptor;
	} 
	
}
