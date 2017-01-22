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
package com.dtstack.jlogstash.factory;

import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.dtstack.jlogstash.outputs.BaseOutput;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:26:39
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class OutputFactory extends InstanceFactory{

	@SuppressWarnings("rawtypes")
	public static BaseOutput getInstance(String outputType,Map outputConfig) throws Exception{
	     Class<?> outputClass = getPluginClass(outputType, "output");
		 configInstance(outputClass,outputConfig);//设置static field
         Constructor<?> ctor = outputClass.getConstructor(Map.class);
         BaseOutput baseOutput = (BaseOutput) ctor.newInstance(outputConfig);
		 configInstance(baseOutput,outputConfig);//设置非static field
         baseOutput.prepare();
         return baseOutput;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static List<BaseOutput> getBatchInstance(List<Map> outputs) throws Exception{
		if(outputs==null||outputs.size()==0)return null;
		List<BaseOutput> baseoutputs = Lists.newArrayList();
		for(Map output:outputs){
			Iterator<Entry<String, Map>> outputIT = output.entrySet().iterator();
			while (outputIT.hasNext()) {
				Map.Entry<String, Map> outputEntry = outputIT.next();
				String outputType = outputEntry.getKey();
				Map outputConfig = outputEntry.getValue();
				if(outputConfig==null)outputConfig=Maps.newLinkedHashMap();
				baseoutputs.add(getInstance(outputType,outputConfig));
			}
		}
		return baseoutputs;
	}
}
