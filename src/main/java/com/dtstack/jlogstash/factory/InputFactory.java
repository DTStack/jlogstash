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

import com.dtstack.jlogstash.assembly.disruptor.JDisruptor;
import com.dtstack.jlogstash.inputs.BaseInput;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:26:28
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class InputFactory extends InstanceFactory{
	
	@SuppressWarnings("rawtypes")
	public static BaseInput getInstance(String inputType,Map inputConfig) throws Exception{
	    Class<?> inputClass = getPluginClass(inputType, "input");
		configInstance(inputClass,inputConfig);//设置static field
		Constructor<?> ctor = inputClass.getConstructor(Map.class);
		BaseInput inputInstance = (BaseInput) ctor.newInstance(inputConfig);
		configInstance(inputInstance,inputConfig);//设置非static field
		inputInstance.prepare();
		return inputInstance;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static List<BaseInput> getBatchInstance(List<Map> inputs,JDisruptor inputToFilterDisruptor) throws Exception{
		BaseInput.setInputToFilterDisruptor(inputToFilterDisruptor);
		List<BaseInput> baseinputs =Lists.newArrayList();
		for (Map input : inputs) {
			Iterator<Entry<String, Map>> inputIT = input.entrySet().iterator();
			while (inputIT.hasNext()) {
				Map.Entry<String, Map> inputEntry = inputIT.next();
				String inputType = inputEntry.getKey();
				Map inputConfig = inputEntry.getValue();
				if(inputConfig==null)inputConfig=Maps.newLinkedHashMap();
				BaseInput baseInput =getInstance(inputType, inputConfig);
				baseinputs.add(baseInput);
			}
		}
	    return baseinputs;
	}
}
