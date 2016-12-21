package com.dtstack.logstash.factory;

import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import com.dtstack.logstash.assembly.qlist.InputQueueList;
import com.dtstack.logstash.inputs.BaseInput;
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
	public static List<BaseInput> getBatchInstance(List<Map> inputs,InputQueueList inputQueueList) throws Exception{
		BaseInput.setInputQueueList(inputQueueList);
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
