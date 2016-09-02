package com.dtstack.logstash.factory;

import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import com.dtstack.logstash.outputs.BaseOutput;
import com.dtstack.logstash.utils.Package;
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

	public static BaseOutput getInstance(String outputType,Map outputConfig) throws Exception{
		 Class<?> outputClass = Class.forName(Package.getRealClassName(outputType,"output"));
		 configInstance(outputClass,outputConfig);
         Constructor<?> ctor = outputClass.getConstructor(Map.class);
         BaseOutput baseOutput = (BaseOutput) ctor.newInstance(outputConfig);
         baseOutput.prepare();
         return baseOutput;
	}
	
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
