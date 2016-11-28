package com.dtstack.logstash.factory;

import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.dtstack.logstash.filters.BaseFilter;
import com.dtstack.logstash.utils.Package;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:26:24
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class FilterFactory extends InstanceFactory{

	
	@SuppressWarnings("rawtypes")
	public static BaseFilter getInstance(String filterType,Map filterConfig) throws Exception{
	      Class<?> filterClass = Class
                  .forName(Package.getRealClassName(filterType, "filter"));
	      configInstance(filterClass,filterConfig);//设置static field
          Constructor<?> ctor = filterClass
                  .getConstructor(Map.class);
          BaseFilter filterInstance = (BaseFilter) ctor
                  .newInstance(filterConfig);
	      configInstance(filterInstance,filterConfig);//设置非static field
          filterInstance.prepare();
          return filterInstance;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static List<BaseFilter> getBatchInstance(List<Map> filters) throws Exception{
		if(filters==null||filters.size()==0)return null;
		List<BaseFilter> baseFilters = Lists.newArrayList();
		for(Map filter:filters){
			Iterator<Entry<String, Map>> filterIT = filter.entrySet().iterator();
			while (filterIT.hasNext()) {
				Map.Entry<String, Map> filterEntry = filterIT.next();
				String filterType = filterEntry.getKey();
				Map filterConfig = filterEntry.getValue();
				if(filterConfig==null)filterConfig=Maps.newLinkedHashMap();
				baseFilters.add(getInstance(filterType,filterConfig));
			}
		}
		return baseFilters;
	}
}
