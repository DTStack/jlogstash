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
package com.dtstack.jlogstash.filters;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.jlogstash.annotation.Required;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:54:23
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class Remove extends BaseFilter {
	@SuppressWarnings("rawtypes")
	public Remove(Map config ) {
		super(config);
	}
	
	Logger logger = LoggerFactory.getLogger(Remove.class);
	
    @Required(required=true)
	private static List<String> fields;
    
    private static boolean removeNUll = false;

	public void prepare() {
		
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Map filter(Map event) {
		try{
			if(event!=null&&event.size()>0){
		        if (removeNUll){
		        	removeNull(event,0);
		        }else{
		        	remove(event);
		        }
			}

		}catch(Exception e){
			logger.error("{}:Remove error:{}",event,e.getCause());
		}
		return event;
	}
	
	
    private void remove(@SuppressWarnings("rawtypes") Map event){
		if(fields!=null&&fields.size()>0){
			for (String t : fields) {
				event.remove(t);
			}
		}
    }
    
//    @SuppressWarnings({ "rawtypes", "unchecked" })
//	private void removeNull(Map event,int index){
//    	Iterator<Map.Entry<String, Object>> sets = event.entrySet().iterator();	
//    	while(sets.hasNext()){
//    		Map.Entry<String, Object> entry = sets.next();
//    		String key =entry.getKey();
//    		if(index==0){
//    			if(fields.contains(key)){
//    				sets.remove();
//    				continue;
//    			}
//    		}
//    		Object obj = entry.getValue();
//    		if(obj==null||"".equals(obj.toString().trim())){
//    			sets.remove();
//    		}else if(obj instanceof Map){
//    			removeNull((Map)obj,1);
//    		}	
//    	}
//    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
	private void removeNull(Object event,int index){
    	if(event instanceof Map){
        	Iterator<Map.Entry<String, Object>> sets = ((Map)event).entrySet().iterator();	
        	while(sets.hasNext()){
        		Map.Entry<String, Object> entry = sets.next();
        		String key =entry.getKey();
        		if(index==0){
        			if(fields.contains(key)){
        				sets.remove();
        				continue;
        			}
        		}
        		Object obj = entry.getValue();
        		if(obj==null||"".equals(obj.toString().trim())){
        			sets.remove();
        		}else if(obj instanceof Map){
        			removeNull((Map)obj,1);
        		}else if(obj instanceof List){
            		for(Object objj:(List)obj){
            			removeNull(objj,1);
            		}
            	}	
        	}
    	}else if(event instanceof List){
    		for(Object obj:(List)event){
    			removeNull(obj,1);
    		}
    	}
    }
    
//    public static void main(String[] args){
//    	Remove.fields = Lists.newArrayList("fdsfsd");
//    	Remove.removeNUll = false;
//    	Map<String,Object> event  = new HashMap<String,Object>();
//    	List<Map<String,Object>> ll = Lists.newArrayList();
//    	Map<String,Object> gg = Maps.newHashMap();
//    	gg.put("gdgd", null);
//    	gg.put("gdgdggg", "hfdgdf");
//    	ll.add(gg);
//    	event.put("fdsfsd","fdfsd");
//    	event.put("gsdfsf", null);
//    	event.put("binds",ll);
//    	Remove remove = new Remove(new HashMap<String,Object>());
//    	remove.prepare();
//    	remove.filter(event);
//    	System.out.println(event);
//    }
}
