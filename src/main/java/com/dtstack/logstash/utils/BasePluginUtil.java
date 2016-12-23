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
package com.dtstack.logstash.utils;

import java.util.Map;
import java.util.Set;

import org.joda.time.DateTimeZone;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年12月09日 下午11:27:03
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class BasePluginUtil {
	
		public void addFields(Map<String,Object> event,Map<String,Object> addFields){
			Set<Map.Entry<String,Object>> sets =addFields.entrySet();
			for(Map.Entry<String,Object> entry:sets){
				String key = entry.getKey();
				if(event.get(key)==null){
					Object value = entry.getValue();
					event.put(key, value);
					if(event.get(value)!=null){
						event.put(key, event.get(value));
					}else if(value instanceof String){
						String vv =value.toString();
						if(vv.indexOf(".")>0){
							String[] vs=vv.split("\\.");
							Object oo = event;
							for(int i=0;i<vs.length;i++){
								oo = loopObject(vs[i],oo);
								if(oo==null)break;	
							}
							if(oo!=null)event.put(key, oo);	
						}else if ("%{hostname}%".equals(vv)){
		        			event.put(key, Public.getHostName());
		        		}else if("%{timestamp}%".equals(vv)){
		        			event.put(key,Public.getTimeStamp(null));
		        		}else if("%{timestamp-utc}%".equals(vv)){
		        			event.put(key,Public.getTimeStamp(DateTimeZone.UTC));
		        		}else if("%{ip}%".equals(vv)){
		        			event.put(key, Public.getHostAddress());
		        		}
		            }
				} 
			}
	    }
	  
		@SuppressWarnings("unchecked")
		private Object loopObject(String value,Object obj){
			if(obj instanceof Map){
				return ((Map<String,Object>)obj).get(value);
			} 
	        return null;
		}
}
