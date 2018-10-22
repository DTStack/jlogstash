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

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.jlogstash.annotation.Required;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:52:37
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class Add extends BaseFilter {
	private static final Logger logger = LoggerFactory.getLogger(Add.class.getName());
	
	@Required(required=true)
	private static Map<String, Object> fields=null;
	
	@SuppressWarnings("rawtypes")
	public Add(Map config) {
		super(config);
	}

	@Override
	public void prepare() {

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected Map filter(final Map event) {
		try{
			Set<Map.Entry<String,Object>> sets =fields.entrySet();
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
								if(oo==null){break;}
							}
							if(oo!=null){event.put(key, oo);}
						}else if ("%{hostname}%".equals(vv)){
		        			event.put(key, getHostName());
		        		}else if("%{timestamp}%".equals(vv)){
		        			event.put(key, getTimeStamp());
		        		}else if("%{ip}%".equals(vv)){
		        			event.put(key, getHostAddress());
		        		}
		            }
				} 
			}
		}catch(Exception e){
			logger.error("Add filter error:{}",e.getCause());
		}
		return event;
	}
	
	@SuppressWarnings("unchecked")
	public static Object loopObject(String value,Object obj){
		if(obj instanceof Map){
			return ((Map<String,Object>)obj).get(value);
		} 
        return null;
	}
	
	public static String getHostNameForLiunx() {  
	    try {  
	        return (InetAddress.getLocalHost()).getHostName();  
	    } catch (Exception uhe) {  
	        String host = uhe.getMessage(); // host = "hostname: hostname"  
	        if (host != null) {  
	            int colon = host.indexOf(':');  
	            if (colon > 0) {  
	                return host.substring(0, colon);  
	            }  
	        }  
			logger.error("getHostName:{}",uhe.getCause());
	        return "UnknownHost";  
	    }  
	}  
	  
	public static String getHostName() {  
	    if (System.getenv("COMPUTERNAME") != null) {  
	        return System.getenv("COMPUTERNAME");  
	    } else {  
	        return getHostNameForLiunx();  
	    }  
	}  
	
	public static String getHostAddress(){
        try {
			InetAddress ia = InetAddress.getLocalHost();
			return ia.getHostAddress();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("getHostAddress:{}",e.getCause());
		}   
        return "127.0.0.1";
	}
	
	public static String getTimeStamp(){
		return DateTime.now().toString();
	}	
}
