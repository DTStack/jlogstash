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
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.jlogstash.annotation.Required;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * 
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class IpIp extends BaseFilter{
		
	private static Logger logger = LoggerFactory.getLogger(IpIp.class);

	private Pattern ipv4Pattern = Pattern.compile("^\\d{1,3}\\.\\d{1,3}.\\d{1,3}.\\d{1,3}$");
	
	@Required(required=true)
	private static Map<String,String> souTar;
	
	public static int size = 50000;
	
	static{
		IP.load("17monipdb.dat");
	}
		
	public static Cache<String, Map<String, Object>> cache;

    @SuppressWarnings("rawtypes")
	public IpIp(Map config) {
		super(config);
		// TODO Auto-generated constructor stub
	}
    
    
	@Override
	public void prepare() {
		if(cache==null){
			cache = CacheBuilder.newBuilder() .maximumSize(size).build();
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected Map filter(Map event){
		 try{
			 Set<Map.Entry<String,String>> entrys = souTar.entrySet();
			 for(Map.Entry<String,String> entry:entrys){ 
				  String ip = (String) event.get(entry.getKey());
				  if(StringUtils.isNoneBlank(ip)){
					  Map<String,Object> re =(Map<String,Object>)cache.getIfPresent(ip);
					  if(re==null){

					  	  if(!checkIsIpFormat(ip)){
					  	  	  continue;
						  }

						  String[] result =IP.find(ip);
						  if(result!=null){
							  re = new HashMap<String,Object>();
							  re.put("country", result[0]);
							  re.put("province", result[1]);
							  re.put("city", result[2]);
							  cache.put(ip, re);
						  }
					  }
					  event.put(entry.getValue(), re);
				  }
			 }	 
		 }catch(Exception e){
			logger.error("DtLogIpIp_filter_error", e);
		 }
		return event;
	}

	public boolean checkIsIpFormat(String ipStr){
		Matcher matcher = ipv4Pattern.matcher(ipStr);
		return matcher.find();
	}
}
