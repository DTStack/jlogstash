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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.parser.UserAgentUtil;
import com.google.common.collect.Maps;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:55:14
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class UA extends BaseFilter {
	
	private static final Logger logger = LoggerFactory.getLogger(UA.class);
	
	private String tagOnFailure="UaParserfail";
	
	private static Map<String,Map<String,Object>> msm = Maps.newHashMap();

	public UA(Map config) {
		super(config);
	}

	@Required(required=true)
	private static String source;
	

	public void prepare() {
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Map filter(Map event) {
		boolean ifsuccess = true;
		try{
			if (event.containsKey(source)) {
				String us = (String)event.get(source);
				Map<String,Object> mm = msm.get(us);
				if(mm==null){
					mm =UserAgentUtil.getUserAgent((us));
				}
				event.put(source, mm);
			}
		}catch(Exception e){
			ifsuccess =false;
			logger.error(e.getMessage());
		}
		return event;
	}
}
