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
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.jlogstash.annotation.Required;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:53:50
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class Json extends BaseFilter {
    private static final Logger logger = LoggerFactory.getLogger(Json.class);
    
    private static ObjectMapper objectMapper = new ObjectMapper();

    public Json(Map config) {
        super(config);
    }

    @Required(required=true)
    private static Map<String,String> fields;
        
    private String tagOnFailure="JsonParserfail";

    public void prepare() {
    }

    @SuppressWarnings("rawtypes")
	@Override
    protected Map filter(final Map event) {
        Set<Map.Entry<String, String>>set =fields.entrySet();     
        for(Map.Entry<String, String> entry:set){
        	String key = entry.getKey();
        	String value = entry.getValue();
            if (event.containsKey(key)) {
                try {
                	 Object obj =objectMapper.readValue((String)event.get(key), Map.class);
                    if (obj != null) {
                        if (StringUtils.isNotBlank(value)) {
                            event.put(value,obj);
                        } else {
                            event.put(key, obj);
                        }
                    }
                } catch (Exception e) {
                    logger.error("failed to json parse field:" + key,e.getCause());
                }
            }
        }
        return event;
    }
}
