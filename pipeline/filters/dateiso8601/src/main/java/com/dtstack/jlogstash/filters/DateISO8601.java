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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.date.DateParser;
import com.dtstack.jlogstash.date.FormatParser;
import com.dtstack.jlogstash.date.UnixMSParser;
import com.dtstack.jlogstash.date.UnixParser;
import com.google.common.collect.Maps;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:52:50
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class DateISO8601 extends BaseFilter {
	private static final Logger logger = LoggerFactory.getLogger(DateISO8601.class);
	
//  match =>{"timestamp":{"srcFormat":"dd/MMM/yyyy:HH:mm:ss Z","target":"timestamp","timezone":"UTC","locale":"en"}}
	@Required(required=true)
	private static Map<String,Map<String,String>> match;
	
	private String tagOnFailure="DateISO8601fail";
	
	private Map<String,DateParser> parsers = Maps.newConcurrentMap();

	public DateISO8601(Map config) {
		super(config);
		super.tagOnFailure = tagOnFailure;
	}

	public void prepare() {
		try{
			Set<Map.Entry<String,Map<String,String>>> matchs =match.entrySet();
			for(Map.Entry<String,Map<String,String>> ma:matchs){
				String src =ma.getKey();
				Map<String,String> value = ma.getValue();
				String format =value.get("srcFormat");
				if (format.equalsIgnoreCase("UNIX")) {
					parsers.put(src, new UnixParser());
				} else if (format.equalsIgnoreCase("UNIX_MS")) {
					parsers.put(src, new UnixMSParser());
				} else {
					String timezone =value.get("timezone");
					String locale = value.get("locale");
					parsers.put(src,new FormatParser(format,timezone, locale));
				}
			}	
		}catch(Exception e){
			logger.error(e.getMessage());
			System.exit(1);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Map filter(Map event) {
		if(parsers.size()>0){
		  Set<Map.Entry<String,DateParser>> sets = parsers.entrySet();
          for(Map.Entry<String,DateParser> entry:sets){
            	  String src =entry.getKey();
            	  if(event.containsKey(src)){
            		    DateParser dateParser  = entry.getValue();
            		    String target = match.get(src).get("target");
            			String input = (String)event.get(src);
            			event.put(target, dateParser.parse(input).toString());
            	  }  
          }
		}
		return event;
	}
}
