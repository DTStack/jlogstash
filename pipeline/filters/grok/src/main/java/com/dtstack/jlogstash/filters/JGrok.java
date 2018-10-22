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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.jlogstash.annotation.Required;
import oi.thekraken.grok.api.Grok;
import oi.thekraken.grok.api.Match;
import oi.thekraken.grok.api.exception.GrokException;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:53:24
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class JGrok extends BaseFilter {

    private static final Logger logger = LoggerFactory.getLogger(JGrok.class.getName());

    @Required(required=true)
    private static List<String> srcs;
    
    private static Map<String, String> patterns;

    private static String patternFile;
    
    private Grok grok =null;

    public JGrok(Map config) {
        super(config);
    }


    static {
    	PatternRead.patternRead();
    }
		
	
	/**
	 * 正则解析
	 * @param event
	 * @param value
	 */
	public  void parserGrok(Map<String,Object> event,String value){
		try{
			Map<String,String> patterns11 =grok.getPatterns();
			Match match =grok.match(value);
			match.captures();
			Map<String,Object> map =match.toMap();
			if(map!=null&&map.size()>0){
				Set<Map.Entry<String,Object>> sets =map.entrySet();
				for(Map.Entry<String,Object> set:sets){
					String key = set.getKey();
					if(!patterns11.containsKey(key)){
						event.put(key, set.getValue());
					}
				}
			}
		}catch(Exception e){
		   logger.error("parserGrok_error", e);
		}
	}
	
	private void addPatternToGrok() throws GrokException{
		Map<String,String> patterns =PatternRead.getPatterns();
		if(patterns.size()>0){
			Set<Map.Entry<String, String>> sets =patterns.entrySet();
			for(Map.Entry<String, String> entry:sets){
				grok.addPattern(entry.getKey(), entry.getValue());
			}
		}
	}
    

    @SuppressWarnings("unchecked")
	@Override
    public void prepare() {
		if(grok==null){
			try {
				grok = new Grok();
				addPatternToGrok();
				addFromPatternFile();
				addFromPatternMap();
			} catch (Exception e) {
				logger.error("grok compile is error:", e);
				System.exit(1);
			}
	
		}
    }

    private void addFromPatternFile(){
		if(StringUtils.isBlank(patternFile)){
			return;
		}

		BufferedReader br = null;
		InputStreamReader ir = null;

		try {
			ir = new InputStreamReader(new FileInputStream(patternFile));
			br = new BufferedReader(ir);
			String line;
			// We dont want \n and commented line
			Pattern pattern = Pattern.compile("^([A-z0-9_]+)\\s+(.*)$");
			while ((line = br.readLine()) != null) {
				Matcher m = pattern.matcher(line);
				if (m.matches()) {
					grok.addPattern(m.group(1), m.group(2));
//					grok.compile(m.group(1));
				}
			}
		} catch (Exception e) {
			logger.error("compile grok pattern from file " + patternFile + " error.", e);
		} finally {
			try {
				if (br != null) {
					br.close();
				}
				if (ir != null) {
					ir.close();
				}

			} catch (IOException e) {
				logger.error("close file io error:", e);
			}
		}

		logger.info("add pattern from file:{} success.", patternFile);
	}

	private void addFromPatternMap(){

    	if(patterns == null){
    		return;
		}

    	try{
			Set<Map.Entry<String, String>> entrys =patterns.entrySet();
			for(Map.Entry<String, String> entry:entrys){
				String key = entry.getKey();
				String value = entry.getValue();
				if(StringUtils.isNotBlank(value)){
					grok.addPattern(key,value);
					grok.compile(key);
				}else{
					grok.compile(key);
				}
			}
		}catch (Exception e){
    		logger.error("grok compile is error:", e);
    		System.exit(-1);
		}

	}

    @SuppressWarnings("unchecked")
    @Override
    protected Map filter(Map event) {
    	try{
    		for(String src:srcs){
        		Object str = event.get(src);
        		if(StringUtils.isNotBlank((String)str)){
        			parserGrok(event,(String)str);
        		}
    		}
    	}catch(Exception e){
    		logger.error("grok filter is error: {}",e.getCause());
    	}
    	return event;  
    }


}
