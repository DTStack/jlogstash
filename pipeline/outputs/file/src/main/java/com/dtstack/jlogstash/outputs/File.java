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
package com.dtstack.jlogstash.outputs;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.outputs.BaseOutput;
import com.dtstack.jlogstash.render.Formatter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:35:44
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class File extends BaseOutput{
	
    private static Logger logger = LoggerFactory.getLogger(File.class);
	
    @Required(required=true)
	private static String path;
	
	private static String codec="json_lines";
			
	private static ObjectMapper objectMapper = new ObjectMapper();
		
	private List<String> codecol = Lists.newCopyOnWriteArrayList();
	
	private String format =null;
	
	private String split = null;
	 
	private Map<String,BufferedWriter> msf = Maps.newConcurrentMap();
	
	private ExecutorService executor = Executors.newSingleThreadExecutor();
	
	private static int interval =5;//seconds
	
	private static String timeZone = "UTC";

	    
	public File(Map config) {
		super(config);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void prepare() {
		// TODO Auto-generated method stub
		try{
	        if(codec.startsWith("line")){
	        	Map<String,String> code =objectMapper.readValue(codec.replace("line","").trim(), Map.class);
	        	format =code.get("format");
	        	split = code.get("split");
	        	if(StringUtils.isBlank(format)||StringUtils.isBlank(split)){
	        		logger.error("codec line is config error...");
	        		System.exit(1);
	        	}
	        	codecol = Arrays.asList(format.split(split));
			}
	       executor.submit(new FileRunnable());   
		}catch(Exception e){
			logger.error("File filter prepare is error", e);
			System.exit(1);
		}
		
	}
	
	class FileRunnable implements Runnable{
		@Override
		public void run() {
			// TODO Auto-generated method stub
			try{
				while(true){
					Thread.sleep(interval*1000);
					Set<Map.Entry<String,BufferedWriter>> sets =msf.entrySet();
					for(Map.Entry<String,BufferedWriter> entry :sets){
						entry.getValue().flush();
					}
				}
			}catch(Exception e){
				logger.error(e.getMessage());
			}
		}
	}

	@Override
	protected void emit(Map event) {
		// TODO Auto-generated method stub
		try {
			String message =formatMessage(event)+System.getProperty("line.separator");
				BufferedWriter bw =getFileWriter(event);
				bw.write(message);		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("file emit fail...", e);
		}
	}
	
	@Override
	public void release(){
		try{
			if(msf!=null){
				Set<Map.Entry<String,BufferedWriter>> sets =msf.entrySet();
				for(Map.Entry<String,BufferedWriter> entry :sets){
					entry.getValue().close();
				}
			}
		}catch(Exception e){
			logger.error("output file release error:{}",e.getMessage());
		}
	}

	private String formatMessage(Map event) throws Exception{
	  if(codec.startsWith("line")){
			StringBuilder sb = new StringBuilder();
			for(String col:codecol){
				sb.append(event.get(col)).append(split);
			}
			String result =sb.toString();
			if(StringUtils.isNotBlank(result)){
				result = result.substring(0, result.length()-1);
			}
			return result;
		}
		return objectMapper.writeValueAsString(event);
	}
	
	private BufferedWriter getFileWriter(Map event) throws Exception{
		String newPath = Formatter.format(event, path,timeZone);
		BufferedWriter bw =msf.get(newPath);
		if(bw==null){
			FileWriter fw= new FileWriter(newPath,true);
			bw = new BufferedWriter(fw);
			msf.put(newPath, bw);
		}
		return bw;
	}
}
