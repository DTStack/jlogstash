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
import java.io.File;
import java.io.FileWriter;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.math.NumberUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.date.UnixMSParser;
import com.dtstack.jlogstash.outputs.BaseOutput;
import com.dtstack.jlogstash.render.Formatter;
import com.google.common.collect.Maps;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:36:10
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class Performance extends BaseOutput{
	
	private static Logger logger = LoggerFactory.getLogger(Performance.class);
	
	private static AtomicLong eventNumber = new AtomicLong(0);
		
	private static int interval = 30;//seconds
	
	/**清理文件执行间隔时间*/
	private static long monitorFileInterval = 10 * 60;//seconds
	
	private static String timeZone = "UTC";

	@Required(required=true)
	private static String path;
		
	/**key:文件路径, value:保留天数*/
	private static Map<String, String> monitorPath;
	
	private Map<String, String> fileTimeFormatMap = Maps.newHashMap();
	
	private Map<String, String> fileNameRegMap = Maps.newHashMap();
	
	private Map<String, String> pathDicMap = Maps.newHashMap();
			 	
	private static ExecutorService executor = Executors.newSingleThreadExecutor();
	
	private static ScheduledExecutorService scheduleExecutor = Executors.newScheduledThreadPool(1);
	
	public Performance(Map<String,Object> config){
		super(config);
	}

	class PerformanceEventRunnable implements Runnable{

		@Override
		public void run() {
			// TODO Auto-generated method stub
			while(true){
				BufferedWriter bufferedWriter = null;
				FileWriter fw = null;
				try {
					Thread.sleep(interval*1000);
					StringBuilder sb = new StringBuilder();
					long number =eventNumber.getAndSet(0);
					DateTime dateTime =new UnixMSParser().parse(String.valueOf(Calendar.getInstance().getTimeInMillis()));
					sb.append(dateTime.toString()).append(" ").append(number).append(System.getProperty("line.separator"));
					String newPath = Formatter.format(new HashMap<String,Object>(),path,timeZone);
					fw = new FileWriter(newPath,true);
					bufferedWriter = new BufferedWriter(fw);
					bufferedWriter.write(sb.toString());
					bufferedWriter.flush();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					logger.error(e.getMessage());
				}finally{
					try{
						if (bufferedWriter != null)bufferedWriter.close();
						if (fw!=null)fw.close();
					}catch(Exception e){
						logger.error(e.getMessage());
					}
				}
			}
		}
	}


	@Override
	public void prepare() {
		compileTimeInfo();
		executor.submit(new PerformanceEventRunnable());
		
		if(monitorPath != null && monitorPath.size() != 0){
			scheduleExecutor.scheduleWithFixedDelay(new ExpiredFileDeleRunnabel(), 0, monitorFileInterval, TimeUnit.SECONDS);
		}
	}
	
	@Override
	protected void emit(Map event) {
		// TODO Auto-generated method stub
		eventNumber.getAndIncrement();
	}
	
	public void compileTimeInfo(){
		
		if(monitorPath == null || monitorPath.size() == 0){
			logger.info("not setting monitorPath");
			return;
		}
		
		Pattern filePattern = Pattern.compile("^(.*)(/|\\\\)([^/\\\\]*(\\%\\{\\+?(.*?)\\})\\S+)$");
		
		for(Entry<String, String> tmp : monitorPath.entrySet()){
			
			Matcher matcher = filePattern.matcher(tmp.getKey());
			if(!matcher.find()){
				logger.error("input path:{} can not matcher to the pattern.");
				continue;
			}
			
			String fileName = matcher.group(3);
			String timeStr = matcher.group(4);
			String timeFormat = matcher.group(5);
			String fileNamePattern = fileName.replace(timeStr, "(\\S+)");
			
			pathDicMap.put(tmp.getKey(), matcher.group(1));
			fileNameRegMap.put(tmp.getKey(), fileNamePattern);
			fileTimeFormatMap.put(tmp.getKey(), timeFormat);
		}
		
	}
	
	class ExpiredFileDeleRunnabel implements Runnable{

		@Override
		public void run() {
			for(Entry<String, String> tmp : monitorPath.entrySet()){
				String dicFileStr = pathDicMap.get(tmp.getKey());
				if(dicFileStr == null){
					continue;
				}
				
				File dicFile = new File(dicFileStr);
				if(!dicFile.exists() || !dicFile.isDirectory()){
					continue;
				}
				
				int maxSaveDay = NumberUtils.toInt(tmp.getValue(), 0);
				String timeFormat = fileTimeFormatMap.get(tmp.getKey());
				String fileNamePattern = fileNameRegMap.get(tmp.getKey());
				
				deleExpiredFile(dicFile, timeFormat, fileNamePattern, maxSaveDay);
			}
		}
		
	}
	
	public void deleExpiredFile(File dic, String timeFormat, String fileNamePattern, int maxSaveDay){
		if(!dic.isDirectory()){
			logger.error("invalid file dictory:{}.", dic.getPath());
			return;
		}
		
		DateTimeFormatter formatter = DateTimeFormat.forPattern(timeFormat).
				withZone(DateTimeZone.forID(timeZone));
		
		DateTime expiredTime = new DateTime();
		expiredTime = expiredTime.plusDays(0-maxSaveDay);
		expiredTime = formatter.parseDateTime(expiredTime.toString(formatter));
		
		Pattern pattern = Pattern.compile(fileNamePattern);
		for(String fileName : dic.list()){
			Matcher matcher = pattern.matcher(fileName);
			if(matcher.find()){
				String timeStr = matcher.group(1);
				DateTime fileDateTime = formatter.parseDateTime(timeStr);
				
				if(fileDateTime.isBefore(expiredTime.getMillis())){
					File deleFile = new File(dic, fileName);
					if(deleFile.exists()){
						logger.info("delete expired file:{}.", fileName);
						deleFile.delete();
					}
				}
			}
		}
	}
}
