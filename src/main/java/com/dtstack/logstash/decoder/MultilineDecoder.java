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
package com.dtstack.logstash.decoder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.logstash.assembly.qlist.InputQueueList;
import com.dtstack.logstash.decoder.IDecode;
import com.google.common.collect.Lists;
/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年11月18日
 * Company: www.dtstack.com
 * @author xuchao
 *
 */
public class MultilineDecoder implements IDecode {
	
	private static final Logger logger = LoggerFactory.getLogger(MultilineDecoder.class);
	
	private String pattern;
	
	private String what; //one of (previous, next)
	
	private boolean negate = false;
	
	private Pattern patternReg; //FIXME 是否需要支持grok
	
	private Map<String, List<String>> bufferMap = new ConcurrentHashMap<String, List<String>>();
	
	private Map<String, Integer> readSizeMap = new ConcurrentHashMap<String, Integer>();
	
	private Map<String, Long> lastFlushMap = new ConcurrentHashMap<String, Long>();
	
	/**累计最大的内存大小**/
	private int max_size = 5 * 1024;
	
	private String customLineDelimiter = (char)29 +"";//使用不可见字符作为分隔符
	
	/**数据刷新间隔,默认20s内未有新数据刷新buff*/
	private int flushInterval = 20 * 1000;
	
	private int expiredInterval = 60 * 60 * 1000;//FIXME 失效时间不要设置超过INTEGER_MAX
	
	private InputQueueList inputQueueList;
	
	private ScheduledExecutorService scheduleExecutor;
		
	public MultilineDecoder(String pattern, String what, InputQueueList queuelist){
		init(pattern, what, queuelist);
	}
	
	public MultilineDecoder(String pattern, String what, boolean negate, InputQueueList queuelist){
		
		this.negate = negate;
		this.init(pattern, what, queuelist);
	}
		
	public void init(String pattern, String what, InputQueueList queuelist){
		
		if(pattern == null || what == null){
			logger.error("pattern and what must not be null.");
			System.exit(-1);
		}
		
		if(!"previous".equals(what) && !"next".equals(what)){
			logger.error("parameter what must in [previous, next]");
			System.out.println(-1);
		}
		
		this.inputQueueList = queuelist;
		
		logger.warn("MultilineDecoder param pattern:{}, what:{}, negate:{}.", new Object[]{pattern, what, negate});
		
		this.pattern = pattern;
		this.what = what;
		
		patternReg = Pattern.compile(this.pattern);
		scheduleExecutor = Executors.newScheduledThreadPool(1);
		scheduleExecutor.scheduleWithFixedDelay(new FlushMonitor(), flushInterval, flushInterval, TimeUnit.MILLISECONDS);
	}

	@Override
	public Map<String, Object> decode(String message, String identify) {
		
		Matcher matcher = patternReg.matcher(message);
		boolean isMathcer = matcher.find();
		boolean hasMatcher = (isMathcer && !negate) || (!isMathcer && negate);
		Map<String, Object> rst;
				
		int readSize = getAndAdd(identify, message.length()) ;
		
		if("next".equals(what)){
			rst = doNext(identify, message, hasMatcher);
		}else{
			rst = doPrevious(identify, message, hasMatcher);
		}
		
		if(readSize >= max_size){
			rst = flush(identify);
		}
		
		return rst;
	}
	
	private void buffer(String identify, String msg){
		List<String> buffer = bufferMap.get(identify);
		if(buffer == null){
			buffer = Lists.newArrayList();
			bufferMap.put(identify, buffer);
		}
		
		buffer.add(msg);
	}
	
	private int getAndAdd(String identify, int addNum){
		Integer currNum = readSizeMap.get(identify);
		currNum = currNum == null ? addNum : (currNum + addNum);
		readSizeMap.put(identify, currNum);
		return currNum;
	}
	
	private Map<String, Object> flush(String identify){
		
		List<String> buffer = bufferMap.get(identify);
		
		if(buffer == null || buffer.size() == 0){
			return null;
		}
		
		String msg = StringUtils.join(buffer, customLineDelimiter);
		Map<String, Object> event = new HashMap<String, Object>();
		event.put("message", msg);
		buffer.clear();
		readSizeMap.remove(identify);
		lastFlushMap.put(identify, System.currentTimeMillis());
		return event;
	}
	
	private Map<String, Object> doNext(String identify, String msg, boolean matched){
		
		Map<String, Object> event = null;
		buffer(identify, msg);
		if(!matched){
			event = flush(identify);
		}
		
		return event;
	}
	
	private Map<String, Object> doPrevious(String identify, String msg, boolean matched){
		
		Map<String, Object> event = null;
		if(!matched){
			event = flush(identify);
		}
		
		buffer(identify, msg);
		
		return event;
	}

	@Override
	public Map<String, Object> decode(String message) {
		// TODO Auto-generated method stub
		logger.error("not support for this func");
		return null;
	}
	
	class FlushMonitor implements Runnable{

		@Override
		public void run() {
			long currTime = System.currentTimeMillis();
			
			Iterator<Entry<String, List<String>>> it = bufferMap.entrySet().iterator();
			for( ; it.hasNext() ; ){
				Entry<String, List<String>> buffer = it.next();
				Long lastUpdate = lastFlushMap.get(buffer.getKey());
				if(lastUpdate == null){
					lastFlushMap.put(buffer.getKey(), currTime);
					continue;
				}
				
				if(currTime - lastUpdate > flushInterval){
					Map<String, Object> event = flush(buffer.getKey());
					if(event != null){
						event.put("path", buffer.getKey());
						inputQueueList.put(event);
					}
				}
				
				if(currTime - lastUpdate > expiredInterval){
					it.remove();
					lastFlushMap.remove(buffer.getKey());
				}
			}
		}
	}
}


