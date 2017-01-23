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
package com.dtstack.jlogstash.assembly;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.jlogstash.exception.LogstashException;
import com.dtstack.jlogstash.property.SystemProperty;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年11月30日 下午1:25:18
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class CmdLineParams {
	
	private static Logger logger  = LoggerFactory.getLogger(CmdLineParams.class);
	
	private static CommandLine line;
	 
	public static void setLine(CommandLine line) {
		CmdLineParams.line = line;
	}

	/**
	 * 获取filter线程数
	 * @param line
	 * @return
	 */
	public static int getFilterWork(){
		String number =line.getOptionValue("w");
        int works =StringUtils.isNotBlank(number)?Integer.parseInt(number):SystemProperty.getInputBase();	
		logger.warn("filter works:{}",String.valueOf(works));
        return works;
	}
	
	/**
	 * 获取output线程数
	 * @param line
	 * @return
	 */
	public static int getOutputWork(){
		String number =line.getOptionValue("o");
        int works =StringUtils.isNotBlank(number)?Integer.parseInt(number):SystemProperty.getOutputBase();	
		logger.warn("output works:{}",String.valueOf(works));
        return works;
	}
	
	public static int getFilterRingBuffer() throws LogstashException{
		String number =line.getOptionValue("i");
        int works =StringUtils.isNotBlank(number)?Integer.parseInt(number):SystemProperty.getBaseIndex();	
        int size = 1<< works;
        if(size < 0){
        	throw new LogstashException("RingBuffer too small");
        }
        if(size > Integer.MAX_VALUE){
        	throw new LogstashException("RingBuffer too big");
        }
		logger.warn("filter ringbuffer:{}",String.valueOf(size));
        return size;
	}
	
	public static int getOutputRingBuffer() throws LogstashException{
		String number =line.getOptionValue("c");
        int works =StringUtils.isNotBlank(number)?Integer.parseInt(number):SystemProperty.getBaseIndex();	
        int size =  1<< works;
        if(size < 0){
        	throw new LogstashException("RingBuffer too small");
        }
        if(size > Integer.MAX_VALUE){
        	throw new LogstashException("RingBuffer too big");
        }
		logger.warn("output ringbuffer:{}",String.valueOf(size));
        return size;
	}
	
	public static String getWaitStrategy(){
		String strategy =line.getOptionValue("s");
        return StringUtils.isNotBlank(strategy)?strategy:"yielding";
	}
	
	public static String getConfigFilePath(){
		return line.getOptionValue("f");
	}
	
	public static String getLogFilePath(){
		return line.getOptionValue("l");
	}
	
	public static boolean hasOptionVVVV(){
		return line.hasOption("vvvv");
	}
	
	public static boolean hasOptionVV(){
		return line.hasOption("vv");
	}
	
	public static boolean hasOptionV(){
		return line.hasOption("v");
	}
}
