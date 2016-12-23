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
package com.dtstack.logstash.ua.parser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:28:11
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class UserAgentUtil {
	
    private static final Logger logger = LoggerFactory.getLogger(UserAgentUtil.class);
	
	public static  Parser uaParser =null;
	
	static{
		try {
			if (uaParser==null) uaParser = new Parser();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static Map<String,Object> getUserAgent(String agent){
		Client c =uaParser.parse(agent);
		Map<String,Object> result = new HashMap<String,Object>(); 
	    result.put("os", c.os.family);
	    result.put("os_v",getOsVersion(c.os));
	    result.put("browser", c.userAgent.family);
	    result.put("browser_v",getBrowserVersion(c.userAgent));
	    result.put("device", c.device.family);
		return result;	
	}
	
	
	public static String getBrowserVersion(UserAgent userAgent){
		StringBuilder sb =new StringBuilder();
		String major =userAgent.major;
		if (major!=null&&!"".equalsIgnoreCase(major)) sb.append(major);
		String minor =userAgent.minor;
		if (minor!=null&&!"".equalsIgnoreCase(minor)) sb.append(".").append(minor);
		String patch =userAgent.patch;
		if (patch!=null&&!"".equalsIgnoreCase(patch)) sb.append(".").append(patch);
		return sb.toString();
	}
	
	public static String getOsVersion(OS os){
		StringBuilder sb =new StringBuilder();
		String major = os.major;
		if (major!=null&&!"".equalsIgnoreCase(major)) sb.append(major);
		String minor =os.minor;
		if (minor!=null&&!"".equalsIgnoreCase(minor)) sb.append(".").append(minor);
		String patch =os.patch;
		if (patch!=null&&!"".equalsIgnoreCase(patch)) sb.append(".").append(patch);
		String patchMinor =os.patchMinor;
		if (patchMinor!=null&&!"".equalsIgnoreCase(patchMinor)) sb.append(".").append(patchMinor);
		return sb.toString();
	}	
}
