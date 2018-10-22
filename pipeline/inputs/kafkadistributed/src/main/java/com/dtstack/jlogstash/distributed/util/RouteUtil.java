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
package com.dtstack.jlogstash.distributed.util;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.dtstack.jlogstash.render.Formatter;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年01月08日 下午1:16:37
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class RouteUtil {
	
	private static String hashKey;
	
	private static  String keyPrefix;
	
	private static String keyHashCode;
	
	public static String getFormatHashKey(Map<String,Object> event) {
		String prefix = Formatter.format(event,keyPrefix);
		int hashcode  = Formatter.format(event,keyHashCode).hashCode();
		String sign  = String.format("%s-%d", prefix,hashcode);
		return sign;
	}

	public static void setHashKey(String hashKey) {
		RouteUtil.hashKey = hashKey;
		if(StringUtils.isNotBlank(RouteUtil.hashKey)){
			String[] hs = hashKey.split(":");
			keyPrefix = hs[0];
			keyHashCode = hs[1];
		}
	}
}
