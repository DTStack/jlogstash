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
package com.dtstack.jlogstash.annotation.plugin;

import java.lang.reflect.Field;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.dtstack.jlogstash.exception.PathException;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:24:57
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class PathPlugin implements AnnotationInterface{

	private static Pattern pattern = Pattern.compile("^\\.|^/|^[a-zA-Z]\\)?:?/.+(/$)?"); 

	@Override
	public void required(Field field, Object obj) throws Exception{
		// TODO Auto-generated method stub
		String path = (String)obj;
		Matcher matcher = pattern.matcher(path);
		if(!matcher.find()){
			throw new PathException(field.getName());
		}	
	}
}
