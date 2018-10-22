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
package com.dtstack.jlogstash.log;

import org.apache.commons.lang3.StringUtils;
import com.dtstack.jlogstash.assembly.CmdLineParams;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:27:21
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public abstract class LogComponent {
	
	public void setupLogger() {}
	
	protected String checkFile(){
		String logfile = CmdLineParams.getLogFilePath();
		if(StringUtils.isBlank(logfile)){
			return String.format("%s/%s", System.getProperty("user.dir"),"logs/jlogstash.log");
		}
		return logfile;
	}
}
