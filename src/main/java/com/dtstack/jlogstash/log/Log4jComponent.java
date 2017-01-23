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

import org.apache.commons.cli.CommandLine;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.dtstack.jlogstash.assembly.CmdLineParams;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:27:09
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class Log4jComponent extends LogComponent{
	
    private static String pattern = "%d %p %C %t %m%n";
	

	@Override
	public void setupLogger() {
		String file =checkFile();
		DailyRollingFileAppender fa = new DailyRollingFileAppender();
		fa.setName("FileLogger");
		fa.setFile(file);
		fa.setLayout(new PatternLayout(pattern));
		setLevel(fa);
		fa.setAppend(true);
		fa.activateOptions();
		Logger.getRootLogger().addAppender(fa);
	}
	
	public void setLevel(DailyRollingFileAppender fa){
		if (CmdLineParams.hasOptionVVVV()) {
			fa.setThreshold(Level.TRACE);
			Logger.getRootLogger().setLevel(Level.TRACE);
		} else if (CmdLineParams.hasOptionVV()) {
			fa.setThreshold(Level.DEBUG);
		} else if (CmdLineParams.hasOptionV()) {
			fa.setThreshold(Level.INFO);
		} else {
			fa.setThreshold(Level.WARN);
		}
	}


}
