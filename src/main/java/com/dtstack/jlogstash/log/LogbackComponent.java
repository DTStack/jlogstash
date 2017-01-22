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

import ch.qos.logback.classic.Logger;

import org.apache.commons.cli.CommandLine;
import org.slf4j.LoggerFactory;

import com.dtstack.jlogstash.assembly.CmdLineParams;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:27:16
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class LogbackComponent extends LogComponent{
	
	private static String formatePattern ="%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{50} [%file:%line] - %msg%n";
	
	private static int day = 7;
	
	@Override
	public void setupLogger() {
	    String file = checkFile();
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger newLogger =loggerContext.getLogger("ROOT");
        //Remove all previously added appenders from this logger instance.
        newLogger.detachAndStopAllAppenders();
        //define appender
        RollingFileAppender<ILoggingEvent> appender = new RollingFileAppender<ILoggingEvent>();
        //policy
        TimeBasedRollingPolicy<ILoggingEvent> policy = new TimeBasedRollingPolicy<ILoggingEvent>();
        policy.setContext(loggerContext);
        policy.setMaxHistory(day);
        policy.setFileNamePattern(formateLogFile(file));
        policy.setParent(appender);
        policy.start();
        //encoder
        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(loggerContext);
        encoder.setPattern(formatePattern);
        encoder.start();
        //start appender
        appender.setRollingPolicy(policy);
        appender.setContext(loggerContext);
        appender.setEncoder(encoder);
        appender.setPrudent(true); //support that multiple JVMs can safely write to the same file.
        appender.start();
        newLogger.addAppender(appender);
        //setup level
        setLevel(newLogger);
        //remove the appenders that inherited 'ROOT'.
        newLogger.setAdditive(false);
	}
	
	private String formateLogFile(String file){
		int index =file.indexOf(".");
		if(index>=0){
			file =file.substring(0, index);
		}
		file =file+"_%d{yyyy-MM-dd}.log";
		return file;
	}

    /**
     * Set logger level in runtime
     * @param logger
     * @param cmdLine
     */
     public void setLevel(Logger logger){
    		if (CmdLineParams.hasOptionVVVV()) {
    			logger.setLevel(Level.TRACE);
    		} else if (CmdLineParams.hasOptionVV()) {
    			logger.setLevel(Level.DEBUG);
    		} else if (CmdLineParams.hasOptionV()) {
    			logger.setLevel(Level.INFO);
    		} else {
    			logger.setLevel(Level.WARN);
    		}
     }
	
}
