package com.dtstack.logstash.log;

import org.apache.commons.cli.CommandLine;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

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
	public void setupLogger(CommandLine cmdLine) {
		String file =checkFile(cmdLine);
		DailyRollingFileAppender fa = new DailyRollingFileAppender();
		fa.setName("FileLogger");
		fa.setFile(file);
		fa.setLayout(new PatternLayout(pattern));
		setLevel(cmdLine,fa);
		fa.setAppend(true);
		fa.activateOptions();
		Logger.getRootLogger().addAppender(fa);
	}
	
	public void setLevel(CommandLine cmdLine,DailyRollingFileAppender fa){
		if (cmdLine.hasOption("vvvv")) {
			fa.setThreshold(Level.TRACE);
			Logger.getRootLogger().setLevel(Level.TRACE);
		} else if (cmdLine.hasOption("vv")) {
			fa.setThreshold(Level.DEBUG);
		} else if (cmdLine.hasOption("v")) {
			fa.setThreshold(Level.INFO);
		} else {
			fa.setThreshold(Level.WARN);
		}
	}

}
