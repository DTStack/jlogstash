package com.dtstack.logstash.log;

import org.apache.commons.cli.CommandLine;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:27:21
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public abstract class LogComponent {
	
	public void setupLogger(CommandLine cmdLine) {}
	
	protected String checkFile(CommandLine cmdLine){
		if(!cmdLine.hasOption("l")){
			System.out.println("log file is not empty");
			System.exit(1);
		}
        return 	cmdLine.getOptionValue("l")	;
	}
	
}
