package com.dtstack.logstash.assembly;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.StringUtils;
import com.dtstack.logstash.monitor.MonitorInfo;
import com.dtstack.logstash.monitor.MonitorService;
import com.dtstack.logstash.property.SystemProperty;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年11月30日 下午1:25:18
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class CmdLineParams {
	
	private static MonitorInfo monitorInfo = new MonitorService().getMonitorInfoBean();
	
	/**
	 * 获取filter线程数
	 * @param line
	 * @return
	 */
	public static int getFilterWork(CommandLine line){
		String number =line.getOptionValue("w");
        int works =StringUtils.isNotBlank(number)?Integer.parseInt(number):monitorInfo.getProcessors();	
        return works;
	}
	
	
	/**
	 * 获取output线程数
	 * @param line
	 * @return
	 */
	public static int getOutputWork(CommandLine line){
		String number =line.getOptionValue("o");
        int works =StringUtils.isNotBlank(number)?Integer.parseInt(number):monitorInfo.getProcessors();	
        return works;
	}
	
	/**
	 *获取input queue size的大小
	 * @param line
	 * @return
	 */
	public static int getInputQueueSize(CommandLine line){
		String number =line.getOptionValue("iqs");
        return 	StringUtils.isNotBlank(number)?Integer.parseInt(number):Integer.parseInt(SystemProperty.getSystemProperty("inputQueueSize"));	
	}
	
	
	/**
	 *获取input queue number的大小
	 * @param line
	 * @return
	 */
	public static int getInputQueueNumber(CommandLine line){
		String number =line.getOptionValue("iqn");
        return StringUtils.isNotBlank(number)?Integer.parseInt(number):monitorInfo.getProcessors();	
	}
	
	/**
	 *获取output queue size的大小
	 * @param line
	 * @return
	 */
	public static int getOutputQueueSize(CommandLine line){
		String number =line.getOptionValue("oqs");
        return 	StringUtils.isNotBlank(number)?Integer.parseInt(number):Integer.parseInt(SystemProperty.getSystemProperty("inputQueueSize"));	
	}
	
	
	/**
	 *获取output queue number的大小
	 * @param line
	 * @return
	 */
	public static int getOutputQueueNumber(CommandLine line){
		String number =line.getOptionValue("oqn");
        return StringUtils.isNotBlank(number)?Integer.parseInt(number):monitorInfo.getProcessors();	
	}
	
	
	/**
	 * 是否开启InputQueueSize log日志输出
	 * @param line
	 * @return
	 */
	public static boolean isInputQueueSizeLog(CommandLine line){
		return line.hasOption("t");
	}

}
