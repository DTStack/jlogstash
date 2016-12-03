package com.dtstack.logstash.assembly;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.logstash.monitor.MonitorInfo;
import com.dtstack.logstash.monitor.MonitorService;
import com.dtstack.logstash.property.SystemProperty;
import com.dtstack.logstash.utils.Public;


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
	
	private static MonitorInfo monitorInfo = new MonitorService().getMonitorInfoBean();
	
	
    /**
     * 获取input queue size 系数
     * @param line
     * @return
     */
	public static double getInputQueueCoefficient(CommandLine line){
		String number =line.getOptionValue("c");
		double coefficient =StringUtils.isNotBlank(number)?Double.parseDouble(number):SystemProperty.getInputProportion();	
		logger.warn("input queue coefficient:{}",String.valueOf(coefficient));
		return coefficient;
	}
	
    /**
     * 获取output queue size 系数
     * @param line
     * @return
     */
	public static double getOutputQueueCoefficient(CommandLine line){
		String number =line.getOptionValue("i");
		double coefficient =StringUtils.isNotBlank(number)?Double.parseDouble(number):SystemProperty.getOutputProportion();	
		logger.warn("output queue coefficient:{}",String.valueOf(coefficient));
		return coefficient;
	}
	
	/**
	 * 获取filter线程数
	 * @param line
	 * @return
	 */
	public static int getFilterWork(CommandLine line){
		String number =line.getOptionValue("w");
        int works =StringUtils.isNotBlank(number)?Integer.parseInt(number):getInputBase();	
		logger.warn("filter works:{}",String.valueOf(works));
        return works;
	}
	
	
	/**
	 * 获取output线程数
	 * @param line
	 * @return
	 */
	public static int getOutputWork(CommandLine line){
		String number =line.getOptionValue("o");
        int works =StringUtils.isNotBlank(number)?Integer.parseInt(number):getOutputBase();	
		logger.warn("output works:{}",String.valueOf(works));
        return works;
	}
	
	/**
	 *获取input queue size的大小
	 * @param line
	 * @return
	 */
	public static int getInputQueueSize(CommandLine line){
		float number = getFilterWork(line);
        int size = Public.getIntValue(monitorInfo.getJvmMaxMemory()*getInputQueueCoefficient(line)*((float)getInputBase()/number));
		logger.warn("input queue size:{}",String.valueOf(size));
        return size;
	}
	
	private static int getInputBase(){
		int process = monitorInfo.getProcessors();
		return process + process/2;
	}
	
	private static int getOutputBase(){
		int process = monitorInfo.getProcessors();
		return process;
	}
		
	
	/**
	 *获取output queue size的大小
	 * @param line
	 * @return
	 */
	public static int getOutputQueueSize(CommandLine line){
		float number =getOutputWork(line);
		int size = Public.getIntValue(monitorInfo.getJvmMaxMemory()*getOutputQueueCoefficient(line)*((float)getOutputBase()/number));
		logger.warn("output queue size:{}",String.valueOf(size));
		return size;	
	}
	
	/**
	 * 是否开启QueueSize log日志输出
	 * @param line
	 * @return
	 */
	public static boolean isQueueSizeLog(CommandLine line){
		return line.hasOption("t");
	}	
}
