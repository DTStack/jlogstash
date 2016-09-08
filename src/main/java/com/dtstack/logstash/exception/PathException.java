package com.dtstack.logstash.exception;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:26:12
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class PathException extends LogstashException{
	
	private static String message = "Path: %s format is error";

	public PathException(String name){
		super(String.format(message, name));
	}
	
}
