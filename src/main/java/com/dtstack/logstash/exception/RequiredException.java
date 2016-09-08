package com.dtstack.logstash.exception;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:26:18
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class RequiredException extends LogstashException{
	
	private static String message = "param: %s is not empty";
	
	public RequiredException(String param){
		super(String.format(message, param));
	}
}
