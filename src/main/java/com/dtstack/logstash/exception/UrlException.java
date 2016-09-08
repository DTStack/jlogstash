package com.dtstack.logstash.exception;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年9月07日 下午1:24:57
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class UrlException extends LogstashException{

	private static String message = "Url: %s format is error";

	
	public UrlException(String name) {
		// TODO Auto-generated constructor stub
		super(String.format(message, name));
	}

}
