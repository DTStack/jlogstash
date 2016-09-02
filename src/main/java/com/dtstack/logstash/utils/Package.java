package com.dtstack.logstash.utils;

import org.apache.commons.lang3.StringUtils;

import com.dtstack.logstash.property.SystemProperty;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:28:43
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class Package {
	
	private static String point =".";
	
	public static String getRealClassName(String name,String key){
		if(StringUtils.isBlank(name))return null;
		if(name.indexOf(point)>=0)return name;
		return SystemProperty.getSystemProperty(key)+point+name;
	}
	
	public static void main(String[] args){
		System.out.println(getRealClassName("com.dtstack.log.sysdev.outputs.File","output"));
	}

}
