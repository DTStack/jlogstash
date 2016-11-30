package com.dtstack.logstash.property;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:27:36
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class SystemProperty {
	
	static{
		System.setProperty("input", "com.dtstack.logstash.inputs");
		System.setProperty("filter", "com.dtstack.logstash.filters");
		System.setProperty("output", "com.dtstack.logstash.outputs");
		System.setProperty("annotationPlugin", "com.dtstack.logstash.annotation.plugin");
		System.setProperty("annotationPackage","com.dtstack.logstash.annotation");
		System.setProperty("proportion",Double.toString(500/1024));
	}
	
	public static String getSystemProperty(String key){
		return System.getProperty(key);
	}
}
