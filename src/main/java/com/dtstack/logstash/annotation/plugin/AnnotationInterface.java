package com.dtstack.logstash.annotation.plugin;

import java.lang.reflect.Field;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:24:50
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public interface AnnotationInterface {
	
	 public void required(Field field,Object obj) throws Exception;

}
