package com.dtstack.logstash.annotation.plugin;

import java.lang.reflect.Field;
import com.dtstack.logstash.annotation.Required;
import com.dtstack.logstash.exception.RequiredException;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:25:03
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class RequiredPlugin implements AnnotationInterface{

	@Override
	public void required(Field field,Object obj) throws Exception{
		// TODO Auto-generated method stub
		Required required = field.getAnnotation(Required.class);
		if(required.required()&&obj==null){
			throw new RequiredException(field.getName());
		}	
	}
}
