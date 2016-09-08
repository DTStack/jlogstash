package com.dtstack.logstash.annotation.plugin;

import java.lang.reflect.Field;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.dtstack.logstash.exception.PathException;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:24:57
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class PathPlugin implements AnnotationInterface{

	private static Pattern pattern = Pattern.compile("^\\.|^/|^[a-zA-Z]\\)?:?/.+(/$)?"); 

	@Override
	public void required(Field field, Object obj) throws Exception{
		// TODO Auto-generated method stub
		String path = (String)obj;
		Matcher matcher = pattern.matcher(path);
		if(!matcher.find()){
			throw new PathException(field.getName());
		}	
	}
}
