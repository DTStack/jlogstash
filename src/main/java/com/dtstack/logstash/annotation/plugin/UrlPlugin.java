package com.dtstack.logstash.annotation.plugin;

import java.lang.reflect.Field;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.dtstack.logstash.exception.UrlException;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年9月07日 下午1:24:57
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class UrlPlugin implements AnnotationInterface{
	
	private static Pattern pattern = Pattern.compile("^(http|https)://[a-zA-Z0-9._-]+(.com|.cn|.net|.org).*"); 

	@Override
	public void required(Field field, Object obj) throws Exception {
		// TODO Auto-generated method stub
		String url = (String)obj;
		Matcher matcher = pattern.matcher(url);
		if(!matcher.find()){
			throw new UrlException(field.getName());
		}
	}
}
