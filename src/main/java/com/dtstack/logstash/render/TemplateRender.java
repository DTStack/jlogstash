package com.dtstack.logstash.render;

import java.util.Map;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:28:03
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public abstract class TemplateRender {

	public abstract String render(Map event);

	public abstract String render(String template, Map event);
}
