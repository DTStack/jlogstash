package com.dtstack.logstash.date;

import org.joda.time.DateTime;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:28:23
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public interface DateParser {
	public DateTime parse(String input);
}
