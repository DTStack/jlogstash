package com.dtstack.logstash.date;

import org.joda.time.DateTime;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:28:47
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class UnixMSParser implements DateParser {

	@Override
	public DateTime parse(String input) {
		// TODO Auto-generated method stub
		return new DateTime(Long.parseLong(input));
	}
}
