package com.dtstack.logstash.decoder;

import java.util.HashMap;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:26:00
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class PlainDecoder implements IDecode {

	@Override
	public Map<String, Object> decode(final String message) {
		HashMap<String, Object> event = new HashMap<String, Object>() {
			{
				put("message", message);
				put("@timestamp", DateTime.now(DateTimeZone.UTC).toString());//日志生成的时间
				put("timestamp", DateTime.now(DateTimeZone.UTC).toString());//日志接收的时间
			}
		};
		return event;
	}
}
