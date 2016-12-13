package com.dtstack.logstash.decoder;

import java.util.HashMap;
import java.util.Map;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:26:00
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class PlainDecoder implements IDecode {

	@SuppressWarnings("serial")
	@Override
	public Map<String, Object> decode(final String message) {
		HashMap<String, Object> event = new HashMap<String, Object>() {
			{
				put("message", message);
			}
		};
		return event;
	}

	@Override
	public Map<String, Object> decode(String message, String identify) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
