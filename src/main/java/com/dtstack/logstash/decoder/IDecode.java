package com.dtstack.logstash.decoder;

import java.util.Map;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:25:51
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public interface IDecode {
	
	public Map<String, Object> decode(String message);
	
	public Map<String, Object> decode(String message, String identify);

}
