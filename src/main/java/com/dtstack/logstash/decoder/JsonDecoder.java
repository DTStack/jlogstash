package com.dtstack.logstash.decoder;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:25:56
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class JsonDecoder implements IDecode {
    private static Logger logger = LoggerFactory.getLogger(JsonDecoder.class);

	private static ObjectMapper objectMapper = new ObjectMapper();

    @SuppressWarnings({ "unchecked", "serial" })
    @Override
    public Map<String, Object> decode(final String message) {
        Map<String, Object> event = null;
        try {
            event = objectMapper.readValue(message, Map.class);
            if(!event.containsKey("message")){
            	event.put("message", message);
            } 
        } catch (Exception e) {
            logger.error(e.getMessage());
            event = new HashMap<String, Object>() {
                {
                    put("message", message);
                }
            };
            return event;
        }
        return event;
    }

	@Override
	public Map<String, Object> decode(String message, String identify) {
		return null;
	}
}
