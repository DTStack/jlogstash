package com.dtstack.logstash.decoder;

import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
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

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object> decode(final String message) {
        Map<String, Object> event = null;
        try {
            event = objectMapper.readValue(message, Map.class);
            if(!event.containsKey("@timestamp")){//日志生成的时间
            	event.put("@timestamp", DateTime.now(DateTimeZone.UTC).toString());
            }
            if(!event.containsKey("timestamp")){//日志接收的时间
            	event.put("timestamp", DateTime.now(DateTimeZone.UTC).toString());
            }
            if(!event.containsKey("message")){
            	event.put("message", message);
            } 
        } catch (Exception e) {
            logger.error(e.getMessage());
            event = new HashMap<String, Object>() {
                {
                    put("message", message);
                    put("@timestamp", DateTime.now(DateTimeZone.UTC).toString());
                    put("timestamp", DateTime.now(DateTimeZone.UTC).toString());
                }
            };
            return event;
        }
        return event;
    }
}
