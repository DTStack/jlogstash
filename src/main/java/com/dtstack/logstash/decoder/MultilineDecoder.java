package com.dtstack.logstash.decoder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.logstash.annotation.Required;
import com.dtstack.logstash.decoder.IDecode;
import com.google.common.collect.Lists;

public class MultilineDecoder implements IDecode {
	
	private static final Logger logger = LoggerFactory.getLogger(MultilineDecoder.class);
	
	private String pattern;
	
	private String what; //one of (previous, next)
	
	private boolean negate = false;
	
	private Pattern patternReg; //FIXME 是否需要支持grok
	
	private List<String> buffer = Lists.newArrayList();
	
	/**最大累加行数**/
	private int maxLines = 50;

	private String customLineDelimiter = "$|$";
	
	private String multilineTag = "multiline";
	
	public MultilineDecoder(String pattern, String what){
		
		init(pattern, what);
	}
	
	public MultilineDecoder(String pattern, String what, boolean negate){
		
		this.negate = negate;
		this.init(pattern, what);
	}
		
	public void init(String pattern, String what){
		
		if(pattern == null || what == null){
			logger.error("pattern and what must not be null.");
			System.exit(-1);
		}
		
		if(!"previous".equals(what) && !"next".equals(what)){
			logger.error("parameter what must in [previous, next]");
			System.out.println(-1);
		}
		
		logger.warn("MultilineDecoder param pattern:{}, what:{}, negate:{}.", new Object[]{pattern, what, negate});
		
		this.pattern = pattern;
		this.what = what;
		
		patternReg = Pattern.compile(this.pattern);
	}

	@Override
	public Map<String, Object> decode(String message) {
		
		Matcher matcher = patternReg.matcher(message);
		boolean isMathcer = matcher.find();
		boolean hasMatcher = (isMathcer && !negate) || (!isMathcer && negate);
		Map<String, Object> rst;
		
		if("next".equals(what)){
			rst = doNext(message, hasMatcher);
		}else{
			rst = doPrevious(message, hasMatcher);
		}
		
		if(buffer.size() >= maxLines){
			rst =  flush();
		}
		
		return rst;
		
	}
	
	private void buffer(String msg){
		buffer.add(msg);
	}
	
	private Map<String, Object> flush(){
		
		if(buffer.size() == 0){
			return null;
		}
		
		String msg = StringUtils.join(buffer, customLineDelimiter);
		Map<String, Object> event = new HashMap<String, Object>();
		if(buffer.size() > 1){
			event.put(multilineTag, "true");
		}
		
		event.put("message", msg);
		event.put("@timestamp", DateTime.now(DateTimeZone.UTC).toString());
		buffer.clear();
		return event;
	}
	
	private Map<String, Object> doNext(String msg, boolean matched){
		
		Map<String, Object> event = null;
		buffer(msg);
		if(!matched){
			event = flush();
		}
		
		return event;
	}
	
	private Map<String, Object> doPrevious(String msg, boolean matched){
		
		Map<String, Object> event = null;
		if(!matched){
			event = flush();
		}
		
		buffer(msg);
		
		return event;
	}

}
