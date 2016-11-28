package com.dtstack.logstash.inputs;

import java.util.Map;

import org.apache.commons.lang3.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.logstash.assembly.InputQueueList;
import com.dtstack.logstash.decoder.IDecode;
import com.dtstack.logstash.decoder.JsonDecoder;
import com.dtstack.logstash.decoder.MultilineDecoder;
import com.dtstack.logstash.decoder.PlainDecoder;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:27:03
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public abstract class BaseInput implements Cloneable, java.io.Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = -7059206319577707365L;
	
	private static final Logger baseLogger = LoggerFactory.getLogger(BaseInput.class);

    protected Map<String, Object> config;
    protected IDecode decoder;
    protected InputQueueList inputQueueList;

    public IDecode createDecoder() {
        String codec = (String) this.config.get("codec");
        if ("json".equals(codec)) {
             return new JsonDecoder();
        } if("multiline".equals(codec)){
        	return createMultiLineDecoder(config);
        } else {
        	 return new PlainDecoder();
        }
    }
    
    public IDecode createMultiLineDecoder(Map config){
    	
    	if( config.get("multiline") == null){
    		baseLogger.error("multiline decoder need to set multiline param.");
    		System.exit(-1);
    	}
    	
    	Map<String, Object> codecConfig = (Map<String, Object>) config.get("multiline");
    	
    	if( codecConfig.get("pattern") == null || codecConfig.get("what") == null){
    		baseLogger.error("multiline decoder need to set param (pattern and what)");
    		System.exit(-1);
    	}
    	
    	String patternStr = (String) codecConfig.get("pattern");
    	String what = (String) codecConfig.get("what");
    	boolean negate = false;
    	
    	if(codecConfig.get("negate") != null){
    		negate = (boolean) codecConfig.get("negate");
    	}
    	
    	return new MultilineDecoder(patternStr, what, negate, inputQueueList);
    }
    

    public BaseInput(Map config,InputQueueList inputQueueList){
        this.config = config;
        this.inputQueueList = inputQueueList;
        decoder = createDecoder();
    }

    public abstract void prepare();

    public abstract void emit();

    public void process(String message) {}
    
    public abstract void release();
    

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

}
