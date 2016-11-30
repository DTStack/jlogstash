package com.dtstack.logstash.inputs;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:27:03
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.logstash.assembly.qlist.InputQueueList;
import com.dtstack.logstash.decoder.IDecode;
import com.dtstack.logstash.decoder.JsonDecoder;
import com.dtstack.logstash.decoder.MultilineDecoder;
import com.dtstack.logstash.decoder.PlainDecoder;
import com.dtstack.logstash.utils.Public;

@SuppressWarnings("serial")
public abstract class BaseInput implements Cloneable, java.io.Serializable{
		
	private static final Logger baseLogger = LoggerFactory.getLogger(BaseInput.class);
	
    protected Map<String, Object> config;
    
    protected IDecode decoder;
    
    private static InputQueueList inputQueueList;
    
    protected Map<String, Object> addFields=null;
    

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
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
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
    

    @SuppressWarnings({ "rawtypes", "unchecked" })
	public BaseInput(Map config){
        this.config = config;
        decoder = createDecoder();
        if(this.config!=null){
        	addFields = (Map<String, Object>) this.config.get("addFields");
        }
    }

    public abstract void prepare();

    public abstract void emit();

    public void process(Map<String,Object> event) {
    	if(addFields!=null){
    		addFields(event);
    	}
    	inputQueueList.put(event);
    }
    
    public abstract void release();
    
    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
    
	private void addFields(Map<String,Object> event){
		Set<Map.Entry<String,Object>> sets =addFields.entrySet();
		for(Map.Entry<String,Object> entry:sets){
			String key = entry.getKey();
			if(event.get(key)==null){
				Object value = entry.getValue();
				event.put(key, value);
				if(event.get(value)!=null){
					event.put(key, event.get(value));
				}else if(value instanceof String){
					String vv =value.toString();
					if(vv.indexOf(".")>0){
						String[] vs=vv.split("\\.");
						Object oo = event;
						for(int i=0;i<vs.length;i++){
							oo = loopObject(vs[i],oo);
							if(oo==null)break;	
						}
						if(oo!=null)event.put(key, oo);	
					}else if ("%{hostname}%".equals(vv)){
	        			event.put(key, Public.getHostName());
	        		}else if("%{timestamp}%".equals(vv)){
	        			event.put(key,Public.getTimeStamp());
	        		}else if("%{ip}%".equals(vv)){
	        			event.put(key, Public.getHostAddress());
	        		}
	            }
			} 
		}
    }
  
	@SuppressWarnings("unchecked")
	private Object loopObject(String value,Object obj){
		if(obj instanceof Map){
			return ((Map<String,Object>)obj).get(value);
		} 
        return null;
	}

	public static void setInputQueueList(InputQueueList inputQueueList) {
		BaseInput.inputQueueList = inputQueueList;
	}
    
}
