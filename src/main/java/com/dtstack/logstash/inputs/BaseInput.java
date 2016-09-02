package com.dtstack.logstash.inputs;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.logstash.assembly.InputQueueList;
import com.dtstack.logstash.decoder.IDecode;
import com.dtstack.logstash.decoder.JsonDecoder;
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

	private static final Logger logger = LoggerFactory.getLogger(BaseInput.class);

    protected Map<String, Object> config;
    protected IDecode decoder;
    protected InputQueueList inputQueueList;

    public IDecode createDecoder() {
        String codec = (String) this.config.get("codec");
        if (codec != null && codec.equalsIgnoreCase("plain")) {
            return new PlainDecoder();
        } else {
            return new JsonDecoder();
        }
    }

    public BaseInput(Map config,InputQueueList inputQueueList){
        this.config = config;
        this.inputQueueList = inputQueueList;
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
