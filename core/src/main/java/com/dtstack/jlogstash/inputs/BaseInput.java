/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.jlogstash.inputs;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:27:03
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
import java.util.Map;

import com.dtstack.jlogstash.metrics.MetricRegistryImpl;
import com.dtstack.jlogstash.metrics.groups.PipelineIOMetricGroup;
import com.dtstack.jlogstash.utils.LocalIpAddressUtil;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.jlogstash.assembly.qlist.QueueList;
import com.dtstack.jlogstash.decoder.IDecode;
import com.dtstack.jlogstash.decoder.JsonDecoder;
import com.dtstack.jlogstash.decoder.MultilineDecoder;
import com.dtstack.jlogstash.decoder.PlainDecoder;
import com.dtstack.jlogstash.utils.BasePluginUtil;

@SuppressWarnings("serial")
public abstract class BaseInput implements Cloneable, java.io.Serializable{
		
	private static final Logger baseLogger = LoggerFactory.getLogger(BaseInput.class);
	
    protected Map<String, Object> config;
    
    private IDecode decoder;
    
    private static QueueList inputQueueList;
    
    protected Map<String, Object> addFields=null;
    
    protected static BasePluginUtil basePluginUtil = new BasePluginUtil();

    private static PipelineIOMetricGroup pipelineIOMetricGroup;

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
    
    public IDecode getDecoder() {
		return decoder;
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
    	if(event!=null&&event.size()>0){
        	if(addFields!=null){
        		basePluginUtil.addFields(event,addFields);
        	}
			pipelineIOMetricGroup.getNumRecordsInCounter().inc();
			pipelineIOMetricGroup.getNumBytesInLocalRateMeter().markEvent(ObjectSizeCalculator.getObjectSize(event));
        	inputQueueList.put(event);
    	}
    }
    
    public abstract void release();
    
    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
   
	public static void setInputQueueList(QueueList inputQueueList) {
		BaseInput.inputQueueList = inputQueueList;
	}

	public static QueueList getInputQueueList() {
		return inputQueueList;
	}

	public static void setMetricRegistry(MetricRegistryImpl metricRegistry, String name) {
		String hostname = LocalIpAddressUtil.getLocalAddress();
		String pluginName = BaseInput.class.getSimpleName();
		BaseInput.pipelineIOMetricGroup = new PipelineIOMetricGroup(metricRegistry, hostname, "input", pluginName, name);
	}
}
