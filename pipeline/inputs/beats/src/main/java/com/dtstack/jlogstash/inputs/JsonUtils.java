package com.dtstack.jlogstash.inputs;

import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

/**
 * 
 * @author sishu.yss
 *
 */
public class JsonUtils {
	
	public final static ObjectMapper mapper = new ObjectMapper();
	
//    public final static ObjectMapper mapper = new ObjectMapper().registerModule(new AfterburnerModule());
}
