package com.dtstack.jlogstash.format;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * 
 * @author sishu.yss
 *
 */
public interface OutputFormat extends Serializable {
	

	void configure();
	

	void open() throws IOException;
	
	
	void writeRecord(Map<String,Object> row) throws IOException;
	

	void close() throws IOException;
}

