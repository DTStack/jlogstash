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
package com.dtstack.jlogstash.filters;

import java.util.List;
import java.util.Map;
import java.net.URLDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.jlogstash.annotation.Required;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:55:33
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class URLDecode extends BaseFilter {
	private static final Logger logger = LoggerFactory.getLogger(URLDecode.class);

	@SuppressWarnings("rawtypes")
	public URLDecode(Map config) {
		super(config);
	}
    
	@Required(required=true)
	private static List<String> fields;
	
	private static String enc="UTF-8";
	
	private String tagOnFailure="URLDecodefail";
	

	@SuppressWarnings("unchecked")
	public void prepare() {
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected Map filter(final Map event) {
		boolean success = true;
		for (String f : fields) {
			if (event.containsKey(f)) {
				try {
					event.put(f,
							URLDecoder.decode((String) event.get(f), this.enc));
				} catch (Exception e) {
					logger.error("URLDecode failed", e);
					success = false;
				}
			}
		}
		return event;
	}
}
