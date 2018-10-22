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

import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.render.FreeMarkerRender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:54:43
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class Replace extends BaseFilter {
	private static final Logger logger = LoggerFactory.getLogger(Replace.class);

	public Replace(Map config) {
		super(config);
	}
	
	@Required(required=true)
	private static String src;
	
	@Required(required=true)
	private static String value;

	private FreeMarkerRender render;

	public void prepare() {
		try {
			this.render = new FreeMarkerRender(value, value);
		} catch (IOException e) {
			logger.error(e.getMessage());
			System.exit(1);
		}
	}

	@Override
	protected Map filter(final Map event) {
		if (event.containsKey(src)) {
			event.put(src, render.render(value, new HashMap() {
				{
					put("event", event);
				}
			}));
		}
		return event;
	}
}
