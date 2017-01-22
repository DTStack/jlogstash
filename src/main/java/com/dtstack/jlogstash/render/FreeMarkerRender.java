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
package com.dtstack.jlogstash.render;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import freemarker.template.Configuration;
import freemarker.template.Template;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:27:56
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class FreeMarkerRender extends TemplateRender {
	private static final Logger logger = LoggerFactory.getLogger(FreeMarkerRender.class);

	private Template t;

	public FreeMarkerRender(String template, String templateName)
			throws IOException {
		Configuration cfg = new Configuration(Configuration.VERSION_2_3_22);
		this.t = new Template(templateName, template, cfg);
	}

	public FreeMarkerRender(String template) throws IOException {
		Configuration cfg = new Configuration(Configuration.VERSION_2_3_22);
		this.t = new Template("", template, cfg);
	}

	public String render(Map event) {
		StringWriter sw = new StringWriter();
		try {
			t.process(event, sw);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return "";
		}
		try {
			sw.close();
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		return sw.toString();
	}

	@Override
	public String render(String template, Map event) {
		// actually it is just used to be compatible with jinjava
		return this.render(event);
	}
}
