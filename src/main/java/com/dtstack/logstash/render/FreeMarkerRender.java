package com.dtstack.logstash.render;

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
