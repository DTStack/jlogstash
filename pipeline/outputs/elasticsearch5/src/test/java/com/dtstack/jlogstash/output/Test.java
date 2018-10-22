package com.dtstack.jlogstash.output;

import java.io.IOException;
import java.util.Map;

import com.dtstack.jlogstash.outputs.Elasticsearch5;
import com.dtstack.jlogstash.render.Formatter;
import com.dtstack.jlogstash.render.FreeMarkerRender;
import com.dtstack.jlogstash.render.TemplateRender;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class Test {
	
	public static void main(String[] args) throws Exception{
//		Map<String,Object> event = Maps.newHashMap();
//		event.put("id",12);
//		event.put("name", "yxp");
//		Elasticsearch5.cluster="daily_dtstack";
//		Elasticsearch5.documentId="${id}";
//		Elasticsearch5.hosts = Lists.newArrayList("121.43.170.183:9300");
//		Elasticsearch5.index="dt_ysq_log";
//		Elasticsearch5 elasticsearch5 = new Elasticsearch5(Maps.newHashMap());
//		elasticsearch5.prepare();
//		elasticsearch5.emit(event);
//		Thread.sleep(30000);
		
		test();
	}
	
	public static void test() throws IOException{
		
		Map<String,Object> event = Maps.newConcurrentMap();
		event.put("ysq", 123);
//		
//		TemplateRender idRender = new FreeMarkerRender("${ysq}");
//
//		System.out.println(idRender.render(event));
		
        String _index = Formatter.format(event, "logs");
        System.out.println(_index);
        System.out.println(Formatter.isFormat(_index));
	}
}
