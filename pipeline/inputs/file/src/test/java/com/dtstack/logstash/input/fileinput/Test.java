package com.dtstack.logstash.input.fileinput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dtstack.jlogstash.inputs.File;
import com.dtstack.jlogstash.inputs.ReadLineUtil;

public class Test {
	
	public void test(){
		Map<String, String> config = new HashMap<String, String>();
		config.put("codec", "plain");
		List<String> path = new ArrayList<String>();
		path.add("E:\\controller.log");
		path.add("E:\\server.log");
		
		File file = new File(config);
//		File.path = path;
		file.prepare();
		file.emit();
	}
	
	public void testReadLine() throws Exception{
		java.io.File file = new java.io.File("D:\\testReadLine.txt");
		ReadLineUtil readLineUtil = new ReadLineUtil(file, "UTF-8", 0);
		String line = null;
		while((line = readLineUtil.readLine()) != null){
			System.out.println(line);
		}
		
		System.out.println(readLineUtil.getCurrBufPos());
		
	}
	
	public static void main(String[] args) throws IOException {
//		Test test = new Test();
//		test.testReadLine();
		System.out.println(Long.MAX_VALUE/1024/1024/1024/1024/1024);
	}

}
