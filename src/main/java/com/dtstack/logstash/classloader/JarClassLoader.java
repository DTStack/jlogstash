package com.dtstack.logstash.classloader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import com.dtstack.logstash.exception.LogstashException;
import com.google.common.collect.Maps;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年12月16日 下午15:26:07
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class JarClassLoader {
	
	private static String userDir = System.getProperty("user.dir");
	
	private final static String JAR_UNIQUEID="jar_uniqueid";
	
	public Map<String,ClassLoader> loadJar(String env) throws LogstashException{
		Map<String,ClassLoader> classLoads = Maps.newConcurrentMap();
		Set<Map.Entry<String,URL[]>> urls = getClassLoadJarUrls().entrySet();
		for(Map.Entry<String,URL[]> url:urls){
			String key = url.getKey();
			if("Produce".equals(env)){
				URLClassLoader  loader = new URLClassLoader(url.getValue());  
				classLoads.put(key, loader);
			}else{
				classLoads.put(key, this.getClass().getClassLoader());
			}
		}
		return classLoads;
	}
	
	private Map<String,URL[]> getClassLoadJarUrls() throws LogstashException{
		String input = String.format("%s/plugin/input", userDir);
		File finput = new File(input);
		if(!finput.exists()){
			throw new LogstashException(String.format("%s direcotry not found", input));
		}
		
		String filter = String.format("%s/plugin/filter", userDir);
		File ffilter = new File(filter);
	    if(!ffilter.exists()){
			throw new LogstashException(String.format("%s direcotry not found", filter));
		}
		
		String output = String.format("%s/plugin/output", userDir);
		File foutput = new File(output);
		if(!foutput.exists()){
				throw new LogstashException(String.format("%s direcotry not found", output));
		}
		
		  
		return null;
	}
	
	private Map<String,URL[]> getClassLoadJarUrls(File dir){
		Map<String,URL[]> jurls = Maps.newConcurrentMap();
		File[] files = dir.listFiles();
		for(File f:files){
			if(f.isFile()&&f.getName().endsWith(".jar")){
				
			}
		}
		return null;
	}
	
	private String getJarUniqueId(String jarPath) throws Exception{
		URL jarURL = new URL(jarPath);
		JarURLConnection jarCon = (JarURLConnection) jarURL.openConnection();
		JarFile jarFile = jarCon.getJarFile();
		Enumeration<JarEntry> jarEntrys = jarFile.entries();
		while (jarEntrys.hasMoreElements()) {
		JarEntry entry = jarEntrys.nextElement();
		String name = entry.getName();
		if (entry.isDirectory()&&name.endsWith("META-INF")) {
			File[] files = new File(name).listFiles();
			for(File f:files){
				if(f.isFile()&&f.getName().equals("MANIFEST.MF")){
					
				}
			 }
			}
		}
		return "";
	}
	
	public static void main(String[] args){

	}
}