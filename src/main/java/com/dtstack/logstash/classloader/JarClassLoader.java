package com.dtstack.logstash.classloader;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
	
	private static Logger logger = LoggerFactory.getLogger(JarClassLoader.class);

	private static String userDir = System.getProperty("user.dir");
	
//	private final static String JAR_UNIQUEID="jar_uniqueid";
	
	/**
	 * 每一个plugin一个classloader
	 * @param env pro or dev
	 * @return
	 * @throws LogstashException
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	public Map<String,ClassLoader> loadJar(String env) throws LogstashException, MalformedURLException, IOException{
		Map<String,ClassLoader> classLoads = Maps.newConcurrentMap();
		Set<Map.Entry<String,URL[]>> urls = getClassLoadJarUrls().entrySet();
		ClassLoader classLoader = this.getClass().getClassLoader();
		for(Map.Entry<String,URL[]> url:urls){
			String key = url.getKey();
			if("dev".equals(env)){
				classLoads.put(key,classLoader);
			}else{
				URLClassLoader  loader = new URLClassLoader(url.getValue(),classLoader);  
				classLoads.put(key, loader);
			}
		}
		return classLoads;
	}
	
	private Map<String,URL[]> getClassLoadJarUrls() throws LogstashException, MalformedURLException, IOException{
		logger.warn("userDir:{}",userDir);
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
		logger.warn("load input plugin class...");
		Map<String,URL[]>  inputs = getClassLoadJarUrls(finput);
		logger.warn("load filter plugin class...");
		Map<String,URL[]>  filters = getClassLoadJarUrls(ffilter);
		logger.warn("load output plugin class...");
		Map<String,URL[]>  outputs = getClassLoadJarUrls(foutput);
		inputs.putAll(filters);
		inputs.putAll(outputs);
		logger.warn("getClassLoadJarUrls:{}",inputs);
		return inputs;
	}
	
	private Map<String,URL[]> getClassLoadJarUrls(File dir) throws MalformedURLException, IOException{
		String dirName = dir.getName();
		Map<String,URL[]> jurls = Maps.newConcurrentMap();
		File[] files = dir.listFiles();
	    if (files!=null&&files.length>0){
			for(File f:files){
				String jarName = f.getName();
				if(f.isFile()&&jarName.endsWith(".jar")){
					jurls.put(String.format("%s:%s",dirName,jarName.split("-")[0].toLowerCase()), new URL[]{f.toURI().toURL()});
				}
			}
	    }
		return jurls;
	}
	
//	private String getJarUniqueId(String jarPath) throws Exception{
//		URL jarURL = new URL(jarPath);
//		JarURLConnection jarCon = (JarURLConnection) jarURL.openConnection();
//		JarFile jarFile = jarCon.getJarFile();
//		Enumeration<JarEntry> jarEntrys = jarFile.entries();
//		while (jarEntrys.hasMoreElements()) {
//		JarEntry entry = jarEntrys.nextElement();
//		String name = entry.getName();
//		if (entry.isDirectory()&&name.endsWith("META-INF")) {
//			File[] files = new File(name).listFiles();
//			for(File f:files){
//				if(f.isFile()&&f.getName().equals("MANIFEST.MF")){
//					
//				}
//			 }
//			}
//		}
//		return "";
//	}
}