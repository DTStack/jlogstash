package com.dtstack.jlogstash.inputs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.code.regexp.Pattern;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 
 * @author sishu.yss
 *
 */
public class WhiteIpTask implements Runnable{
	
	private static Logger logger = LoggerFactory.getLogger(WhiteIpTask.class);
	
	private String whiteIpPath;
	
	private Set<String> whiteList;
	
    private Map<String,Pattern> whiteListPatterns = Maps.newConcurrentMap();
	
	private static String interval = ",";
	
	private long lastModify = 0;
	
	public WhiteIpTask(String whiteIpPath,Set<String> whiteList){
		this.whiteIpPath = whiteIpPath;
		this.whiteList = whiteList;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		while(true){
			try {
				  String ips = readFileByLines(whiteIpPath);
				  if(StringUtils.isNotBlank(ips)){
					  List<String> lss = Arrays.asList(ips.split(interval));
					  if(whiteList.size()==0){
						  whiteList.addAll(lss);
						  for(String ip:lss){
							  whiteListPatterns.put(ip, Pattern.compile(ip));
						  }
					  }else{
						  for(String ip:whiteList){
							  if(!lss.contains(ip)){
								  whiteList.remove(ip);
								  whiteListPatterns.remove(ip);
							  }
						  }
						  whiteList.addAll(lss);
						  for(String ip:whiteList){
							  whiteListPatterns.put(ip, Pattern.compile(ip));
						  }
					  }
				  }
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				logger.error("",e);
			}
		}
	}
	
	
    public String readFileByLines(String fileName) {
        File file = new File(fileName);
        StringBuffer sb = new StringBuffer();
        long lastNowModify = file.lastModified();
        if(lastModify != lastNowModify){
        	lastModify  = lastNowModify;
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(file));
                String tempString = null;
                // 一次读入一行，直到读入null为文件结束
                while ((tempString = reader.readLine()) != null) {
                    // 显示行号
                	sb.append(tempString).append(interval);
                }
                reader.close();
            } catch (IOException e) {
            	logger.error("",e);
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e1) {
                    	
                    }
                }
            }
        }
        return sb.toString();
    }
    
    public boolean isWhiteIp(String ip){
    	return isWhiteIp(Lists.newArrayList(ip));
    } 
    
    public  boolean isWhiteIp(Map<String,Object> data){
    	Object sourceIp = data.get("local_ip");
    	List<String> ips = Lists.newArrayList();
    	if(sourceIp instanceof String){
    		ips.addAll(Arrays.asList(((String) sourceIp).split(interval)));
    	}else if(sourceIp instanceof List){
    		ips.addAll((List<String>)sourceIp);
    	}
    	return isWhiteIp(ips);
    }
    
    public boolean isWhiteIp(List<String> ips){
	    for(String ip:ips){
	    	Set<Map.Entry<String, Pattern>> patterns =whiteListPatterns.entrySet();
	    	for(Map.Entry<String, Pattern> pattern:patterns){
	    		if(pattern.getValue().matcher(ip).find()){
	    			return true;
	    		}
	    	}
	    }
	    return false;
    }
    
    public static void main(String[] args){
    	Pattern pattern = Pattern.compile("127.0.*.*");
    	System.out.println(pattern.matcher("127.0.0.1").find());
    }
}
