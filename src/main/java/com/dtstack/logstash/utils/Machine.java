package com.dtstack.logstash.utils;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:28:38
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class Machine {	
  public static int availableProcessors(){
	  return Runtime.getRuntime().availableProcessors();
  }
}
