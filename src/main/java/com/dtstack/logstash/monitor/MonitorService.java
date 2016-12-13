package com.dtstack.logstash.monitor;
//
//import java.lang.management.ManagementFactory;
//import java.lang.management.OperatingSystemMXBean;

import com.dtstack.logstash.utils.Public;

public class MonitorService {
	
    private final static double MB = 1024 * 1024 * 1.0;
    
    private final static double GB = 1024 * 1024 * 1024 * 1.0;
	
    public MonitorInfo getMonitorInfoBean() {
        // jvm
        double totalMemory = Runtime.getRuntime().totalMemory() / MB;
        double freeMemory = Runtime.getRuntime().freeMemory() / MB;
        double maxMemory = Runtime.getRuntime().maxMemory() / MB;	
        // MonitorInfo
        MonitorInfo infoBean = new MonitorInfo();
        infoBean.setJvmFreeMemory(Public.getIntValue(totalMemory));
        infoBean.setJvmFreeMemory(Public.getIntValue(freeMemory));
        infoBean.setJvmMaxMemory(Public.getIntValue(maxMemory));
        infoBean.setProcessors(Runtime.getRuntime().availableProcessors());
        return infoBean;
    }
}
