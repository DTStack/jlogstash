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
package com.dtstack.jlogstash.monitor;
//
//import java.lang.management.ManagementFactory;
//import java.lang.management.OperatingSystemMXBean;

import com.dtstack.jlogstash.utils.Public;

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
