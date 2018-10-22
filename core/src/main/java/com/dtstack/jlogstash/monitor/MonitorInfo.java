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

/**
 * 
 * Reason: TODO ADD REASON(可选) Date: 2016年11月29日 下午15:30:18 Company:
 * www.dtstack.com
 * 
 * @author sishu.yss
 *
 */
public class MonitorInfo {
	
    /** jvm可使用内存. */
    private long jvmTotalMemory;
    
    /** jvm剩余内存. */
    private long jvmFreeMemory;
    
    /** jvm最大可使用内存. */
    private long jvmMaxMemory;
    
    /** 操作系统. */
    private String osName;
    
    /** 总的物理内存. */
    private long osTotalMemorySize;
    
    /** 剩余的物理内存. */
    private long osFreeMemorySize;
    
    /** 已使用的物理内存. */
    private long osUsedMemorySize;
    
    /** 核心数. */
    private int processors;


    public long getJvmTotalMemory() {
		return jvmTotalMemory;
	}

	public void setJvmTotalMemory(long jvmTotalMemory) {
		this.jvmTotalMemory = jvmTotalMemory;
	}

	public long getJvmFreeMemory() {
		return jvmFreeMemory;
	}

	public void setJvmFreeMemory(long jvmFreeMemory) {
		this.jvmFreeMemory = jvmFreeMemory;
	}

	public long getJvmMaxMemory() {
		return jvmMaxMemory;
	}

	public void setJvmMaxMemory(long jvmMaxMemory) {
		this.jvmMaxMemory = jvmMaxMemory;
	}

	public long getOsTotalMemorySize() {
		return osTotalMemorySize;
	}

	public void setOsTotalMemorySize(long osTotalMemorySize) {
		this.osTotalMemorySize = osTotalMemorySize;
	}

	public long getOsFreeMemorySize() {
		return osFreeMemorySize;
	}

	public void setOsFreeMemorySize(long osFreeMemorySize) {
		this.osFreeMemorySize = osFreeMemorySize;
	}

	public long getOsUsedMemorySize() {
		return osUsedMemorySize;
	}

	public void setOsUsedMemorySize(long osUsedMemorySize) {
		this.osUsedMemorySize = osUsedMemorySize;
	}

	public String getOsName() {
        return osName;
    }

    public void setOsName(String osName) {
        this.osName = osName;
    }

    public int getProcessors() {
        return processors;
    }

    public void setProcessors(int processors) {
        this.processors = processors;
    }

}
