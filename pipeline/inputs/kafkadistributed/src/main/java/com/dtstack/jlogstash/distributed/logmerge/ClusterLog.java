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
package com.dtstack.jlogstash.distributed.logmerge;

import com.dtstack.jlogstash.distributed.util.RouteUtil;
import com.google.common.collect.Maps;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * 日志信息,包含日志发送时间
 * Date: 2016/12/28
 * Company: www.dtstack.com
 *
 * @ahthor xuchao
 */

public class ClusterLog {

    private static final Logger logger = LoggerFactory.getLogger(ClusterLog.class);

    private static ObjectMapper objectMapper = new ObjectMapper();

    private long offset;

    private String host;

    private String path;

    private String loginfo;

    private Map<String,Object> originalLog;

    private String logType;

    private String flag;

    private Long geneTime;

    public String getLogFlag(){
       return this.flag;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getLoginfo() {
        return loginfo;
    }

    public void setLoginfo(String loginfo) {
        this.loginfo = loginfo;
    }

    @Override
    public String toString() {
        return loginfo;
    }

    public Map<String,Object> getOriginalLog() {
        return originalLog;
    }

    public void setOriginalLog(Map<String,Object> originalLog) {
        this.originalLog = originalLog;
    }

    public String getLogType() {
        return logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    public Long getGeneTime() {
        return geneTime;
    }

    /**
     * 获取除了message字段以外的信息
     * @return
     */
    public Map<String, Object> getBaseInfo(){
        Map<String,Object> eventMap  = Maps.newHashMap();
        eventMap.putAll(this.originalLog);
        eventMap.remove("message");
        return eventMap;
    }


    public static ClusterLog generateClusterLog(String log) throws IOException {

        Map<String, Object> eventMap  = objectMapper.readValue(log,Map.class);
        ClusterLog clusterLog  = new ClusterLog();
        String msg = (String)eventMap.get("message");
        String host = (String)eventMap.get("host");
        String path = (String)eventMap.get("path");
        String logType = (String)eventMap.get("logtype");
        //offset 必须有 需要根据这个来做排序
        clusterLog.setOffset(Long.valueOf(eventMap.get("offset").toString()));
        clusterLog.setLoginfo(msg);
        clusterLog.host = host;
        clusterLog.geneTime = System.currentTimeMillis();
        clusterLog.path = path;
        clusterLog.logType = logType;
        clusterLog.flag = RouteUtil.getFormatHashKey(eventMap);
        eventMap.put("logflag",clusterLog.flag);
        clusterLog.originalLog = eventMap;
        return clusterLog;
    }

}
