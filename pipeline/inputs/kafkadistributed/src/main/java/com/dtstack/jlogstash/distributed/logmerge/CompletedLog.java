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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * 一条完整的日志记录
 * Date: 2016/12/30
 * Company: www.dtstack.com
 *
 * @ahthor xuchao
 */

public class CompletedLog {

    private String lineSP = System.getProperty("line.separator");

    private Map<String, Object> eventMap = Maps.newHashMap();

    private List<String> logInfo = Lists.newArrayList();

    public List<String> getLogInfo() {
        return logInfo;
    }

    public void setLogInfo(List<String> logInfo) {
        this.logInfo = logInfo;
    }

    public void addLog(String log){
        logInfo.add(log);
    }

    public Map<String, Object> getEventMap() {
        return eventMap;
    }

    public void setEventMap(Map<String, Object> eventMap) {
        this.eventMap = eventMap;
    }

    public void complete(Map<String, Object> extInfo){
        String msg = StringUtils.join(logInfo, lineSP);
        eventMap.put("message", msg);

        if(extInfo != null){
            eventMap.putAll(extInfo);
        }
    }

    @Override
    public String toString() {
        return logInfo.toString();
    }
}
