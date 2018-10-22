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

import com.dtstack.jlogstash.jdistributed.gclog.CMSPreLogInfo;
import com.dtstack.jlogstash.exception.ExceptionUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * FIXME 考虑超时未使用的内存的清理
 * Date: 2016/12/30
 * Company: www.dtstack.com
 * @ahthor xuchao
 */

public class LogPool {

    private static Logger logger = LoggerFactory.getLogger(LogPool.class);

    private Map<String, IPreLog> logInfoMap = Maps.newHashMap();

    private LogDeleWatcher logDeleWatcher;

    private static LogPool singleton = new LogPool();

    public static LogPool getInstance(){
        return singleton;
    }

    private LogPool(){
        init();
    }

    public void init(){
        logDeleWatcher = new LogDeleWatcher(this);
        logDeleWatcher.startup();
    }

    public void addLog(String log){
        try{
            ClusterLog clusterLog = ClusterLog.generateClusterLog(log);
            if(log == null){
                logger.info("analyse msg from log err:{}.", log);
                return;
            }
            String flag = clusterLog.getLogFlag();
            IPreLog preLogInfo = logInfoMap.get(flag);
            if(preLogInfo == null){
                preLogInfo = createPreLogInfoByLogType(clusterLog);
                if (preLogInfo == null) return;
                logInfoMap.put(flag, preLogInfo);
            }
            preLogInfo.addLog(clusterLog);
        }catch(Exception e){
            logger.error(ExceptionUtil.getErrorMessage(e));
        }
    }

    public List<CompletedLog> mergeLog(){
        List<CompletedLog> rstLog = Lists.newArrayList();
        for(IPreLog preLog : logInfoMap.values()){
            rstLog.addAll(preLog.mergeGcLog());
        }

        return rstLog;
    }

    public void deleteLog(){
        for(IPreLog preLog : logInfoMap.values()){
            preLog.dealTimeout();
        }
    }

    public boolean hasNext(String flag){
        IPreLog preLogInfo = logInfoMap.get(flag);
        if(preLogInfo == null){
            return false;
        }

        return preLogInfo.hasNext();
    }

    public void dealTimeout(){
        for(IPreLog preLog : logInfoMap.values()){
            preLog.dealTimeout();
        }
    }


    private IPreLog createPreLogInfoByLogType(ClusterLog clusterLog){
        if(LogTypeConstant.CMS18.equalsIgnoreCase(clusterLog.getLogType())){
            return new CMSPreLogInfo(clusterLog.getLogFlag());
        }else{
            logger.info("not support log type of {}.", clusterLog.getLogType());
            logger.info("original log is {}.", clusterLog.getOriginalLog());
            return null;
        }
    }

    /**
     * 获取未完成的日志信息
     */
    public List<Map<String,Object>> getNotCompleteLog(){
        List<Map<String, Object>> notCompleteList = Lists.newArrayList();
        for (IPreLog preLog : logInfoMap.values()){
            notCompleteList.addAll(preLog.getNotCompleteLog());
        }
        return  notCompleteList;
    }


    /**
     * 获取指定未完成的日志信息
     */
    public List<Map<String,Object>> getNotCompleteLog(List<String> nodes){
        List<Map<String, Object>> notCompleteList = Lists.newArrayList();
        Set<Map.Entry<String,IPreLog>> sets = logInfoMap.entrySet();
        for(Map.Entry<String,IPreLog> entry:sets){
          if(nodes.contains(entry.getKey())){
              notCompleteList.addAll(entry.getValue().getNotCompleteLog());
          }
        }
        return  notCompleteList;
    }

    public static void main(String[] args) {
        LogPool pool = LogPool.getInstance();
        pool.addLog("{\"host\":\"123\", \"path\":\"123\", \"logtype\":\"cms18_gclog\", \"offset\":\"0\", \"message\":\"2016-12-28T10:07:21.971+0800: 1190255.366: [CMS-concurrent-mark: 0.970/0.970 secs] [Times: user=1.29 sys=0.09, real=0.97 secs]\"}");

        pool.addLog("{\"host\":\"123\", \"path\":\"123\", \"logtype\":\"cms18_gclog\", \"offset\":\"1\", \"message\":\"2016-12-28T10:07:21.971+0800: 1190255.3662016-12-28T10:07:20.994+0800: 1190254.390: [GC (CMS Initial Mark) [1 CMS-initial-mark: 2786997K(3670016K)] 2839212K(4141888K), 0.0059182 secs] [Times: user=0.02 sys=0.00, real=0.01 secs]\"}");
        pool.addLog("{\"host\":\"123\", \"path\":\"123\", \"logtype\":\"cms18_gclog\", \"offset\":\"2\", \"message\":\"2016-12-28T10:07:21.000+0800: 1190254.396: [CMS-concurrent-mark-start]\"}");
        pool.addLog("{\"host\":\"123\", \"path\":\"123\", \"logtype\":\"cms18_gclog\", \"offset\":\"3\", \"message\":\"2016-12-28T10:07:21.971+0800: 1190255.366: [CMS-concurrent-mark: 0.970/0.970 secs] [Times: user=1.29 sys=0.09, real=0.97 secs]\"}");
        pool.addLog("{\"host\":\"123\", \"path\":\"123\", \"logtype\":\"cms18_gclog\", \"offset\":\"4\", \"message\":\"2016-12-28T10:07:21.971+0800: 1190255.366: [CMS-concurrent-preclean-start]\"}");
        pool.addLog("{\"host\":\"123\", \"path\":\"123\", \"logtype\":\"cms18_gclog\", \"offset\":\"5\", \"message\":\"2016-12-28T10:07:21.991+0800: 1190255.387: [CMS-concurrent-preclean: 0.019/0.020 secs] [Times: user=0.02 sys=0.01, real=0.02 secs]\"}");
        pool.addLog("{\"host\":\"123\", \"path\":\"123\", \"logtype\":\"cms18_gclog\", \"offset\":\"6\", \"message\":\"2016-12-28T10:07:21.991+0800: 1190255.387: [CMS-concurrent-abortable-preclean-start]\"}");
        pool.addLog("{\"host\":\"123\", \"path\":\"123\", \"logtype\":\"cms18_gclog\", \"offset\":\"7\", \"message\":\"2016-12-28T10:07:27.079+0800: 1190260.474: [CMS-concurrent-abortable-preclean: 2.646/5.088 secs] [Times: user=4.50 sys=0.25, real=5.09 secs]\"}");
        pool.addLog("{\"host\":\"123\", \"path\":\"123\", \"logtype\":\"cms18_gclog\", \"offset\":\"8\", \"message\":\"2016-12-28T10:07:27.081+0800: 1190260.476: [GC (CMS Final Remark) [YG occupancy: 150056 K (471872 K)]2016-12-28T10:07:27.081+0800: 1190260.476: [Rescan (parallel) , 0.0174614 secs]2016-12-28T10:07:27.098+0800: 1190260.494: [weak refs processing, 0.0057690 secs]2016-12-28T10:07:27.104+0800: 1190260.499: [class unloading, 0.0364629 secs]2016-12-28T10:07:27.141+0800: 1190260.536: [scrub symbol table, 0.0153273 secs]2016-12-28T10:07:27.156+0800: 1190260.551: [scrub string table, 0.0021518 secs][1 CMS-remark: 2789424K(3670016K)] 2939480K(4141888K), 0.0790679 secs] [Times: user=0.11 sys=0.01, real=0.08 secs]\"}");
        pool.addLog("{\"host\":\"123\", \"path\":\"123\", \"logtype\":\"cms18_gclog\", \"offset\":\"9\", \"message\":\"2016-12-28T10:07:27.161+0800: 1190260.557: [CMS-concurrent-sweep-start]\"}");
        pool.addLog("{\"host\":\"123\", \"path\":\"123\", \"logtype\":\"cms18_gclog\", \"offset\":\"10\", \"message\":\"2016-12-28T10:07:27.899+0800: 1190261.294: [CMS-concurrent-sweep: 0.738/0.738 secs] [Times: user=1.57 sys=0.00, real=0.74 secs]\"}");
        pool.addLog("{\"host\":\"123\", \"path\":\"123\", \"logtype\":\"cms18_gclog\", \"offset\":\"11\", \"message\":\"2016-12-28T10:07:27.899+0800: 1190261.294: [CMS-concurrent-reset-start]\"}");
        pool.addLog("{\"host\":\"123\", \"path\":\"123\", \"logtype\":\"cms18_gclog\", \"offset\":\"12\", \"message\":\"2016-12-28T10:07:27.908+0800: 1190261.303: [CMS-concurrent-reset: 0.009/0.009 secs] [Times: user=0.00 sys=0.00, real=0.00 secs]\"}");
        System.out.println("add over");
    }
}
