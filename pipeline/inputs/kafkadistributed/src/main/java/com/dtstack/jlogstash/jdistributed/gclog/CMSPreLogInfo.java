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
package com.dtstack.jlogstash.jdistributed.gclog;

import com.dtstack.jlogstash.distributed.logmerge.*;
import com.dtstack.jlogstash.inputs.BaseInput;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 接收发送过来的日志容器,日志根据时间排序(升序)
 * 当前仅针对一个用户的一个日志文件
 * FIXME 需要判断每条记录的过期时间
 * Date: 2016/12/28
 * Company: www.dtstack.com
 * @ahthor xuchao
 */

public class CMSPreLogInfo implements IPreLog {

    private static final Logger logger = LoggerFactory.getLogger(CMSPreLogInfo.class);

    /**cms消息的超时时间*/
    private static final int TIME_OUT = 10 * 60 * 1000;

    private CMSLogPattern logMerge = new CMSLogPattern();

    private List<ClusterLog> logList; //FIXME CopyOnWriteArrayList 不适合该场景,考虑用其他的队列替换

    private String flag;

    private final ReentrantLock lock = new ReentrantLock(false);

    private static final Gson gson = new Gson();

    public CMSPreLogInfo(String flag){
        this.flag  = flag;
        logList = new CopyOnWriteArrayList<ClusterLog>();
    }

    /**
     * 需要根据规则判断是否可以加入到该队列中
     * @param addLog
     * @return
     */
    public boolean addLog(ClusterLog addLog){//插入的时候根据时间排序,升序

        if(!logMerge.checkIsFullGC(addLog.getLoginfo())){//非full gc 直接添加到inputlist
            Map<String, Object> eventMap = addLog.getOriginalLog();
            if(logMerge.checkIsYoungGC(addLog.getLoginfo())){//判断是不是younggc
                eventMap.put("gctype", GCTypeConstant.YOUNG_LOG_TYPE);
            }else if(logMerge.checkIsGCBegin(addLog.getLoginfo())){//判断是不是gc开始标识
                eventMap.put("gctype", GCTypeConstant.GC_BEGIN);
            }

            BaseInput.getInputQueueList().put(eventMap);
            return true;
        }

        logger.debug("offset:{},---cmslog:{}.", addLog.getOffset(), addLog.getLoginfo());
        try{
            lock.lockInterruptibly();
            int addPos = logList.size();
            for(int i=0; i<logList.size(); i++){
                ClusterLog compLog = logList.get(i);
                if(addLog.getOffset() < compLog.getOffset()){
                    addPos = i;
                    break;
                }
            }
            logger.warn("add log:{}.", addLog.getLoginfo());
            logList.add(addPos, addLog);
        }catch (Exception e){
            logger.error("", e);
        }finally {
            lock.unlock();
        }


        if(logList.size() >= CMSLogPattern.MERGE_NUM){
            logger.warn("pre merge log, logList size:{}.", logList.size());
            List<CompletedLog> rstList = mergeGcLog();
            logger.warn("after merge log, logList size:{}, rstList size:{}.", logList.size(), rstList.size());
            if(rstList != null){
                for(CompletedLog log : rstList){
                    logger.warn("pre insert into inputqueuelist, logInfo:{}.", log.getLogInfo());
                    BaseInput.getInputQueueList().put(log.getEventMap());
                    logger.warn("after insert into inputqueuelist");
                }
            }
        }
        return true;
    }


    /**
     * 合并出完整的日志
     * @return
     */
    @Override
    public List<CompletedLog> mergeGcLog(){

        List<CompletedLog> rstList = Lists.newArrayList();
        try{
            lock.lockInterruptibly();
            int logListSize = logList.size();
            for(int i=0; i < logListSize; ){

                if(i + 1 > logList.size()){
                    break;
                }

                ClusterLog currLog = logList.get(i);
                if(!logMerge.checkInitialMark(currLog.getLoginfo())){
                    i++;
                    continue;
                }

                if(!checkIsCompleteLog(i)){
                    i++;
                    continue;
                }

                CompletedLog cmsLog = new CompletedLog();
                int end = CMSLogPattern.MERGE_NUM + i;
                for (int logIndex = i; logIndex < end; logIndex++){
                    ClusterLog targetLog = logList.remove(i);//一直remove第i个
                    if(targetLog == null){
                        break;
                    }

                    if(logIndex == i){
                        cmsLog.setEventMap(targetLog.getBaseInfo());
                    }

                    cmsLog.addLog(targetLog.getLoginfo());
                }

                Map<String, Object> extInfo = Maps.newHashMap();
                extInfo.put("gctype", GCTypeConstant.CMS_LOG_TYPE);
                cmsLog.complete(extInfo);
                rstList.add(cmsLog);
            }
        }catch(Exception e){
            logger.error("", e);
        }finally {
            lock.unlock();
        }

        return rstList;
    }

    @Override
    public ClusterLog remove(int index){
        return logList.remove(index);
    }

    @Override
    public boolean remove(ClusterLog log ){
        return logList.remove(log);
    }

    @Override
    public List<Map<String, Object>> getNotCompleteLog() {
        Gson gson = new Gson();
        List<Map<String, Object>> rstList = Lists.newArrayList();
        for (ClusterLog log : logList){
            rstList.add(log.getOriginalLog());
        }

        logger.warn("----getnotcomp----:{}", gson.toJson(rstList));

        return rstList;
    }

    /**
     * 判断是不是一条完整的日志
     * @return
     */
    public boolean checkIsCompleteLog(int startIndex){
        boolean isCompleteLog = logMerge.checkIsCompleteLog(logList, startIndex);
        if (isCompleteLog){
            logger.debug("get a full msg..");
        }

        return isCompleteLog;
    }


    @Override
    public void dealTimeout(){//删除过期数据

        try{
            lock.lockInterruptibly();
            long expriodTime = System.currentTimeMillis() - TIME_OUT;
            for(ClusterLog log : logList){
                if(log.getGeneTime() < expriodTime){
                    logList.remove(log);
                    logger.warn("remove time out log:{}", log.getLoginfo());
                }
            }
        }catch (Exception e){
            logger.error("", e);
        }finally {
            lock.unlock();
        }

    }

    @Override
    public int getPoolSize() {
        return logList.size();
    }

    public boolean hasNext(){
        if(logList.size() >= CMSLogPattern.MERGE_NUM){
            return true;
        }

        return false;
    }


    @Override
    public String toString() {
        return logList.toString();
    }
}
