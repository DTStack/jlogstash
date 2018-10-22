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

import com.dtstack.jlogstash.exception.ExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 * 未处理日志存放接口
 * Date: 2016/12/30
 * Company: www.dtstack.com
 * @ahthor xuchao
 */
public abstract class IPreLogx {

    private static final Logger logger = LoggerFactory
            .getLogger(IPreLogx.class);

    private  final ReentrantLock lock = new ReentrantLock(false);

    private  final Condition notFull = lock.newCondition();

    protected List<ClusterLog> logList = new CopyOnWriteArrayList<ClusterLog>();

    protected static int maxClusterLogSize = 10000 ;

    /**
     * 判断是不是一个完整的日志逻辑
     * @return
     */
    abstract boolean checkIsCompleteLog();



    /**
     * 执行日志合并
     * @return
     */
    abstract CompletedLog mergeGcLog();

    /**
     * 添加一条日志源
     * @param addLog
     * @return
     */
    abstract boolean addLog(ClusterLog addLog);

    abstract ClusterLog remove(int index);

    abstract boolean remove(ClusterLog log);

    abstract List<Map<String,Object>> getNotCompleteLog();

    /***
     * 暂时未想到比较好的解决办法,当前处理是每次超过过期时间就删除第一条数据
     */
    abstract void dealTimeout();

    void addLogList(int index,ClusterLog clusterLog){
           while (logList.size() >= maxClusterLogSize){
               try{
                   lock.lockInterruptibly();
                   notFull.await();
               }catch(Exception e){
                    logger.error(ExceptionUtil.getErrorMessage(e));
               }finally {
                   lock.unlock();
               }
           }
        logList.add(index,clusterLog);
    }

    ClusterLog removeLogList(int index){
        ClusterLog clusterLog =  logList.remove(index);
        if(logList.size()<maxClusterLogSize){
            try{
                lock.lockInterruptibly();
                notFull.signalAll();
            }catch(Exception e){
                logger.error(ExceptionUtil.getErrorMessage(e));
            }finally {
                lock.unlock();
            }
        }
        return clusterLog;
    }

    boolean removeLogList(Object obj){
            boolean result = logList.remove(obj);
            if(logList.size()<maxClusterLogSize){
                try{
                    lock.lockInterruptibly();
                    notFull.signalAll();
                }catch(Exception e){
                    logger.error(ExceptionUtil.getErrorMessage(e));
                }finally {
                    lock.unlock();
                }
            }
            return result;
    }
}
