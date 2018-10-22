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

import java.util.List;
import java.util.Map;

/**
 * 未处理日志存放接口
 * Date: 2016/12/30
 * Company: www.dtstack.com
 * @ahthor xuchao
 */
public interface IPreLog {

    /**
     * 判断从startIndex开始是否包含一条完整日志
     * @return
     */
    boolean checkIsCompleteLog(int startIndex);


    /**
     * 执行日志合并
     * @return
     */
    List<CompletedLog> mergeGcLog();

    /**
     * 添加一条日志源
     * @param addLog
     * @return
     */
    boolean addLog(ClusterLog addLog);

    ClusterLog remove(int index);

    boolean remove(ClusterLog log );

    List<Map<String,Object>> getNotCompleteLog();

    /***
     * 暂时未想到比较好的解决办法,当前处理是每次超过过期时间就删除第一条数据
     */
    void dealTimeout();

    int getPoolSize();

    boolean hasNext();

}
