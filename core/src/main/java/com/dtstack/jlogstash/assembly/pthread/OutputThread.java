/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.jlogstash.assembly.pthread;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import com.dtstack.jlogstash.assembly.qlist.QueueList;
import com.dtstack.jlogstash.factory.LogstashThreadFactory;
import com.dtstack.jlogstash.outputs.IBaseOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.jlogstash.exception.ExceptionUtil;
import com.dtstack.jlogstash.factory.OutputFactory;

/**
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年11月29日 下午15:30:18
 * Company: www.dtstack.com
 *
 * @author sishu.yss
 */
public class OutputThread implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(OutputThread.class);


    private List<IBaseOutput> outputProcessors;

    private static ExecutorService outputExecutor;

    private BlockingQueue<Map<String, Object>> outputQueue;


    public OutputThread(List<IBaseOutput> outputProcessors, BlockingQueue<Map<String, Object>> outputQueue) {
        this.outputProcessors = outputProcessors;
        this.outputQueue = outputQueue;
    }

    @SuppressWarnings("rawtypes")
    public static void initOutPutThread(List<Map> outputs, QueueList outPutQueueList, List<IBaseOutput> allBaseOutputs) throws Exception {
        if (outputExecutor == null) {
            int size = outPutQueueList.getQueueList().size();
            outputExecutor = new ThreadPoolExecutor(size, size,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(), new LogstashThreadFactory(OutputThread.class.getName()));
        }
        for (BlockingQueue<Map<String, Object>> queueList : outPutQueueList.getQueueList()) {
            List<IBaseOutput> baseOutputs = OutputFactory.getBatchInstance(outputs);
            allBaseOutputs.addAll(baseOutputs);
            outputExecutor.submit(new OutputThread(baseOutputs, queueList));
        }
    }

    @Override
    public void run() {
        Map<String, Object> event = null;
        try {
            Thread.sleep(2000);
            while (true) {
                if (!priorityFail()) {
                    event = this.outputQueue.take();
                    if (event != null) {
                        for (IBaseOutput bo : outputProcessors) {
                            bo.process(event);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("{}:output event failed:{}", event, ExceptionUtil.getErrorMessage(e));
        }
    }

    private boolean priorityFail() {
        //优先处理失败信息
        boolean dealFailMsg = false;
        for (IBaseOutput bo : outputProcessors) {
            if (bo.isConsistency()) {
                dealFailMsg = dealFailMsg || bo.dealFailedMsg();
            }
        }
        return dealFailMsg;
    }
}
