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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;

import com.dtstack.jlogstash.factory.LogstashThreadFactory;
import com.dtstack.jlogstash.inputs.IBaseInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:25:29
 * Company: www.dtstack.com
 *
 * @author sishu.yss
 */
public class InputThread implements Runnable {

    private Logger logger = LoggerFactory.getLogger(InputThread.class);

    private IBaseInput baseInput;

    private static ExecutorService inputExecutor;

    public InputThread(IBaseInput baseInput) {
        this.baseInput = baseInput;
    }

    @Override
    public void run() {
        if (baseInput == null) {
            logger.error("input plugin is not null");
            System.exit(1);
        }
        baseInput.emit();
    }

    public static void initInputThread(List<IBaseInput> baseInputs) {
        if (inputExecutor == null) {
            int size = baseInputs.size();
            inputExecutor = new ThreadPoolExecutor(size, size,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(), new LogstashThreadFactory(InputThread.class.getName()));
        }
        for (IBaseInput input : baseInputs) {
            inputExecutor.submit(new InputThread(input));
        }
    }
}
