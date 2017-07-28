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
package com.dtstack.jlogstash.assembly;

import com.dtstack.jlogstash.assembly.pthread.FilterThread;
import com.dtstack.jlogstash.assembly.pthread.InputThread;
import com.dtstack.jlogstash.assembly.pthread.OutputThread;
import com.dtstack.jlogstash.assembly.qlist.InputQueueList;
import com.dtstack.jlogstash.assembly.qlist.OutPutQueueList;
import com.dtstack.jlogstash.configs.YamlConfig;
import com.dtstack.jlogstash.exception.LogstashException;
import com.dtstack.jlogstash.factory.InputFactory;
import com.dtstack.jlogstash.inputs.BaseInput;
import com.dtstack.jlogstash.outputs.BaseOutput;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 *
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:25:11
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class AssemblyPipeline {

    private static Logger logger = LoggerFactory.getLogger(AssemblyPipeline.class);

    private InputQueueList initInputQueueList;

    private OutPutQueueList initOutputQueueList;

    private List<BaseInput> baseInputs;

    private List<BaseOutput> allBaseOutputs = Lists.newCopyOnWriteArrayList();

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void assemblyPipeline() throws Exception {
        logger.debug("load config start ...");
        Map configs = new YamlConfig().parse(CmdLineParams.getConfigFilePath());
        logger.debug(configs.toString());
        logger.debug("initInputQueueList start ...");
        initInputQueueList = InputQueueList.getInputQueueListInstance(CmdLineParams.getFilterWork(), CmdLineParams.getInputQueueSize());
        List<Map> inputs = (List<Map>) configs.get("inputs");
        if (inputs == null || inputs.size() == 0) {
            throw new LogstashException("input plugin is empty");
        }
        initOutputQueueList = OutPutQueueList.getOutPutQueueListInstance(CmdLineParams.getOutputWork(), CmdLineParams.getOutputQueueSize());
        List<Map> outputs = (List<Map>) configs.get("outputs");
        if (outputs == null || outputs.size() == 0) {
            throw new LogstashException("output plugin is empty");
        }
        List<Map> filters = (List<Map>) configs.get("filters");
        baseInputs = InputFactory.getBatchInstance(inputs, initInputQueueList);
        InputThread.initInputThread(baseInputs);
        FilterThread.initFilterThread(filters, initInputQueueList, initOutputQueueList);
        OutputThread.initOutPutThread(outputs, initOutputQueueList, allBaseOutputs);
        addShutDownHook();
    }

    private void addShutDownHook() {
        ShutDownHook shutDownHook = new ShutDownHook(initInputQueueList, initOutputQueueList, baseInputs, allBaseOutputs);
        shutDownHook.addShutDownHook();
    }
}