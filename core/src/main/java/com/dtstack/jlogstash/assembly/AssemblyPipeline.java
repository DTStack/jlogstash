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
import com.dtstack.jlogstash.assembly.qlist.FilterQueueList;
import com.dtstack.jlogstash.assembly.qlist.OutPutQueueList;
import com.dtstack.jlogstash.configs.YamlConfig;
import com.dtstack.jlogstash.exception.LogstashException;
import com.dtstack.jlogstash.factory.InputFactory;
import com.dtstack.jlogstash.inputs.BaseInput;
import com.dtstack.jlogstash.metrics.MetricRegistryImpl;
import com.dtstack.jlogstash.outputs.BaseOutput;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.jlogstash.configs.ConfigObject;

import java.util.List;
import java.util.Map;

/**
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:25:11
 * Company: www.dtstack.com
 *
 * @author sishu.yss
 */
public class AssemblyPipeline {

    private static Logger logger = LoggerFactory.getLogger(AssemblyPipeline.class);

    private FilterQueueList initFilterQueueList;

    private OutPutQueueList initOutputQueueList;

    private List<BaseInput> baseInputs;

    private List<BaseOutput> allBaseOutputs = Lists.newCopyOnWriteArrayList();

    private MetricRegistryImpl metricRegistry;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void assemblyPipeline() throws Exception {
        logger.info("load config start ...");
        ConfigObject configs = new YamlConfig().parse(CmdLineParams.getConfigFile());
        List<Map> inputs = configs.getInputs();
        if (CollectionUtils.isEmpty(inputs)) {
            throw new LogstashException("input plugin is empty");
        }
        List<Map> outputs = configs.getOutputs();
        if (CollectionUtils.isEmpty(outputs)) {
            throw new LogstashException("output plugin is empty");
        }
        logger.info("assemblyPipeline start ...");
        List<Map> filters = configs.getFilters();

        List<Map> metrics = configs.getMetrics();
        if (CollectionUtils.isNotEmpty(metrics)) {
            metricRegistry = new MetricRegistryImpl(metrics);
            BaseInput.setMetricRegistry(metricRegistry);
            BaseOutput.setMetricRegistry(metricRegistry);
        }
        if (CollectionUtils.isNotEmpty(filters)) {
            initFilterQueueList = FilterQueueList.getFilterQueueListInstance(CmdLineParams.getFilterWork(), CmdLineParams.getFilterQueueSize());
            baseInputs = InputFactory.getBatchInstance(inputs, initFilterQueueList);
            InputThread.initInputThread(baseInputs);
            initOutputQueueList = OutPutQueueList.getOutPutQueueListInstance(CmdLineParams.getOutputWork(), CmdLineParams.getOutputQueueSize());
            FilterThread.initFilterThread(filters, initFilterQueueList, initOutputQueueList);
            OutputThread.initOutPutThread(outputs, initOutputQueueList, allBaseOutputs);
        } else {
            initOutputQueueList = OutPutQueueList.getOutPutQueueListInstance(CmdLineParams.getOutputWork(), CmdLineParams.getOutputQueueSize());
            baseInputs = InputFactory.getBatchInstance(inputs, initOutputQueueList);
            InputThread.initInputThread(baseInputs);
            OutputThread.initOutPutThread(outputs, initOutputQueueList, allBaseOutputs);
        }
        addShutDownHook();
    }

    private void addShutDownHook() {
        ShutDownHook shutDownHook = new ShutDownHook(initFilterQueueList, initOutputQueueList, baseInputs, allBaseOutputs, metricRegistry);
        shutDownHook.addShutDownHook();
    }
}