/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.jlogstash.metrics;

import com.dtstack.jlogstash.assembly.CmdLineParams;
import com.dtstack.jlogstash.metrics.base.CharacterFilter;
import com.dtstack.jlogstash.metrics.groups.ComponentMetricGroup;
import com.dtstack.jlogstash.metrics.groups.JlogstashJobMetricGroup;
import com.dtstack.jlogstash.metrics.groups.PipelineInputMetricGroup;
import com.dtstack.jlogstash.metrics.groups.PipelineOutputMetricGroup;
import com.dtstack.jlogstash.metrics.util.MetricUtils;
import com.dtstack.jlogstash.utils.LocalIpAddressUtil;

import java.util.List;
import java.util.Map;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/7/30
 */
public class JlogstashMetric {

    private static List<Map> metrics;
    private static JlogstashMetric jlogstashMetric = new JlogstashMetric();


    private static MetricRegistryImpl metricRegistry;
    private static JlogstashJobMetricGroup jlogstashJobMetricGroup;
    private static PipelineInputMetricGroup pipelineInputMetricGroup;
    private static PipelineOutputMetricGroup pipelineOutputMetricGroup;


    public static JlogstashMetric getInstance(List<Map> m) {
        metrics = m;
        return jlogstashMetric;
    }

    public JlogstashMetric() {
        String hostname = LocalIpAddressUtil.getLocalAddress();

        metricRegistry = new MetricRegistryImpl(metrics);
        jlogstashJobMetricGroup = MetricUtils.instantiateTaskManagerMetricGroup(metricRegistry);
        pipelineInputMetricGroup = new PipelineInputMetricGroup<>(metricRegistry, hostname, "input", "", CmdLineParams.getName());
        pipelineOutputMetricGroup = new PipelineOutputMetricGroup<>(metricRegistry, hostname, "output", "", CmdLineParams.getName());

    }

    public static JlogstashJobMetricGroup getJlogstashJobMetricGroup() {
        return jlogstashJobMetricGroup;
    }

    public static PipelineInputMetricGroup getPipelineInputMetricGroup() {
        return pipelineInputMetricGroup;
    }

    public static PipelineOutputMetricGroup getPipelineOutputMetricGroup() {
        return pipelineOutputMetricGroup;
    }

    public void close() {
        if (jlogstashJobMetricGroup != null) {
            jlogstashJobMetricGroup.close();
        }
        if (pipelineInputMetricGroup != null) {
            pipelineInputMetricGroup.close();
        }
        if (pipelineOutputMetricGroup != null) {
            pipelineOutputMetricGroup.close();
        }
        // metrics shutdown
        if (metricRegistry != null) {
            metricRegistry.shutdown();
            metricRegistry = null;
        }
    }
}
