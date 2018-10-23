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

package com.dtstack.jlogstash.metrics.groups;


import com.dtstack.jlogstash.metrics.CharacterFilter;
import com.dtstack.jlogstash.metrics.MetricRegistry;
import com.dtstack.jlogstash.metrics.scope.ScopeFormat;

import java.util.Collections;
import java.util.Map;

/**
 * copy from https://github.com/apache/flink
 * <p>
 * Special {@link com.dtstack.jlogstash.metrics.MetricGroup} representing a JobManager.
 * <p>
 * <p>Contains extra logic for adding jobs with tasks, and removing jobs when they do
 * not contain tasks any more
 */
public class JobMetricGroup extends ComponentMetricGroup<JobMetricGroup> {

    private final String hostname;

    public JobMetricGroup(MetricRegistry registry, String hostname, String jobName) {
        super(registry, registry.getScopeFormat().formatScope(hostname, null, null, jobName), null);
        this.hostname = hostname;
    }

    public String hostname() {
        return hostname;
    }

    // ------------------------------------------------------------------------
    //  Component Metric Group Specifics
    // ------------------------------------------------------------------------

    @Override
    protected void putVariables(Map<String, String> variables) {
        variables.put(ScopeFormat.SCOPE_HOST, hostname);
    }

    @Override
    protected Iterable<? extends ComponentMetricGroup> subComponents() {
        return Collections.EMPTY_LIST;
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return "jlogstash";
    }
}

