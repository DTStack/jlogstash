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


import com.dtstack.jlogstash.metrics.MetricNames;
import com.dtstack.jlogstash.metrics.MetricRegistry;
import com.dtstack.jlogstash.metrics.base.CharacterFilter;
import com.dtstack.jlogstash.metrics.base.Counter;
import com.dtstack.jlogstash.metrics.base.Meter;
import com.dtstack.jlogstash.metrics.base.MeterView;
import com.dtstack.jlogstash.metrics.base.SimpleCounter;
import com.dtstack.jlogstash.metrics.scope.ScopeFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Metric group that contains shareable pre-defined IO-related metrics. The metrics registration is
 * forwarded to the parent task metric group.
 */
public class PipelineInputMetricGroup<C extends ComponentMetricGroup<C>> extends ComponentMetricGroup {

    private final Counter numBytesInLocal;
    private final SumCounter numRecordsIn;

    private final Meter numBytesInRateLocal;
    private final Meter numRecordsInRate;

    private final String hostname;
    private final String pluginType;
    private final String pluginName;

    public PipelineInputMetricGroup(MetricRegistry registry,
                                    String hostname,
                                    String pluginType,
                                    String pluginName) {
        super(registry, registry.getScopeFormats().getPipelineScopeFormat().formatScope(hostname, pluginType, pluginName), null);

        this.hostname = hostname;
        this.pluginType = pluginType;
        this.pluginName = pluginName;

        this.numBytesInLocal = counter(MetricNames.IO_NUM_BYTES_IN_LOCAL);
        this.numBytesInRateLocal = meter(MetricNames.IO_NUM_BYTES_IN_LOCAL_RATE, new MeterView(numBytesInLocal, 60));
        this.numRecordsIn = (SumCounter) counter(MetricNames.IO_NUM_RECORDS_IN, new SumCounter());
        this.numRecordsInRate = meter(MetricNames.IO_NUM_RECORDS_IN_RATE, new MeterView(numRecordsIn, 60));
    }

    // ============================================================================================
    // Getters
    // ============================================================================================

    public Counter getNumBytesInLocalCounter() {
        return numBytesInLocal;
    }

    public Counter getNumRecordsInCounter() {
        return numRecordsIn;
    }

    public Meter getNumBytesInLocalRateMeter() {
        return numBytesInRateLocal;
    }


    @Override
    protected void putVariables(Map variables) {
        variables.put(ScopeFormat.SCOPE_HOST, hostname);
        variables.put(ScopeFormat.SCOPE_PLUGINE_TYPE, pluginType);
        variables.put(ScopeFormat.SCOPE_PLUGINE_NAME, pluginName);
    }

    @Override
    protected Iterable<? extends ComponentMetricGroup> subComponents() {
        return null;
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return "pipeline";
    }

    // ============================================================================================
    // Metric Reuse
    // ============================================================================================
    public void reuseRecordsInputCounter(Counter numRecordsInCounter) {
        this.numRecordsIn.addCounter(numRecordsInCounter);
    }

    /**
     * A {@link SimpleCounter} that can contain other {@link Counter}s. A call to {@link SumCounter#getCount()} returns
     * the sum of this counters and all contained counters.
     */
    private static class SumCounter extends SimpleCounter {
        private final List<Counter> internalCounters = new ArrayList<>();

        SumCounter() {
        }

        public void addCounter(Counter toAdd) {
            internalCounters.add(toAdd);
        }

        @Override
        public long getCount() {
            long sum = super.getCount();
            for (Counter counter : internalCounters) {
                sum += counter.getCount();
            }
            return sum;
        }
    }
}
