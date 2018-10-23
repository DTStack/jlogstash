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
import com.dtstack.jlogstash.metrics.Counter;
import com.dtstack.jlogstash.metrics.Meter;
import com.dtstack.jlogstash.metrics.MeterView;
import com.dtstack.jlogstash.metrics.MetricNames;
import com.dtstack.jlogstash.metrics.MetricRegistry;
import com.dtstack.jlogstash.metrics.SimpleCounter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Metric group that contains shareable pre-defined IO-related metrics. The metrics registration is
 * forwarded to the parent task metric group.
 */
public class PipelineIOMetricGroup<C extends ComponentMetricGroup<C>> extends ComponentMetricGroup {

    private final Counter numBytesOut;
    private final Counter numBytesInLocal;
    private final Counter numBytesInRemote;
    private final SumCounter numRecordsIn;
    private final SumCounter numRecordsOut;

    private final Meter numBytesInRateLocal;
    private final Meter numBytesInRateRemote;
    private final Meter numBytesOutRate;
    private final Meter numRecordsInRate;
    private final Meter numRecordsOutRate;

    public PipelineIOMetricGroup(MetricRegistry registry,
                                 C parent,
                                 String[] scope) {
        super(registry, scope, parent);

        this.numBytesOut = counter(MetricNames.IO_NUM_BYTES_OUT);
        this.numBytesInLocal = counter(MetricNames.IO_NUM_BYTES_IN_LOCAL);
        this.numBytesInRemote = counter(MetricNames.IO_NUM_BYTES_IN_REMOTE);
        this.numBytesOutRate = meter(MetricNames.IO_NUM_BYTES_OUT_RATE, new MeterView(numBytesOut, 60));
        this.numBytesInRateLocal = meter(MetricNames.IO_NUM_BYTES_IN_LOCAL_RATE, new MeterView(numBytesInLocal, 60));
        this.numBytesInRateRemote = meter(MetricNames.IO_NUM_BYTES_IN_REMOTE_RATE, new MeterView(numBytesInRemote, 60));
        this.numRecordsIn = (SumCounter) counter(MetricNames.IO_NUM_RECORDS_IN, new SumCounter());
        this.numRecordsOut = (SumCounter) counter(MetricNames.IO_NUM_RECORDS_OUT, new SumCounter());
        this.numRecordsInRate = meter(MetricNames.IO_NUM_RECORDS_IN_RATE, new MeterView(numRecordsIn, 60));
        this.numRecordsOutRate = meter(MetricNames.IO_NUM_RECORDS_OUT_RATE, new MeterView(numRecordsOut, 60));
    }

//	public IOMetrics createSnapshot() {
//		return new IOMetrics(numRecordsInRate, numRecordsOutRate, numBytesInRateLocal, numBytesInRateRemote, numBytesOutRate);
//	}

    // ============================================================================================
    // Getters
    // ============================================================================================
    public Counter getNumBytesOutCounter() {
        return numBytesOut;
    }

    public Counter getNumBytesInLocalCounter() {
        return numBytesInLocal;
    }

    public Counter getNumBytesInRemoteCounter() {
        return numBytesInRemote;
    }

    public Counter getNumRecordsInCounter() {
        return numRecordsIn;
    }

    public Counter getNumRecordsOutCounter() {
        return numRecordsOut;
    }

    public Meter getNumBytesInLocalRateMeter() {
        return numBytesInRateLocal;
    }

    public Meter getNumBytesInRemoteRateMeter() {
        return numBytesInRateRemote;
    }

    public Meter getNumBytesOutRateMeter() {
        return numBytesOutRate;
    }

    // ============================================================================================
    // Buffer metrics
    // ============================================================================================

    /**
     * Initialize Buffer Metrics for a task.
     */
//	public void initializeBufferMetrics(Task task) {
//		final MetricGroup buffers = addGroup("buffers");
//		buffers.gauge("inputQueueLength", new InputBuffersGauge(task));
//		buffers.gauge("outputQueueLength", new OutputBuffersGauge(task));
//		buffers.gauge("inPoolUsage", new InputBufferPoolUsageGauge(task));
//		buffers.gauge("outPoolUsage", new OutputBufferPoolUsageGauge(task));
//	}
    @Override
    protected void putVariables(Map variables) {

    }

    @Override
    protected Iterable<? extends ComponentMetricGroup> subComponents() {
        return null;
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return null;
    }

//	/**
//	 * Gauge measuring the number of queued input buffers of a task.
//	 */
//	private static final class InputBuffersGauge implements Gauge<Integer> {
//
//		private final Task task;
//
//		public InputBuffersGauge(Task task) {
//			this.task = task;
//		}
//
//		@Override
//		public Integer getValue() {
//			int totalBuffers = 0;
//
//			for (SingleInputGate inputGate : task.getAllInputGates()) {
//				totalBuffers += inputGate.getNumberOfQueuedBuffers();
//			}
//
//			return totalBuffers;
//		}
//	}
//
//	/**
//	 * Gauge measuring the number of queued output buffers of a task.
//	 */
//	private static final class OutputBuffersGauge implements Gauge<Integer> {
//
//		private final Task task;
//
//		public OutputBuffersGauge(Task task) {
//			this.task = task;
//		}
//
//		@Override
//		public Integer getValue() {
//			int totalBuffers = 0;
//
//			for (ResultPartition producedPartition : task.getProducedPartitions()) {
//				totalBuffers += producedPartition.getNumberOfQueuedBuffers();
//			}
//
//			return totalBuffers;
//		}
//	}
//
//	/**
//	 * Gauge measuring the input buffer pool usage gauge of a task.
//	 */
//	private static final class InputBufferPoolUsageGauge implements Gauge<Float> {
//
//		private final Task task;
//
//		public InputBufferPoolUsageGauge(Task task) {
//			this.task = task;
//		}
//
//		@Override
//		public Float getValue() {
//			int usedBuffers = 0;
//			int bufferPoolSize = 0;
//
//			for (SingleInputGate inputGate : task.getAllInputGates()) {
//				usedBuffers += inputGate.getBufferPool().bestEffortGetNumOfUsedBuffers();
//				bufferPoolSize += inputGate.getBufferPool().getNumBuffers();
//			}
//
//			if (bufferPoolSize != 0) {
//				return ((float) usedBuffers) / bufferPoolSize;
//			} else {
//				return 0.0f;
//			}
//		}
//	}
//
//	/**
//	 * Gauge measuring the output buffer pool usage gauge of a task.
//	 */
//	private static final class OutputBufferPoolUsageGauge implements Gauge<Float> {
//
//		private final Task task;
//
//		public OutputBufferPoolUsageGauge(Task task) {
//			this.task = task;
//		}
//
//		@Override
//		public Float getValue() {
//			int usedBuffers = 0;
//			int bufferPoolSize = 0;
//
//			for (ResultPartition resultPartition : task.getProducedPartitions()) {
//				usedBuffers += resultPartition.getBufferPool().bestEffortGetNumOfUsedBuffers();
//				bufferPoolSize += resultPartition.getBufferPool().getNumBuffers();
//			}
//
//			if (bufferPoolSize != 0) {
//				return ((float) usedBuffers) / bufferPoolSize;
//			} else {
//				return 0.0f;
//			}
//		}
//	}

    // ============================================================================================
    // Metric Reuse
    // ============================================================================================
    public void reuseRecordsInputCounter(Counter numRecordsInCounter) {
        this.numRecordsIn.addCounter(numRecordsInCounter);
    }

    public void reuseRecordsOutputCounter(Counter numRecordsOutCounter) {
        this.numRecordsOut.addCounter(numRecordsOutCounter);
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
