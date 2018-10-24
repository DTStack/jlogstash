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

package com.dtstack.jlogstash.metrics.promethues;

import com.dtstack.jlogstash.metrics.base.MetricConfig;
import com.dtstack.jlogstash.metrics.base.reporter.Scheduled;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;

import java.io.IOException;


/**
 * {@link com.dtstack.jlogstash.metrics.base.reporter.MetricReporter} that exports {@link com.dtstack.jlogstash.metrics.base.Metric Metrics} via Prometheus {@link PushGateway}.
 */
public class PrometheusPushGatewayReporter extends AbstractPrometheusReporter implements Scheduled {

	private PushGateway pushGateway;
	private String jobName;
	private boolean deleteOnShutdown;

	@Override
	public void open(MetricConfig config) {
		String host = config.getString("host", null);
		int port = config.getInteger("port", -1);
		String configuredJobName = config.getString("jobName", "");
		deleteOnShutdown = config.getBoolean("deleteOnShutdown", true);

		if (host == null || host.isEmpty() || port < 1) {
			throw new IllegalArgumentException("Invalid host/port configuration. Host: " + host + " Port: " + port);
		}

		this.jobName = configuredJobName;

		pushGateway = new PushGateway(host + ':' + port);
		log.info("Configured PrometheusPushGatewayReporter with {host:{}, port:{}, jobName: {}, deleteOnShutdown:{}}", host, port, jobName, deleteOnShutdown);
	}

	@Override
	public void report() {
		try {
			pushGateway.push(CollectorRegistry.defaultRegistry, jobName);
		} catch (Exception e) {
			log.warn("Failed to push metrics to PushGateway with jobName {}.", jobName, e);
		}
	}

	@Override
	public void close() {
		if (deleteOnShutdown && pushGateway != null) {
			try {
				pushGateway.delete(jobName);
			} catch (IOException e) {
				log.warn("Failed to delete metrics from PushGateway with jobName {}.", jobName, e);
			}
		}
		super.close();
	}
}
