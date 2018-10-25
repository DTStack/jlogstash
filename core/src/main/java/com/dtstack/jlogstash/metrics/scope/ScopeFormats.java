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

package com.dtstack.jlogstash.metrics.scope;


/**
 * A container for component scope formats.
 */
public final class ScopeFormats {

	private final PipelineScopeFormat pipelineScopeFormat;
	private final JlogstashJobScopeFormat jlogstashJobScopeFormat;

	// ------------------------------------------------------------------------

	/**
	 * Creates all scope formats, based on the given scope format strings.
	 */
	private ScopeFormats(
			String jlogstashJobScopeFormat,
			String pipelineScopeFormat) {
		this.jlogstashJobScopeFormat = new JlogstashJobScopeFormat(jlogstashJobScopeFormat);
		this.pipelineScopeFormat = new PipelineScopeFormat(pipelineScopeFormat);
	}

	// ------------------------------------------------------------------------
	//  Accessors
	// ------------------------------------------------------------------------


	public JlogstashJobScopeFormat getJlogstashJobScopeFormat() {
		return jlogstashJobScopeFormat;
	}

	public PipelineScopeFormat getPipelineScopeFormat() {
		return this.pipelineScopeFormat;
	}

	// ------------------------------------------------------------------------
	//  Parsing from Config
	// ------------------------------------------------------------------------

	public static ScopeFormats fromDefault() {
		String jlogstashJobScopeFormat = "<host>.jlogstash.<job_name>";
		String pipelineScopeFormat = "<host>.jlogstash.<plugin_type>.<plugin_name>.<job_name>";

		return new ScopeFormats(jlogstashJobScopeFormat, pipelineScopeFormat);
	}
}
