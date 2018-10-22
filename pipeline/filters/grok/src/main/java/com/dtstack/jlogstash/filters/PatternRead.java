/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.jlogstash.filters;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * 
 * @author sishu.yss
 *
 */
public class PatternRead {

	private static Logger logger = LoggerFactory.getLogger(PatternRead.class);

	private static String patternFile = "pattern";

	private static Map<String, String> patterns = Maps.newLinkedHashMap();

	public static void patternRead() {
		if (patterns.size() == 0) {
			BufferedReader br = null;
			InputStreamReader ir = null;
			try {
				ir = new InputStreamReader(PatternRead.class.getClassLoader()
						.getResourceAsStream(patternFile));
				br = new BufferedReader(ir);
				String line;
				// We dont want \n and commented line
				Pattern pattern = Pattern.compile("^([A-z0-9_]+)\\s+(.*)$");
				while ((line = br.readLine()) != null) {
					Matcher m = pattern.matcher(line);
					if (m.matches()) {
						patterns.put(m.group(1), m.group(2));
					}
				}
			} catch (Exception e) {
				logger.error(e.getMessage());
			} finally {
				try {
					if (br != null) {
						br.close();
					}
					if (ir != null) {
						ir.close();
					}

				} catch (IOException e) {
					// TODO Auto-generated catch block
					logger.error(e.getMessage());
				}
			}
		}

	}

	public static Map<String, String> getPatterns() {
		return patterns;
	}
}
