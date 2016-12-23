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
package com.dtstack.logstash.date;

import java.util.Locale;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:28:28
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class FormatParser implements DateParser {

	private DateTimeFormatter formatter;

	public FormatParser(String format, String timezone, String locale) {
		this.formatter = DateTimeFormat.forPattern(format);

		if (timezone != null) {
			this.formatter = this.formatter.withZone(DateTimeZone
					.forID(timezone));
		} else {
			this.formatter = this.formatter.withOffsetParsed();
		}

		if (locale != null) {
			this.formatter = this.formatter.withLocale(Locale
					.forLanguageTag(locale));
		}
	}

	@Override
	public DateTime parse(String input) {
		return this.formatter.parseDateTime(input);
	}
}
