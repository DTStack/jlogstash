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
