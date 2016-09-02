package com.dtstack.logstash.date;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:28:33
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class ISODateParser implements DateParser {

	private DateTimeFormatter formatter;

	public ISODateParser(String timezone) {

		this.formatter = ISODateTimeFormat.dateTimeParser();

		if (timezone != null) {
			this.formatter = this.formatter.withZone(DateTimeZone
					.forID(timezone));
		} else {
			this.formatter = this.formatter.withOffsetParsed();
		}
	}

	@Override
	public DateTime parse(String input) {
		return this.formatter.parseDateTime(input);
	}
}
