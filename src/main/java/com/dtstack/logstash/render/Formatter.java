package com.dtstack.logstash.render;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:27:43
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class Formatter {
	private static Pattern p = Pattern.compile("(\\%\\{.*?\\})");
	private static DateTimeFormatter ISOformatter = ISODateTimeFormat
			.dateTimeParser().withOffsetParsed();

	public Formatter() {
	}

	public static String format(Map event, String format) {
      return format(event,format,"UTC");
	}
	
	public static String format(Map event, String format, String Timezone) {
		Matcher m = p.matcher(format);
		StringBuffer sb = new StringBuffer();
		while (m.find()) {
			String match = m.group();
			String key = (String) match.subSequence(2, match.length() - 1);
			if (key.equalsIgnoreCase("+s")) {
				Object o = event.get("@timestamp");
				if (o.getClass() == Long.class) {
					m.appendReplacement(sb, o.toString());
				}
			} else if (key.startsWith("+")) {
				DateTimeFormatter formatter = DateTimeFormat.forPattern(
						(String) key.subSequence(1, key.length())).withZone(
						DateTimeZone.forID(Timezone));
				Object o = event.get("@timestamp");
				if (o == null) {
					DateTime timestamp = new DateTime();
					m.appendReplacement(sb, timestamp.toString(formatter));
				} else {
					if (o.getClass() == DateTime.class) {
						m.appendReplacement(sb,
								((DateTime) o).toString(formatter));
					} else if (o.getClass() == Long.class) {
						DateTime timestamp = new DateTime((Long) o);
						m.appendReplacement(sb, timestamp.toString(formatter));
					} else if (o.getClass() == String.class) {
						DateTime timestamp = ISOformatter
								.parseDateTime((String) o);
						m.appendReplacement(sb, timestamp.toString(formatter));
					}
				}
			} else if (event.containsKey(key)) {
				m.appendReplacement(sb, event.get(key).toString());
			}

		}
		m.appendTail(sb);
		return sb.toString();
	}
}