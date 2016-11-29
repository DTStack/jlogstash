package com.dtstack.logstash.utils;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年11月28日 下午1:27:03
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
import java.math.BigDecimal;
import java.net.InetAddress;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Public {

	private static final Logger publicLogger = LoggerFactory
			.getLogger(Public.class);

	public static String getHostNameForLiunx() {
		try {
			return (InetAddress.getLocalHost()).getHostName();
		} catch (Exception uhe) {
			String host = uhe.getMessage(); // host = "hostname: hostname"
			if (host != null) {
				int colon = host.indexOf(':');
				if (colon > 0) {
					return host.substring(0, colon);
				}
			}
			publicLogger.error("getHostName:{}", uhe.getCause());
			return "UnknownHost";
		}
	}

	public static String getHostName() {
		if (System.getenv("COMPUTERNAME") != null) {
			return System.getenv("COMPUTERNAME");
		} else {
			return getHostNameForLiunx();
		}
	}

	public static String getHostAddress() {
		try {
			InetAddress ia = InetAddress.getLocalHost();
			return ia.getHostAddress();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			publicLogger.error("getHostAddress:{}", e.getCause());
		}
		return "127.0.0.1";
	}

	public static String getTimeStamp() {
		return DateTime.now().toString();
	}

	public static int getIntValue(double d) {
		return new BigDecimal(d).setScale(0, BigDecimal.ROUND_HALF_UP)
				.intValue();
	}
}
