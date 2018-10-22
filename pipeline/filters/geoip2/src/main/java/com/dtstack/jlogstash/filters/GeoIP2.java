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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:53:07
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class GeoIP2 extends BaseFilter {
	private static final Logger logger = LoggerFactory.getLogger(GeoIP2.class);

	public GeoIP2(Map config) {
		super(config);
	}

	private String source;
	private String target;
	private DatabaseReader reader;

	public void prepare() {
		if (!config.containsKey("source")) {
			logger.error("no source configured in GeoIP");
			System.exit(1);
		}
		this.source = (String) config.get("source");

		if (config.containsKey("target")) {
			this.target = (String) config.get("target");
		} else {
			this.target = "geoip";
		}

		if (config.containsKey("tagOnFailure")) {
			this.tagOnFailure = (String) config.get("tagOnFailure");
		} else {
			this.tagOnFailure = "geoipfail";
		}

		// A File object pointing to your GeoIP2 or GeoLite2 database
		if (!config.containsKey("database")) {
			logger.error("no database configured in GeoIP");
			System.exit(1);
		}
		File database = new File((String) config.get("database"));

		// This creates the DatabaseReader object, which should be reused
		// across
		// lookups.
		try {
			this.reader = new DatabaseReader.Builder(database).build();
		} catch (IOException e) {
			logger.error("failed to prepare DatabaseReader for geoip");
			logger.error("",e);
			System.exit(1);
		}
	};

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected Map filter(final Map event) {
		if (event.containsKey(this.source)) {

			boolean success = true;

			InetAddress ipAddress;
			try {
				ipAddress = InetAddress.getByName((String) event.get(source));

				// Replace "city" with the appropriate method for your database,
				// e.g.,
				// "country".
				CityResponse response = reader.city(ipAddress);

				Country country = response.getCountry();

				// Subdivision subdivision =
				response.getMostSpecificSubdivision();
				// Subdivision subdivision =
				// response.getMostSpecificSubdivision();
				// System.out.println(subdivision.getName());
				// System.out.println(subdivision.getIsoCode());

				City city = response.getCity();

				// Postal postal = response.getPostal();
				// System.out.println(postal.getCode());

				Location location = response.getLocation();

				Map targetObj = new HashMap();
				event.put(this.target, targetObj);
				targetObj.put("country_code", country.getIsoCode());
				targetObj.put("country_name", country.getName());
				targetObj.put("city_name", city.getName());
				double latitude = location.getLatitude();
				double longitude = location.getLongitude();
				targetObj.put("latitude", latitude);
				targetObj.put("longitude", longitude);
				targetObj.put("location", new double[] { longitude, latitude });
			} catch (Exception e) {
				logger.debug("",e);
				success = false;
			}
		}

		return event;
	}
}
