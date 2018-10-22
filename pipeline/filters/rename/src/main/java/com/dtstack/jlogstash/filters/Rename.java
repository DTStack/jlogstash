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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import com.dtstack.jlogstash.annotation.Required;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:54:32
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class Rename extends BaseFilter {
	public Rename(Map config) {
		super(config);
	}

	@Required(required=true)
	private static Map<String, String> fields;

	public void prepare() {
	}

	@Override
	protected Map filter(Map event) {
		if(fields!=null){
			Iterator<Entry<String, String>> it = fields.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, String> entry = it.next();
				String oldname = entry.getKey();
				String newname = entry.getValue();
				if (event.containsKey(oldname)) {
					event.put(newname, event.remove(oldname));
				}
			}
		}
		return event;
	}
}
