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

import com.dtstack.jlogstash.annotation.Required;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 *
 * Reason: TODO ADD REASON(可选)
 * Date: 2018年9月3日 下午20:10:00
 * Company: www.dtstack.com
 * @author jiangbo
 *
 */
public class SplitStr extends BaseFilter {

    private static final Logger logger = LoggerFactory.getLogger(SplitStr.class);

    @Required(required=true)
    private static  List<Map<String,Object>> parseConfig;

    private static final String DELIM_KEY = "delim";

    private static final String KEYS_KEY = "cols";

    private static final String CONTENT_KEY = "textKey";

    private static final String FILE_NAME_KEY = "signKey";

    private static final String FILE_NAME_VAL_KEY = "signVal";

    public SplitStr(Map config) {
        super(config);
    }

    @Override
    public void prepare() {

    }

    @Override
    protected Map filter(Map event) {
        for (Map<String, Object> conf : parseConfig) {
            try{
                if (event.get(conf.get(FILE_NAME_KEY)).equals(conf.get(FILE_NAME_VAL_KEY))){
                    List<String> keys = (List<String>) conf.get(KEYS_KEY);
                    String[] items = event.get(conf.get(CONTENT_KEY)).toString().split(conf.get(DELIM_KEY).toString());
                    int size = keys.size();
                    for (int i = 0; i < size; i++) {
                        if(i < items.length){
                            event.put(keys.get(i),items[i]);
                        } else {
                            event.put(keys.get(i),null);
                        }
                    }
                }
            }catch (Exception e){
                logger.error("parse data:" + event);
                logger.error("",e);
            }
        }
        return event;
    }

}
