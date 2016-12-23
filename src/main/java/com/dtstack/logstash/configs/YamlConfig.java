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
package com.dtstack.logstash.configs;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:25:45
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("rawtypes")
public class YamlConfig implements Config{
    private static final String HTTP = "http://";
    private static final String HTTPS = "https://";
    private static Logger logger = LoggerFactory.getLogger(YamlConfig.class);

    @Override
    public Map parse(String filename){
    	try{
            Yaml yaml = new Yaml();
            if (filename.startsWith(YamlConfig.HTTP) || filename.startsWith(YamlConfig.HTTPS)) {
                URL httpUrl;
                URLConnection connection;
                httpUrl = new URL(filename);
                connection = httpUrl.openConnection();
                connection.connect();
                return (Map) yaml.load(connection.getInputStream());
            } else {
                FileInputStream input = new FileInputStream(new File(filename));
                return (Map) yaml.load(input);
            }
    	}catch(Exception e){
    		logger.error("load yaml config error", e);
    		System.exit(1);
    	}
       return null;
    }
}
