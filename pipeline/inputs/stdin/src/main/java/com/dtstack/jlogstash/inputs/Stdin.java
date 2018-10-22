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
package com.dtstack.jlogstash.inputs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:17:27
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class Stdin extends BaseInput {
    private static final Logger logger = LoggerFactory.getLogger(Stdin.class);

    public Stdin(Map<String, Object> config){
        super(config);
    }

    @Override
    public void prepare() {
    }

    public void emit() {
        try {
            BufferedReader br =
                    new BufferedReader(new InputStreamReader(System.in));

            String input;

            while ((input = br.readLine()) != null) {
                try {
                    Map<String, Object> event = this.getDecoder()
                            .decode(input);
                    this.process(event);
                } catch (Exception e) {
                    logger.error("{}:process event failed:{}",input,e.getCause());
                }
            }
        } catch (IOException io) {
            logger.error("Stdin loop got exception:{}",io.getCause());
        }
    }

	@Override
	public void release() {
		// TODO Auto-generated method stub
		
	}
}
