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
package com.dtstack.jlogstash.distributed.http.server.callback;

import java.io.OutputStream;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.jlogstash.exception.ExceptionUtil;
import com.sun.net.httpserver.HttpExchange;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年12月30日 下午1:16:37
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("restriction")
public class ApiCallbackMethod {
	private final static Logger logger = LoggerFactory
			.getLogger(ApiCallbackMethod.class);
	
	private static ObjectMapper objectMapper = new ObjectMapper();

	public static void doCallback(ApiCallback ac,HttpExchange he) {
		 OutputStream os = null;
		 ApiResult apiResult = new ApiResult();
		try {
			os = he.getResponseBody();
			long start = System.currentTimeMillis();
			ac.execute(apiResult);
			apiResult.setCode(200);
			long end = System.currentTimeMillis();
			apiResult.setSpace(end - start);
		} catch (Throwable e) {
			apiResult.serverError();
			logger.error(ExceptionUtil.getErrorMessage(e));
		}finally{
			if(os!=null)
				try {
					byte[] result =objectMapper.writeValueAsBytes(apiResult);
					he.sendResponseHeaders(200, result.length);
					os.write(result);
					os.close();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					logger.error(ExceptionUtil.getErrorMessage(e));
				}
		}
	}
}
