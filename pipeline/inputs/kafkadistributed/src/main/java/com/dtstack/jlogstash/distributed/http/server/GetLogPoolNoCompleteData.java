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
package com.dtstack.jlogstash.distributed.http.server;

import com.dtstack.jlogstash.distributed.ZkDistributed;
import com.dtstack.jlogstash.distributed.http.server.callback.ApiCallback;
import com.dtstack.jlogstash.distributed.http.server.callback.ApiCallbackMethod;
import com.dtstack.jlogstash.distributed.http.server.callback.ApiResult;
import com.dtstack.jlogstash.distributed.logmerge.LogPool;
import com.sun.net.httpserver.HttpExchange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by sishu.yss on 2017/1/13.
 */
public class GetLogPoolNoCompleteData extends PostHandler{

    private final static Logger logger = LoggerFactory
            .getLogger(ImmediatelyLoadNodeDataHandler.class);


    private ZkDistributed zkDistributed;

    public GetLogPoolNoCompleteData(ZkDistributed zkDistributed){
        this.zkDistributed = zkDistributed;
    }

    @Override
    public void handle(HttpExchange he) throws IOException {
        // TODO Auto-generated method stub

        ApiCallbackMethod.doCallback(new ApiCallback(){
            @Override
            public void execute(ApiResult apiResult) throws Exception {
                // TODO Auto-generated method stub
                apiResult.setData(LogPool.getInstance().getNotCompleteLog());
            }
        }, he);

    }
}
