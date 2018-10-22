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
package com.dtstack.jlogstash.distributed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.jlogstash.distributed.util.CountUtil;
import com.dtstack.jlogstash.exception.ExceptionUtil;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年1月4日 下午1:16:37
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class DownReblance implements Runnable{
	
	
	private static final Logger logger = LoggerFactory.getLogger(DownReblance.class);

	private final static int INTERVAL = 2000;
	
	private ZkDistributed zkDistributed;
	
	private MasterCheck masterCheck;
	
    public DownReblance(ZkDistributed zkDistributed, MasterCheck masterCheck){
    	this.zkDistributed = zkDistributed;
    	this.masterCheck = masterCheck;
    }
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		int index=0;
		try{
			while(true){
				++index;
				Thread.sleep(INTERVAL);
				if(this.masterCheck.isMaster()){
					this.zkDistributed.downTracsitionReblance();
				}
				if(CountUtil.count(index, 5))logger.warn("DownReblance start again...");
			}
		}catch(Exception e){
			logger.error(ExceptionUtil.getErrorMessage(e));
		}
	}
}
