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

import java.io.IOException;

/**
 * 文件读取接口
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年11月25日
 * Company: www.dtstack.com
 * @author xuchao
 *
 */
public interface IReader {
	
	/**
	 * 读取文件一行,返回null表示读文件结束
	 * @return
	 * @throws IOException
	 * @throws Exception 
	 */
	public String readLine() throws Exception;
	
	/**
	 * 获取当前读文件名
	 * @return
	 */
	public String getFileName();
	
	/**
	 * 获取当前文件读取位置(byte)
	 * @return
	 */
	public long getCurrBufPos();
	
	/***
	 * 是否需监控文件内容变化
	 * @return
	 */
	public boolean needMonitorChg();
	
	/**
	 * 文件读取完需要处理的内容
	 * @throws IOException
	 */
	public void doAfterReaderOver() throws IOException;

}
