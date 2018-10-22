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
import java.util.concurrent.ConcurrentHashMap;

/**
 * 暂时只支持.rar,.zip,.tar.gz,.tar,普通文本
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年11月22日
 * Company: www.dtstack.com
 * @author xuchao
 *
 */
public class ReadFactory {
	
	public static IReader createReader(java.io.File file, String encoding, ConcurrentHashMap<String, Long> fileCurrPos
			, String startPos) throws IOException{
		
		IReader reader = null;
		String fileName = file.getPath();
		if(fileName.toLowerCase().endsWith(".zip")){
			reader = ReadZipFile.createInstance(fileName, encoding, fileCurrPos);
		}else if(fileName.toLowerCase().endsWith(".rar")){
			reader = ReadRarFile.createInstance(fileName, encoding, fileCurrPos);
		}else if(fileName.toLowerCase().endsWith(".tar.gz") || fileName.toLowerCase().endsWith(".tar")){
			reader = ReadTarFile.createInstance(fileName, encoding, fileCurrPos);
		}else{
			Long filePos = fileCurrPos.get(fileName);
			
			if(filePos == null){//未读取过的根据配置的读取点开始
				reader = new ReadLineUtil(file, encoding, startPos);
			}else{
				reader = new ReadLineUtil(file, encoding, filePos);
			}
		}
		
		return reader;
	}
	
	

}
