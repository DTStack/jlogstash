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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 读取tar文件信息
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年11月24日
 * Company: www.dtstack.com
 * @author xuchao
 *
 */
public class ReadTarFile implements IReader {
	
	private static final Logger logger = LoggerFactory.getLogger(ReadTarFile.class);
		
	private BufferedReader currBuff;
	
	private TarArchiveInputStream tarAchive;
	
	private FileInputStream fins;
	
	private String currFileName;
	
	private int currFileSize = 0;
	
	private String tarFileName = "";
	
	private String encoding = "UTF-8"; 
	
	private boolean readEnd = false;
	
	private Map<String, Long> fileCurrPos;
	
	private ReadTarFile(String fileName, String encoding, Map<String, Long> fileCurrPos){
		this.tarFileName = fileName;
		this.fileCurrPos = fileCurrPos;
		this.encoding = encoding;
	}
	
	public static ReadTarFile createInstance(String fileName, String encoding, ConcurrentHashMap<String, Long> fileCurrPos){
		ReadTarFile readTar = new ReadTarFile(fileName, encoding, fileCurrPos);
		if(readTar.init()){
			return readTar;
		}
		
		return null;
	}
	
	public boolean init(){
		if (!tarFileName.toLowerCase().endsWith(".tar") && !tarFileName.toLowerCase().endsWith(".tar.gz")) {
			logger.error("file:{} is not a tar file.", tarFileName);
			return false;
		}
		
		try{
			
			fins = new FileInputStream(tarFileName);
			if(tarFileName.endsWith("tar.gz")){
				CompressorInputStream in = new GzipCompressorInputStream(fins, true);
				tarAchive = new TarArchiveInputStream(in);
			}else{
				tarAchive = new TarArchiveInputStream(fins);
			}
			getNextBuffer();
		}catch(Exception e){
			logger.error("", e);
			return false;
		}
		
		return true;
	}
		
	public BufferedReader getNextBuffer(){
		
		if(currBuff != null){
			//不能关闭currBuffer否则得重新创建inputstream
			currBuff = null;
		}
		
		TarArchiveEntry entry = null;
		
		try{
			while((entry = tarAchive.getNextTarEntry()) != null){
				if(entry.isDirectory()){
					continue;
				}
				
				currFileName = entry.getName();
				currFileSize = (int) entry.getSize();
				String identify = getIdentify(currFileName);
				long skipNum = getSkipNum(identify);
				if(skipNum >= entry.getSize()){
					continue;
				}
				
				tarAchive.skip(skipNum);
				currBuff = new BufferedReader(new InputStreamReader(tarAchive, encoding));
				break;
			}
			
		}catch(Exception e){
			logger.error("", e);
		}
		
		if(currBuff == null){
			try {
				doAfterReaderOver();
			} catch (IOException e) {
				logger.error("", e);
			}
		}
		
		return currBuff;
	}
	
	/**
	 * 清理之前记录的zip文件的子文件信息eg: e:\\d\mydata.tar|mydata/aa.log
	 * @throws IOException 
	 */
	public void doAfterReaderOver() throws IOException{
		
		tarAchive.close();
		fins.close();
		
		Iterator<Entry<String, Long>> it = fileCurrPos.entrySet().iterator();
		String preFix = tarFileName + "|";
		while(it.hasNext()){
			Entry<String, Long> entry = it.next();
			if(entry.getKey().startsWith(preFix)){
				it.remove();
			}
		}
		
		//重新插入一条表示zip包读取完成的信息
		fileCurrPos.put(tarFileName, -1l);
		readEnd = true;
	}

	@Override
	public String readLine() throws IOException {
		
		if(currBuff == null){
			return null;
		}
		
		String str = currBuff.readLine();
		if(str == null){
			BufferedReader buff = getNextBuffer();
			if(buff != null){
				str = currBuff.readLine();
			}
		}
		return str;
	}

	@Override
	public String getFileName() {
		
		if(currFileName == null){
			return tarFileName;
		}
		
		return getIdentify(currFileName);
	}

	@Override
	public long getCurrBufPos() {

		if(readEnd){
			return -1;
		}
		
		int available = 0;
		try {
			available = tarAchive.available();
		} catch (IOException e) {
			logger.error("error:", e);
			return 0;
		}
		
		return currFileSize - available;
	
	}
	
	private String getIdentify(String fileName){
		return tarFileName + "|" + fileName; 
	}
	
	private long getSkipNum(String identify){
		Long skipNum = fileCurrPos.get(identify);
		skipNum = skipNum == null ? 0 : skipNum;
		return skipNum;
	}
	    
    @Override
	public boolean needMonitorChg() {
		return false;
	}
}
