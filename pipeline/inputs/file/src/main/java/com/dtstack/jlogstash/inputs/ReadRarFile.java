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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.junrar.Archive;
import com.github.junrar.rarfile.FileHeader;


/**
 * 
 * Date: 2016年11月19日
 * Company: www.dtstack.com
 * @author xuchao
 *
 */
public class ReadRarFile implements IReader{
	
	private static final Logger logger = LoggerFactory.getLogger(ReadRarFile.class);
	
	private ConcurrentHashMap<String, Long> fileCurrPos  = new ConcurrentHashMap<String, Long>();
	
	private Archive archive;
		
	private BufferedReader currBuff;
	
	private InputStream currIns;
	
	private String currFileName;
	
	private long currFileSize = 0;
	
	public boolean readEnd = false;
	
	private String rarFileName = "";
	
	private String encoding = "UTF-8"; 
	
	public static ReadRarFile createInstance(String fileName, String encoding, ConcurrentHashMap<String, Long> fileCurrPos){
		ReadRarFile readRarFile = new ReadRarFile(fileName, encoding, fileCurrPos);
		if(readRarFile.init()){
			return readRarFile;
		}
		
		return null;
	}
		
	private ReadRarFile(String fileName, String encoding, ConcurrentHashMap<String, Long> fileCurrPos){
		this.fileCurrPos = fileCurrPos;
		this.rarFileName = fileName;
		this.encoding = encoding;
	}
	
	public long getRarSkipNum(String assignName){
		Long skipNum = fileCurrPos.get(assignName);
		skipNum = skipNum == null ? 0 : skipNum;
		return skipNum;
	}
	
	private boolean init(){
		if (!rarFileName.toLowerCase().endsWith(".rar")) {
			logger.error("not rar file, fileName:{}!", rarFileName);
			return false;
		}

		try {
			archive = new Archive(new File(rarFileName));
		} catch(Exception e){
			logger.error("", e);
			return false;
		}
		getNextBuffer();
		
		return true;
	}
	
	private String getIdentify(String fileName){
		return  rarFileName + "|" + fileName; 
	}
	
	public BufferedReader getNextBuffer(){
		
		if(currBuff != null){
			try {
				currBuff.close();
			} catch (IOException e) {
				logger.error("", e);
			}
			currBuff = null;
		}
		
		if(currIns != null){
			try {
				currIns.close();
			} catch (IOException e) {
				logger.error("", e);
			}
			currIns = null;
			flushPos();
		}
		
		currFileSize = 0;
		currFileName = null;
		
		FileHeader fh = archive.nextFileHeader();
		while (fh != null) {
			if (!fh.isDirectory()) {// 文件夹

				try {//之所以这么写try，是因为万一这里面有了异常，不影响继续解压.

					InputStream ins = archive.getInputStream(fh);
					currFileName = fh.getFileNameString();
					String identify = getIdentify(currFileName);
					long skipNum = getRarSkipNum(identify);
					currFileSize = fh.getDataSize();
					
					if(currFileSize <= skipNum){
						fh = archive.nextFileHeader();
						continue;
					}
					
					ins.skip(skipNum);
					currBuff = new BufferedReader(new InputStreamReader(ins, encoding));
					break;
				} catch (Exception e) {
					logger.error("", e);
				}

			}
			fh = archive.nextFileHeader();
		}
		
		if(currBuff == null){//释放资源
			try {
				doAfterReaderOver();
			} catch (IOException e) {
				logger.error("", e);
			}
		}
		
		return currBuff;
	}
	

	public void doAfterReaderOver() throws IOException{
		
		archive.close();
		
		Iterator<Entry<String, Long>> it = fileCurrPos.entrySet().iterator();
		String preFix = rarFileName + "|";
		while(it.hasNext()){
			Entry<String, Long> entry = it.next();
			if(entry.getKey().startsWith(preFix)){
				it.remove();
			}
		}
		
		//重新插入一条表示zip包读取完成的信息
		fileCurrPos.put(rarFileName, -1l);
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
			return rarFileName;
		}
		return currFileName;
	}

	@Override
	public long getCurrBufPos() {
		
		if(readEnd){
			return -1;
		}
		
		int available = 0;
		try {
			available = currIns.available();
		} catch (IOException e) {
			logger.error("error:", e);
			return 0;
		}
		
		return currFileSize - available;
	}
	
	/**
	 * 每个子文件写完的时候都更新一次
	 */
	public void flushPos(){
		if(currFileName == null){
			logger.error("invalid state, may have set currFileName null at not right place.");
			return;
		}
		fileCurrPos.put(getIdentify(currFileName), currFileSize);
	}
	
	@Override
	public boolean needMonitorChg() {
		return false;
	}	
}
