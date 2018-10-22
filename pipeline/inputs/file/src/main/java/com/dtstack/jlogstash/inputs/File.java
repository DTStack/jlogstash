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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.codehaus.plexus.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.decoder.IDecode;
import com.dtstack.jlogstash.decoder.JsonDecoder;
import com.dtstack.jlogstash.decoder.MultilineDecoder;
import com.dtstack.jlogstash.decoder.PlainDecoder;
import com.google.common.collect.Lists;

/**
 * Reason: jlogstash 文件类型的读入插件
 * Date: 2016年11月19日
 * Company: www.dtstack.com
 * @author xuchao
 *
 */
public class File extends BaseInput{
	
	private static final long serialVersionUID = -1822028651072758886L;

	private static final Logger logger = LoggerFactory.getLogger(File.class);
	
	private static String encoding = "UTF-8";
	
	private static Map<String, Object> pathcodecMap = null;

	/**指定文件的行的聚合规则key:文件名称, val:eg:multiline,json,plain*/
	private Map<String, IDecode> codecMap = new ConcurrentHashMap<String, IDecode>();
	
	@Required(required=true)
	private  static  List<String> path;
	
	private  static List<String> exclude;
	
	private  static int maxOpenFiles = 0;//0表示没有上限
		
	private  static String startPosition = "end";//one of ["beginning", "end"]
	
	/**key:文件夹路径, 匹配信息列表,  10s检测一次*/
	private Map<String, List<String>> moniDic = new ConcurrentHashMap<String,  List<String>>();
	
	/**文件当前读取位置点*/
	private ConcurrentHashMap<String, Long> fileCurrPos = new ConcurrentHashMap<String, Long>();
	
	private static String sinceDbPath = "./sincedb.yaml";
		
	private static int sinceDbWriteInterval = 15; //sincedb.yaml 更新频率(时间s)
	
	/**当读取设置行之后更新当前文件读取位置*/
	private static int readLineNum4UpdateMap = 1000;
	
	private static byte delimiter = '\n';
	
	private List<String> realPaths = Lists.newArrayList();
	
	/**默认值:cpu线程数*/
	public static int readFileThreadNum = -1;
		
	private Map<Integer, BlockingQueue<String>> threadReadFileMap = new ConcurrentHashMap<>();
		
	private ConcurrentHashMap<String, Long> monitorMap = new ConcurrentHashMap<String, Long>();
	
	private boolean runFlag = false;
	
	private ExecutorService executor;
	
	private ScheduledExecutorService scheduleExecutor;
	
	private ReentrantLock writeFileLock = new ReentrantLock();

	public File(Map config) {
		super(config);
	}
	
	public void init(){
		
		if(pathcodecMap == null || pathcodecMap.size() == 0){
			return;
		}
		
		if(path == null){
			path = Lists.newArrayList();
		}
		
		//设置codec
		for(Entry<String, Object> entry : pathcodecMap.entrySet()){
			String filePatternName = entry.getKey();
			Object codecInfo = entry.getValue();
			path.add(filePatternName);
			
			if(codecInfo instanceof String){
				if("json".equals(codecInfo)){
		            codecMap.put(filePatternName, new  JsonDecoder());
		        }else if("plain".equals(codecInfo)){
		        	codecMap.put(filePatternName, new PlainDecoder());
		        }else{
		        	logger.error("invalid codec type:{}. please check config!", codecInfo);
		        	System.exit(-1);
		        }
			}else if( codecInfo instanceof Map){
				IDecode multilineDecoder = createMultiLineDecoder((Map)codecInfo);
				codecMap.put(filePatternName, multilineDecoder);
			}else{
				logger.error("invalid codec type:{}, please check param of 'pathcodecMap'.", codecInfo.getClass());
				System.exit(-1);
			}
		}
	}

	@Override
	public void prepare() {
		
		init();
		
		if(path == null || path.size() == 0){
			logger.error("don't set any input file. [path, pathcodecMap] must not be empty at the same time.");
			System.exit(-1);
		}
		
		if(realPaths.size() == 0){
			List<String> ps = generateRealPath();
			realPaths.addAll(ps);
			
			if(maxOpenFiles > 0 && realPaths.size() > maxOpenFiles){
				logger.error("file numbers is exceed, maxOpenFiles is {}", maxOpenFiles);
				System.exit(-1);
			}
		}
		
		if(readFileThreadNum <= 0){
			readFileThreadNum = Runtime.getRuntime().availableProcessors();
		}
		
		checkoutSinceDb();
		filterFinishFile();
		ReadLineUtil.setDelimiter(delimiter);
		
		for(String fileStr : realPaths){
			addFile(fileStr);
		}
		
		runFlag = true;
	}
	
	/**
	 * 获取文件路径。支持精准路径、目录或者模糊匹配的文件。会过滤排除的文件。
	 * @return
	 */
	private List<String> generateRealPath(){
		
		List<String> ps = Lists.newArrayList();
		for(String p : path){
			
			if(p.contains("*") || p.contains("?")){//模糊匹配
				ps.addAll(getPatternFile(p));
				continue;
			}
			
			java.io.File file = new java.io.File(p);
			if(!file.exists()){
				logger.error("file:{} is not exists.", p);
				System.exit(-1);
			}
			
			if(file.isDirectory()){
				for(java.io.File tmpFile : file.listFiles()){
					ps.add(tmpFile.getPath());
				}
				
				addMonitorDic(p, null);
			}else{
				ps.add(file.getPath());
			}
		}
		
		Iterator<String> it = ps.iterator();
		for( ;it.hasNext();){
			String name = it.next();
			if(isExcludeFile(name)){
				it.remove();
			}
		}
		
		return ps;
	}
	
	/**
	 * 返回匹配正则的file列表。
	 * @param patternName
	 * @return
	 */
	private List<String> getPatternFile(String patternName){
		
		List<String> fileList = Lists.newArrayList(); 
		String dir = patternName.substring(0, patternName.lastIndexOf("/"));
		String filePattern = patternName.substring(patternName.lastIndexOf("/") + 1);
		java.io.File dirFile = new java.io.File(dir);
		if(!dirFile.isDirectory()){
			logger.info("don't exists dir in pattern:{}", patternName);
			return fileList;
		}
		
		addMonitorDic(dir, filePattern);
		
		for(java.io.File tmpFile : dirFile.listFiles()){
			
			if(tmpFile.isDirectory()){
				//FIXME 暂时不对指定文件夹下的子文件做模糊匹配,如果有需求在该处修改
				continue;
			}
			
			if(filePatternMatcher(filePattern, tmpFile.getName())){
				fileList.add(tmpFile.getPath());
			}
		}
		
		return fileList;
	}
	
	/**
	 * 把patternName的list存入moniDic，其key为dir。
	 * @param dir
	 * @param patternName
	 */
	public void addMonitorDic(String dir, String patternName){
		List<String> patternList = moniDic.get(dir);
		if(patternList == null){
			patternList = Lists.newArrayList();
			moniDic.put(dir, patternList);
		}
		
		if(patternName != null){
			patternList.add(patternName);
		}
		
	}
	
	/**
	 * 文件名正则匹配。
	 * @param pattern
	 * @param str
	 * @return
	 */
	private boolean filePatternMatcher(String pattern, String str) {
		
		pattern = pattern.replace("\\", "").replace("/", "");
		str = str.replace("\\", "").replace("/", "");
		
        int patternLength = pattern.length();
        int strLength = str.length();
        int strIndex = 0;
        char ch;
        for (int patternIndex = 0; patternIndex < patternLength; patternIndex++) {
            ch = pattern.charAt(patternIndex);
            if (ch == '*') {
                //通配符星号*表示可以匹配任意多个字符
                while (strIndex < strLength) {
                    if (filePatternMatcher(pattern.substring(patternIndex + 1),
                            str.substring(strIndex))) {
                        return true;
                    }
                    strIndex++;
                }
            } else if (ch == '?') {
                //通配符问号?表示匹配任意一个字符
                strIndex++;
                if (strIndex > strLength) {
                    //表示str中已经没有字符匹配?了。
                    return false;
                }
            } else {
                if ((strIndex >= strLength) || (ch != str.charAt(strIndex))) {
                    return false;
                }
                strIndex++;
            }
        }
        return (strIndex == strLength);
    }
		
	/**
	 * 过滤排除文件
	 * @param fileName
	 * @return
	 */
	public boolean isExcludeFile(String fileName){
		
		if(exclude == null){
			return false;
		}
		
		for(String patternName : exclude){
			if(filePatternMatcher(patternName, fileName)){
				return true;
			}
		}
		
		return false;
	}
	
	/**
	 * 把sincedb.yaml的内容存入fileCurrPos。
	 */
	private void checkoutSinceDb(){
		Yaml yaml = new Yaml();
		java.io.File sinceFile = new java.io.File(sinceDbPath);
		if(!sinceFile.exists()){
			return;
		}
		
		InputStream io = null;
		try {
			io = new FileInputStream(sinceFile);
			Map<String, Object> fileMap = yaml.loadAs(io, Map.class);
			if(fileMap == null){
				return;
			}

			for(Entry<String, Object> tmp : fileMap.entrySet()){
				if(tmp.getValue() == null){
					continue;
				}

				fileCurrPos.put(tmp.getKey(), Long.valueOf(tmp.getValue() + ""));
			}
		} catch (FileNotFoundException e) {
			logger.error("open file:{} err:{}!", sinceDbPath, e.getCause());
			System.exit(1);
		}finally{
			if(io != null){
				try {
					io.close();
				} catch (IOException e) {
					logger.error("", e);
				}
			}
		}
	}
	
	/**
	 * 过滤掉已经读取完成的文件
	 */
	private void filterFinishFile(){
		for(Entry<String, Long> entry : fileCurrPos.entrySet()){
			if(entry.getValue().longValue() == -1l){//表示该文件已经读取完成
				realPaths.remove(entry.getKey());
			}
		}
	}
	
	/**
	 * 使用替换的方式防止出现写不全的情况
	 */
	private void dumpSinceDb(){
		
		FileWriter fw = null;
		boolean isSuccess = false;
		String tmpSinceDbName = sinceDbPath + ".tmp";
		
		try{
			writeFileLock.lock();
			Yaml tmpYaml = new Yaml();
			fw = new FileWriter(tmpSinceDbName);
			tmpYaml.dump(fileCurrPos, fw);
			isSuccess = true;
		}catch(Exception e){
			logger.error("", e);
			logger.info("curr file pos:{}", fileCurrPos);
		}finally{
			try {
				fw.close();
			} catch (IOException e) {
				logger.error("", e);
			}
			
			writeFileLock.unlock();
		}
		
		if(!isSuccess){
			return;
		}
		
		java.io.File srcFile = new java.io.File(tmpSinceDbName);
		java.io.File dstFile = new java.io.File(sinceDbPath);
		try {
			FileUtils.rename(srcFile, dstFile);
		} catch (IOException e) {
			logger.error("", e);
		}
		
	}
	
	public void addFile(String fileName){
		int hashCode = Math.abs(fileName.hashCode());
		int index = hashCode % readFileThreadNum;
		BlockingQueue<String> readQueue = threadReadFileMap.get(index);
		
		if(readQueue == null){
			readQueue = new LinkedBlockingQueue<>();
			threadReadFileMap.put(index, readQueue);
		}
		
		readQueue.offer(fileName);
	}

	@Override
	public void emit() {
		executor = Executors.newFixedThreadPool(readFileThreadNum + 2);
		scheduleExecutor = Executors.newScheduledThreadPool(1);
		
		executor.submit(new MonitorChangeRunnable());
		executor.submit(new MonitorNewFileRunnable());
		for(int i=0; i<readFileThreadNum; i++){
			executor.submit(new FileRunnable(i));
		}
		
		scheduleExecutor.scheduleWithFixedDelay(new DumpSinceDbRunnable(), 
				sinceDbWriteInterval, sinceDbWriteInterval, TimeUnit.SECONDS);
	}

	@Override
	public void release() {
		//在正常关闭的时候记录offset
		executor.shutdownNow();
		scheduleExecutor.shutdownNow();
		dumpSinceDb();
	}
	
	public IDecode getDecoder(String fileName){
		for(Entry<String, IDecode> entry : codecMap.entrySet()){
			if(filePatternMatcher(entry.getKey(), fileName)){
				return entry.getValue();
			}
		}
		
		logger.info("can't find decoder from config. return default decoder.");
		return this.getDecoder();
	}
		
		
	class FileRunnable implements Runnable{
								
		private final int index;
		
		public FileRunnable(int index) {
			this.index = index;
		}

		public void run() {
			
			while(runFlag){
				
				BlockingQueue<String> needReadList = threadReadFileMap.get(index);
				if(needReadList == null){
					logger.warn("invalid FileRunnable thread, threadReadFileMap don't init needReadList of this index:{}.", index);
					return;
				}
				
				String readFileName = null;
				try {
					readFileName = needReadList.poll(10, TimeUnit.SECONDS);
					if(readFileName == null){
						continue;
					}
				} catch (InterruptedException e) {
					logger.error("", e);
					continue;
				}
					
				long lastModTime = 0l;	
				IReader reader = null;
				try {
					java.io.File readFile = new java.io.File(readFileName);
					if(!readFile.exists()){
						logger.error("file:{} is not exists!", readFileName);
						continue;
					}
					
					lastModTime = readFile.lastModified();
					reader = ReadFactory.createReader(readFile, encoding, fileCurrPos, startPosition);
					if(reader == null){
						continue;
					}
					
					String line = null;
					int readLineNum = 0;
					IDecode fileDecoder = getDecoder(readFileName);
					boolean isMultiLine = false;
					if(fileDecoder instanceof MultilineDecoder){
						isMultiLine = true;
					}
					
					while( (line = reader.readLine()) != null){
						readLineNum++;
						
						if(!"".equals(line.trim())){
							Map<String, Object> event = null;
							
							if(isMultiLine){
								event = fileDecoder.decode(line, readFileName);
							}else{
								event = fileDecoder.decode(line);
							}
							
							if (event != null && event.size() > 0){
								event.put("path", readFileName);
								event.put("offset", reader.getCurrBufPos());
								process(event);
							}
						}
						
						if(readLineNum%readLineNum4UpdateMap == 0){
							fileCurrPos.put(reader.getFileName(), reader.getCurrBufPos());
						}
					}
					
					fileCurrPos.put(readFileName, reader.getCurrBufPos());
				} catch (Exception e) {
					logger.error("", e);
				}finally{
					if(reader != null && reader.needMonitorChg()){
						monitorMap.put(readFileName, lastModTime);//确保文件回到监控列表
					}
				}
			}
		}
	}
	
	/**
	 * 监控文件变化,将有变化的文件插入到needReadList里
	 * 2s查看一次
	 * FIXME 需要对已经删除的文件做清理
	 * @author xuchao
	 *
	 */
	class MonitorChangeRunnable implements Runnable{

		public void run() {
			
			while(runFlag){
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					logger.error("", e);
				}
				
				Iterator<Entry<String, Long>> iterator = monitorMap.entrySet().iterator();
				for( ;iterator.hasNext(); ){
					Entry<String, Long> entry = iterator.next();
					java.io.File monitorFile = new java.io.File(entry.getKey());
					if(!monitorFile.exists()){
						logger.info("file:{} not exists,may be delete!", entry.getKey());
						continue;
					}
										
//					if(monitorFile.lastModified() > entry.getValue()){
//						iterator.remove();
//						addFile(entry.getKey());
//					}
					if(fileCurrPos.get(entry.getKey())<monitorFile.length()){
						iterator.remove();
						addFile(entry.getKey());
					}	
				}
				
			}
		}
		
	}
	
	/** 
	 * 监控新出现的符合条件的文件,并加入到文件读取列表里
	 * @author xuchao
	 *
	 */
	class MonitorNewFileRunnable implements Runnable{

		public void run() {
			
			while(runFlag){
				
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					logger.error("", e);
				}
				
				for(Entry<String, List<String>> dirTmp : moniDic.entrySet()){
					
					String dicName = dirTmp.getKey();
					List<String> patternList = dirTmp.getValue();
					
					java.io.File file = new java.io.File(dicName);					
					
					if(!file.exists() || !file.isDirectory()){
						continue;
					}
					
					for(java.io.File tmpFile : file.listFiles()){
						
						if(tmpFile.isDirectory()){//FIXME 不监控新出现的子文件夹
							continue;
						}
						
						if(isExcludeFile(tmpFile.getPath())){
							continue;
						}
						
						if(fileCurrPos.get(tmpFile.getPath()) != null && fileCurrPos.get(tmpFile.getPath()).longValue() == -1l){
							continue;//已经完结的数据
						}
						
						if(patternList.size() == 0 && !realPaths.contains(tmpFile.getPath())){
							realPaths.add(tmpFile.getPath());
							addFile(tmpFile.getPath());
							continue;
						}
						
						for(String patternName : patternList){
							if(filePatternMatcher(patternName, tmpFile.getName()) && !realPaths.contains(tmpFile.getPath())){
								realPaths.add(tmpFile.getPath());
								addFile(tmpFile.getPath());
								break;
							}
						}
					}
				}
			}
		}
		
	}
	
	class DumpSinceDbRunnable implements Runnable{

		public void run() {
			dumpSinceDb();
		}
		
	}
}
