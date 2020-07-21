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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * RandomAccessFile 方式按行读取文件
 * 2020-7-17 15:51:39 当文件大小过大时，MappedByteBuffer分段读取
 *
 * @author xuchao
 *
 */
public class ReadLineUtil implements IReader{

	private static final Logger logger = LoggerFactory.getLogger(ReadLineUtil.class);

	private static byte lineBreak = '\n';
	private FileChannel channel;
	private String encoding;
	private long fileLength;
	private ByteBuffer buf ;
	private int bufPos = 0;
	private ByteArrayOutputStream baos = new ByteArrayOutputStream();
	private RandomAccessFile raf;
	/**
	 * 文件总大小
	 */
	private long readSize = 0;
	/**
	 * 已读取文件大小
	 */
	private long completedSize = 0;
	/**
	 * 当前读取批次大小
	 */
	private long currentReadSize = 0;
	/**
	 * 单个批次最大读取大小(默认1G)，不能超过2G
	 */
	private long maxReadSizeOnce = 1L << 30;
	/**
	 * 跳过读取大小
	 */
	private long skipNum = 0;
	private String fileName = "";

	public ReadLineUtil(File file, String encoding, long pos)
			throws IOException {
		this.fileName = file.getPath();
		raf = new RandomAccessFile(file, "r");
		init(encoding, pos);
	}

	public ReadLineUtil(File file, String encoding, String startPos) throws IOException{
		raf = new RandomAccessFile(file, "r");
		this.fileName = file.getPath();
		if("beginning".equalsIgnoreCase(startPos)){
			init(encoding, 0);
		}else{
			init(encoding, raf.length());
		}
	}

	private void init(String encoding, long pos) throws IOException{
		channel = raf.getChannel();
		fileLength = raf.length();
		this.encoding = encoding;
		this.skipNum = pos;
		readSize = fileLength - this.skipNum;
		// 文件剩余大小大于单批次最大则读单批次大小
		currentReadSize = readSize > maxReadSizeOnce ? maxReadSizeOnce : readSize;
		buf = channel.map(FileChannel.MapMode.READ_ONLY, this.skipNum, currentReadSize);
	}

	@Override
	public String readLine() throws Exception{
		while (bufPos <= currentReadSize) {
			//FIXME if the file size max then Integer.Max,it is err
			if (bufPos == currentReadSize) {
				// 根据剩余大小设置下一个批次大小
				currentReadSize = readSize - completedSize > maxReadSizeOnce ? maxReadSizeOnce :
						readSize - completedSize;
				if (currentReadSize == 0) {
					// 文件读取完成
					break;
				}
				// 重置MappedByteBuffer
				buf = channel.map(FileChannel.MapMode.READ_ONLY, this.skipNum + completedSize, currentReadSize);
				bufPos = 0;
			}
			byte c = buf.get(bufPos);
			bufPos++;
			if (c == '\r' || c == lineBreak) {
				if (c != lineBreak) {
					continue;
				}
				return bufToString();
			}
			baos.write(c);
		}

		if(baos.size() != 0){
			return bufToString();
		}

		doAfterReaderOver();
		return null;
	}

	@Override
	public void doAfterReaderOver(){
		try {
			channel.close();
			raf.close();
		} catch (IOException e) {
			logger.error("", e);
		}
	}

	private String bufToString() throws UnsupportedEncodingException {
		if (baos.size() == 0) {
			return "";
		}

		byte[] bytes = baos.toByteArray();

		baos.reset();
		return new String(bytes, encoding);
	}

	public static void setDelimiter(byte lineBreak){
		ReadLineUtil.lineBreak = lineBreak;
	}

	@Override
	public long getCurrBufPos(){
		return  skipNum + completedSize;
	}

	@Override
	public String getFileName() {
		return fileName;
	}

	@Override
	public boolean needMonitorChg() {
		return true;
	}
}
