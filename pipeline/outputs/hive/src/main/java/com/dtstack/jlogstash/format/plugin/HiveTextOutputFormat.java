/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.jlogstash.format.plugin;

import com.dtstack.jlogstash.format.CompressEnum;
import com.dtstack.jlogstash.format.HiveOutputFormat;
import com.dtstack.jlogstash.format.util.HostUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormatBak;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;


/**
 * @author haisi
 */
@SuppressWarnings("serial")
public class HiveTextOutputFormat extends HiveOutputFormat {

	private static Logger logger = LoggerFactory
			.getLogger(HiveTextOutputFormat.class);

	private String delimiter;

	public HiveTextOutputFormat(Configuration conf, String outputFilePath,
								List<String> columnNames, List<String> columnTypes,
								String compress, String writeMode, Charset charset, String delimiter) {
		this.conf = conf;
		this.outputFilePath = outputFilePath;
		this.columnNames = columnNames;
		this.columnTypes = columnTypes;
		this.compress = compress;
		this.writeMode = writeMode;
		this.charset = charset;
		this.delimiter = delimiter;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void configure() {
		super.configure();
		outputFormat = new TextOutputFormatBak<>();
		Class<? extends CompressionCodec> codecClass = null;
		if (CompressEnum.NONE.name().equalsIgnoreCase(compress)) {
			codecClass = null;
		} else if (CompressEnum.GZIP.name().equalsIgnoreCase(compress)) {
			codecClass = org.apache.hadoop.io.compress.GzipCodec.class;
		} else if (CompressEnum.BZIP2.name().equalsIgnoreCase(compress)) {
			codecClass = org.apache.hadoop.io.compress.BZip2Codec.class;
		} else {
			throw new IllegalArgumentException("Unsupported compress format: "
					+ compress);
		}
		if (codecClass != null) {
			FileOutputFormat.setOutputCompressorClass(jobConf, codecClass);
		}
	}

	@Override
	public void open() throws IOException {
		String pathStr = null;
		if (outputFormat instanceof TextOutputFormatBak){
			 pathStr = String.format("%s/%s-%d.txt", outputFilePath, HostUtil.getHostName(), Thread.currentThread().getId());
		} else {
			 pathStr = String.format("%s/%s-%d-%s.txt", outputFilePath, HostUtil.getHostName(), Thread.currentThread().getId(), UUID.randomUUID().toString());
		}
		logger.info("hive path:{}", pathStr);
		// // 此处好像并没有什么卵用
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		String attempt = "attempt_" + dateFormat.format(new Date())
				+ "_0001_m_000000_" +Thread.currentThread().getId();
		jobConf.set("mapreduce.task.attempt.id", attempt);
		FileOutputFormat.setOutputPath(jobConf, new Path(pathStr));
		this.recordWriter = this.outputFormat.getRecordWriter(null, jobConf, pathStr, Reporter.NULL);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void writeRecord(Map<String, Object> row) throws Exception {
		String[] record = new String[this.columnSize];
		for (int i = 0; i < this.columnSize; i++) {
			Object obj = row.get(this.columnNames.get(i));
			if (obj == null) {
				record[i] = "";
			} else {
				if (obj instanceof Map) {
					record[i] = objectMapper.writeValueAsString(obj);
				} else {
					record[i] = obj.toString();
				}
			}
		}
		recordWriter.write(NullWritable.get(),
				new Text(StringUtils.join(delimiter, record)));
	}
}
