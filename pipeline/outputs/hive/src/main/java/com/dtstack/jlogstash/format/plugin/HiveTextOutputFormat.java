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
import com.dtstack.jlogstash.format.util.DateUtil;
import com.dtstack.jlogstash.format.util.HostUtil;
import com.dtstack.jlogstash.utils.ExceptionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormatBak;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
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
		super.open();
		if (outputFormat instanceof TextOutputFormatBak){
			fileName = String.format("%s-%d.txt", HostUtil.getHostName(), Thread.currentThread().getId());
		} else {
			fileName = String.format("%s-%d-%s.txt", HostUtil.getHostName(), Thread.currentThread().getId(), UUID.randomUUID().toString());
		}
		tmpPath = String.format("%s/%s/%s", outputFilePath, DATA_SUBDIR, fileName);
		finishedPath = String.format("%s/%s", outputFilePath, fileName);
		logger.info("hive tmpPath:{}", tmpPath);
		// // 此处好像并没有什么卵用
		String attempt = "attempt_" + DateUtil.getUnstandardFormatter().format(new Date())
				+ "_0001_m_000000_" +Thread.currentThread().getId();
		jobConf.set("mapreduce.task.attempt.id", attempt);
		FileOutputFormat.setOutputPath(jobConf, new Path(tmpPath));
		this.recordWriter = this.outputFormat.getRecordWriter(null, jobConf, tmpPath, Reporter.NULL);
	}

	@Override
	public Object[] convert2Record(Map<String, Object> row) throws Exception {
		String[] record = new String[this.columnSize];
		for (int i = 0; i < this.columnSize; i++) {
			Object fieldData = row.get(this.columnNames.get(i));
			try {
				if (fieldData == null) {
					record[i] = "";
				} else {
					if (fieldData instanceof Map) {
						record[i] = objectMapper.writeValueAsString(fieldData);
					} else {
						record[i] = fieldData.toString();
					}
				}
			} catch (Exception e) {
				throw new Exception("field convert error:"+ ExceptionUtil.getStackTrace(e)+", columnName=" + this.columnNames.get(i) +
						", fieldData=" + fieldData, e);
			}
		}
		return record;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void writeRecord(Object[] record) throws Exception {
		super.writeRecord(record);
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (Object s : record) {
			if (first) {
				first = false;
			} else {
				sb.append(delimiter);
			}
			sb.append(s.toString());
		}
		recordWriter.write(NullWritable.get(), new Text(sb.toString()));
	}
}
