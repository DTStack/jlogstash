/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.jlogstash.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;


/**
 * @author haisi
 */
public abstract class HiveOutputFormat implements OutputFormat {

    protected static final String SP = "/";
    protected static final String DATA_SUBDIR = ".data";
    protected Charset charset;
    protected String writeMode;
    protected String compress;
    protected List<String> columnNames;
    protected int columnSize;
    protected List<String> columnTypes;
    protected String outputFilePath;
    protected FileOutputFormat<?, ?> outputFormat;
    protected JobConf jobConf;
    protected Configuration conf;
    protected RecordWriter recordWriter;
    protected volatile boolean isClosed = true;
    protected long lastRecordTime = System.currentTimeMillis();
    protected String fileName;
    protected String tmpPath;
    protected String finishedPath;


    public static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure() {
        columnSize = this.columnNames.size();
        jobConf = new JobConf(conf);
    }

    @Override
    public void writeRecord(Object[] record) throws Exception {
        lastRecordTime = System.currentTimeMillis();
        if (isClosed()) {
            open();
        }
    }

    @Override
    public void open() throws IOException {
        isClosed = false;
    }

    @Override
    public void close() throws IOException {
        RecordWriter<?, ?> rw = this.recordWriter;
        if (rw != null && !isClosed) {
            rw.close(Reporter.NULL);
        }
        isClosed = true;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public boolean isTimeout(TimePartitionFormat.PartitionEnum partitionEnum) {
        if (null == partitionEnum) {
            return false;
        } else if (TimePartitionFormat.PartitionEnum.DAY == partitionEnum) {
            return (System.currentTimeMillis() - lastRecordTime) >= 86400000 * 2;
        } else if (TimePartitionFormat.PartitionEnum.HOUR == partitionEnum) {
            return (System.currentTimeMillis() - lastRecordTime) >= 3600000 * 2;
        } else if (TimePartitionFormat.PartitionEnum.MINUTE == partitionEnum) {
            return (System.currentTimeMillis() - lastRecordTime) >= 60000 * 2;
        } else {
            return true;
        }
    }
}
