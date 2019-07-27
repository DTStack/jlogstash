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

package com.dtstack.jlogstash.format.dirty;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.jlogstash.format.TableInfo;
import com.dtstack.jlogstash.format.util.DateUtil;
import com.dtstack.jlogstash.format.util.HostUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormatBak;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

public class DirtyDataManager {

    private static Logger logger = LoggerFactory.getLogger(DirtyDataManager.class);

    private TableInfo tableInfo;
    private Configuration config;
    private JobConf jobConf;
    private FileOutputFormat outputFormat;
    private RecordWriter recordWriter;


    public DirtyDataManager(TableInfo tableInfo, Configuration configuration) {
        this.tableInfo = tableInfo;
        this.config = configuration;
        this.jobConf = new JobConf(config);
        outputFormat = new TextOutputFormatBak<>();
    }

    public void writeData(Map row, Throwable ex) {
        try {
            String content = JSONObject.toJSONString(row);
            recordWriter.write(NullWritable.get(), new Text(org.apache.hadoop.util.StringUtils.join(tableInfo.getDelimiter(), new String[]{content, JSONObject.toJSONString(ex.toString()), DateUtil.timestampToString(new Date())})));
        } catch (IOException e) {
            logger.error("{}", e);
        }
    }

    public void open() {
        try {
            String location = String.format("%s/%s-%d.txt", tableInfo.getPath(), HostUtil.getHostName(), Thread.currentThread().getId());
            FileOutputFormat.setOutputPath(jobConf, new Path(location));
            this.recordWriter = this.outputFormat.getRecordWriter(null, jobConf, location, Reporter.NULL);


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            RecordWriter<?, ?> rw = this.recordWriter;
            if (rw != null) {
                rw.close(Reporter.NULL);
            }
        } catch (IOException e) {
            logger.error("{}", e);
        }
    }

}
