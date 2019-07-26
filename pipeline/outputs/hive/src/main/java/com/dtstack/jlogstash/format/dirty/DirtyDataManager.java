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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

public class DirtyDataManager {

    private TableInfo tableInfo;
    private Configuration config;
    private BufferedWriter bw;

    public DirtyDataManager(TableInfo tableInfo, Configuration configuration) {
        this.tableInfo = tableInfo;
        this.config = configuration;
    }

    public void writeData(Map row, Throwable ex) {
        String content = JSONObject.toJSONString(row);
        String line = StringUtils.join(new String[]{content, JSONObject.toJSONString(ex.toString()), DateUtil.timestampToString(new Date())}, tableInfo.getDelimiter());
        try {
            bw.write(line);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void open() {
        try {
            FileSystem fs = FileSystem.get(config);
            String location = tableInfo.getPath() + "/" + UUID.randomUUID() + ".txt";
            Path path = new Path(location);
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(path, true)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        if (bw != null) {
            try {
                bw.flush();
                bw.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
