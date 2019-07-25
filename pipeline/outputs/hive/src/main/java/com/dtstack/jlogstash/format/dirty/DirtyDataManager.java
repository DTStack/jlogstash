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

import static com.dtstack.jlogstash.format.dirty.WriteErrorTypes.*;

public class DirtyDataManager {

    private String location;
    private Configuration config;
    private BufferedWriter bw;

    private static final String FIELD_DELIMITER = "\u0001";
    private static final String LINE_DELIMITER = "\n";


    public DirtyDataManager(String path, Map<String, String> configMap) {
        location = path + "/" + UUID.randomUUID() + ".txt";
        config = new Configuration();
        if (configMap != null) {
            for (Map.Entry<String, String> entry : configMap.entrySet()) {
                config.set(entry.getKey(), entry.getValue());
            }
        }
        config.set("fs.hdfs.impl.disable.cache", "true");
    }

    public String writeData(Map row, WriteRecordException ex) {
        String content = JSONObject.toJSONString(row);
        String errorType = retrieveCategory(ex);
        String line = StringUtils.join(new String[]{content, errorType, JSONObject.toJSONString(ex.toString()), DateUtil.timestampToString(new Date())}, FIELD_DELIMITER);
        try {
            bw.write(line);
            bw.write(LINE_DELIMITER);
            return errorType;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String retrieveCategory(WriteRecordException ex) {
        Throwable cause = ex.getCause();
        if (cause instanceof NullPointerException) {
            return ERR_NULL_POINTER;
        }
        return ERR_FORMAT_TRANSFORM;
    }

    public void open() {
        try {
            FileSystem fs = FileSystem.get(config);
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
