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
package com.dtstack.jlogstash.inputs;

import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.assembly.CmdLineParams;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Reason: TODO ADD REASON(可选)
 * Date: 2018年8月28日
 * Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class Binlog extends BaseInput {

    private static final Logger logger = LoggerFactory.getLogger(Binlog.class);

    private static String hadoopConf = System.getenv("HADOOP_CONF_DIR");

    private static String hadoopUserName = "admin";

    private static Map<String, Object> hadoopConfigMap;

    private Configuration configuration;

    private FileSystem dfs;

    private Path posPathTmp;
    private Path posPath;

    /**
     * plugin properties
     */

    private String taskId = "defaultTaskId";

    @Required(required = true)
    private String host;

    private static boolean pavingData = false;

    private int port = 3306;

    private long slaveId = 3344L;

    private String jdbcUrl;

    @Required(required = true)
    private String username;

    @Required(required = true)
    private String password;

    private Map<String, Object> start;

    private String filter;

    private String cat;

    private static List<String> table;

    /**
     * internal fields
     */

    private MysqlEventParser controller;

    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private volatile EntryPosition entryPosition;

    private EntryPosition lastEntryPosition;

    private List<String> categories = new ArrayList<>();

    public Binlog(Map config) {
        super(config);
        taskId = CmdLineParams.getName();
    }

    public void updateLastPos(EntryPosition entryPosition) {
        this.entryPosition = entryPosition;
    }

    public boolean accept(String type) {
        return categories.isEmpty() || categories.contains(type);
    }

    private void parseCategories() {
        if (!StringUtils.isBlank(cat)) {
            System.out.println(categories);
            categories = Arrays.asList(cat.toUpperCase().split(","));
        }
    }

    private EntryPosition findStartPosition() {
        if (start != null && start.size() != 0) {
            EntryPosition startPosition = new EntryPosition();
            String journalName = (String) start.get("journalName");
            if (StringUtils.isNotEmpty(journalName)) {
                if (new BinlogJournalValidator(host, port, username, password).check(journalName)) {
                    startPosition.setJournalName(journalName);
                } else {
                    throw new IllegalArgumentException("Can't find journalName: " + journalName);
                }
            }
            startPosition.setTimestamp((Long) start.get("timestamp"));
            startPosition.setPosition((Long) start.get("position"));
            return startPosition;
        }

        EntryPosition startPosition = null;
        if (configuration == null) {
            try {
                startPosition = BinlogPosUtil.readPos(taskId);
            } catch (IOException e) {
                logger.error("Failed to read pos file: " + e.getMessage());
            }
        } else {
            try (FSDataInputStream inputStream = dfs.open(posPath)) {
                String json = inputStream.readUTF();
                startPosition = new ObjectMapper().readValue(json, EntryPosition.class);
            } catch (Exception e) {
                logger.error("Failed to read pos file: " + e.getMessage());
            }
        }
        return startPosition;
    }

    @Override
    public void prepare() {
        try {
            logger.info("binlog prepare started..");

            parseCategories();

            setHadoopConfiguration();
            if (configuration != null) {
                posPathTmp = new Path(configuration.get("fs.defaultFS"), "/user/jlogstash/." + taskId + "_output");
                posPath = new Path(configuration.get("fs.defaultFS"), "/user/jlogstash/" + taskId + "_output");
                dfs = FileSystem.get(configuration);
            }

            controller = new MysqlEventParser();
            controller.setConnectionCharset(Charset.forName("UTF-8"));
            controller.setSlaveId(slaveId);
            controller.setDetectingEnable(false);
            controller.setMasterInfo(new AuthenticationInfo(new InetSocketAddress(host, port), username, password));
            controller.setEnableTsdb(true);
            controller.setDestination("example");
            controller.setParallel(true);
            controller.setParallelBufferSize(256);
            controller.setParallelThreadSize(2);
            controller.setIsGTIDMode(false);

            BinlogEventSink sink = new BinlogEventSink(this);
            sink.setPavingData(pavingData);
            controller.setEventSink(sink);

            controller.setLogPositionManager(new BinlogPositionManager(this));

            EntryPosition startPosition = findStartPosition();
            if (startPosition != null) {
                controller.setMasterPosition(startPosition);
            }

            /**
             * mysql 数据解析关注的表，Perl正则表达式.

             多个正则之间以逗号(,)分隔，转义符需要双斜杠(\\)


             常见例子：

             1.  所有表：.*   or  .*\\..*
             2.  canal schema下所有表： canal\\..*
             3.  canal下的以canal打头的表：canal\\.canal.*
             4.  canal schema下的一张表：canal\\.test1

             5.  多个规则组合使用：canal\\..*,mysql.test1,mysql.test2 (逗号分隔)
             */
            if (table != null && table.size() != 0 && jdbcUrl != null) {
                int idx = jdbcUrl.lastIndexOf('?');
                String database = null;
                if (idx != -1) {
                    database = StringUtils.substring(jdbcUrl, jdbcUrl.lastIndexOf('/') + 1, idx);
                } else {
                    database = StringUtils.substring(jdbcUrl, jdbcUrl.lastIndexOf('/') + 1);
                }
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < table.size(); i++) {
                    sb.append(database).append(".").append(table.get(i));
                    if (i != table.size() - 1) {
                        sb.append(",");
                    }
                }
                filter = sb.toString();
            }
            if (filter != null) {
                controller.setEventFilter(new AviaterRegexFilter(filter));
            }

            logger.info("binlog prepare ended..");
        } catch (Exception e) {
            logger.error("", e);
            System.exit(-1);
        }
    }

    @Override
    public void emit() {
        logger.info("binlog emit started...");

        controller.start();

        long period = configuration != null ? 5000 : 1000;
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                savePos();
            }
        }, period, period, TimeUnit.MILLISECONDS);

        logger.info("binlog emit ended...");
    }

    @Override
    public void release() {

        logger.info("binlog release...");

        if (controller != null) {
            controller.stop();
        }

        if (scheduler != null) {
            scheduler.shutdown();
        }

        savePos();
        logger.info("binlog release..., save pos:{}", entryPosition);
    }

    private void savePos() {
        if (entryPosition == null || entryPosition.getPosition() == null || entryPosition.getJournalName() == null
                || lastEntryPosition != null && lastEntryPosition.getPosition().equals(entryPosition.getPosition()) && lastEntryPosition.getJournalName().equals(entryPosition.getJournalName())
                ) {
            return;
        }
        lastEntryPosition = entryPosition;
        if (configuration != null) {
            FSDataOutputStream out = null;
            try {
                out = dfs.create(posPathTmp);
                dfs.setPermission(posPathTmp, new FsPermission(FsPermission.createImmutable((short) 0777)));
                out.writeUTF(new ObjectMapper().writeValueAsString(entryPosition));
                if (dfs.exists(posPath)) {
                    dfs.delete(posPath);
                }
                dfs.rename(posPathTmp, posPath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                IOUtils.closeStream(out);
            }
            logger.debug("save pos to hdfs, entryPosition:{}", entryPosition);
        } else {
            try {
                BinlogPosUtil.savePos(taskId + "_output", entryPosition);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            logger.debug("save pos to local, entryPosition:{}", entryPosition);
        }
    }

    private void setHadoopConfiguration() throws Exception {
        if (hadoopUserName != null) {
            System.setProperty("HADOOP_USER_NAME", hadoopUserName);
        }
        if (hadoopConfigMap != null) {
            configuration = new Configuration(false);
            for (Map.Entry<String, Object> entry : hadoopConfigMap.entrySet()) {
                configuration.set(entry.getKey(), entry.getValue().toString());
            }
            configuration.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        }
    }

}
