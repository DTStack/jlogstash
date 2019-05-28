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

import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.assembly.CmdLineParams;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
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
 *
 * Reason: TODO ADD REASON(可选)
 * Date: 2018年8月28日
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 *
 */
public class Binlog extends BaseInput {

    private static final Logger logger = LoggerFactory.getLogger(Binlog.class);

    /** plugin properties */

    private String taskId = "defaultTaskId";

    @Required(required = true)
    private String host;

    private int port = 3306;

    private long slaveId = 3344L;

    @Required(required = true)
    private String username;

    @Required(required = true)
    private String password;

    private Map<String,Object> start;

    private String filter;

    private String cat;

    /** internal fields */

    private MysqlEventParser controller;

    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private volatile EntryPosition entryPosition = new EntryPosition();

    private List<String> categories = new ArrayList<>();

    public Binlog(Map config) {
        super(config);
        taskId = CmdLineParams.getName();
        new com.dtstack.jlogstash.inputs.LogbackComponent().setupLogger();
    }

    public void updateLastPos(EntryPosition entryPosition) {
        this.entryPosition = entryPosition;
    }

    public boolean accept(String type) {
        return categories.isEmpty() || categories.contains(type);
    }

    private void parseCategories() {
        if(!StringUtils.isBlank(cat)) {
            System.out.println(categories);
            categories = Arrays.asList(cat.toUpperCase().split(","));
        }
    }

    private EntryPosition findStartPosition() {
        if(start != null && start.size() != 0) {
            EntryPosition startPosition = new EntryPosition();
            String journalName = (String) start.get("journalName");
            if(StringUtils.isNotEmpty(journalName)) {
                if(new BinlogJournalValidator(host, port, username, password).check(journalName)) {
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
        try {
            startPosition = BinlogPosUtil.readPos(taskId);
        } catch(IOException e) {
            logger.error("Failed to read pos file: " + e.getMessage());
        }

        return startPosition;
    }

    @Override
    public void prepare() {
        logger.info("binlog prepare started..");

        parseCategories();

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
        controller.setEventSink(new BinlogEventSink(this));

        controller.setLogPositionManager(new BinlogPositionManager(this));

        EntryPosition startPosition = findStartPosition();
        if (startPosition != null) {
           controller.setMasterPosition(startPosition);
        }


        if (filter != null) {
            controller.setEventFilter(new AviaterRegexFilter(filter));
        }

        logger.info("binlog prepare ended..");
    }

    @Override
    public void emit() {
        logger.info("binlog emit started...");

        controller.start();

        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    BinlogPosUtil.savePos(taskId + "_output", entryPosition);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 500, 1000, TimeUnit.MILLISECONDS);

        logger.info("binlog emit ended...");
    }

    @Override
    public void release() {
        if(controller != null) {
            controller.stop();
        }

        if(scheduler != null) {
            scheduler.shutdown();
        }

        try {
            BinlogPosUtil.savePos(taskId + "_output", entryPosition);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        Map<String,Object> config = new HashMap<>();
        Binlog binlog = new Binlog(config);
        binlog.host = "rdos1";
        binlog.username = "canal";
        binlog.password = "canal";

        binlog.prepare();
        binlog.emit();
    }

}
