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

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;


public class BinlogEventSink extends AbstractCanalLifeCycle implements com.alibaba.otter.canal.sink.CanalEventSink<List<CanalEntry.Entry>> {

    private static final Logger logger = LoggerFactory.getLogger(BinlogEventSink.class);

    private Binlog binlog;

    private boolean pavingData;

    public BinlogEventSink(Binlog binlog) {
        this.binlog = binlog;
    }

    @Override
    public boolean sink(List<CanalEntry.Entry> entries, InetSocketAddress inetSocketAddress, String s) throws CanalSinkException, InterruptedException {

        for (CanalEntry.Entry entry : entries) {
            CanalEntry.EntryType entryType = entry.getEntryType();

            if (entryType != CanalEntry.EntryType.ROWDATA) {
                continue;
            }

            if (logger.isDebugEnabled()) {
                logger.debug("binlog sink, entryType:{}", entry.getEntryType());
            }

            CanalEntry.RowChange rowChange = parseRowChange(entry);

            if(rowChange == null) {
                return false;
            }

            CanalEntry.Header header = entry.getHeader();
            long ts = header.getExecuteTime();
            String schema = header.getSchemaName();
            String table = header.getTableName();
            processRowChange(rowChange, schema, table, ts);
        }

        return true;
    }

    private CanalEntry.RowChange parseRowChange(CanalEntry.Entry entry) {
        CanalEntry.RowChange rowChange = null;
        try {
            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        } catch (Exception e) {
            logger.error("ERROR ## parser of eromanga-event has an error , data:" + entry.toString());
        }
        return rowChange;
    }

    private void processRowChange(CanalEntry.RowChange rowChange, String schema, String table, long ts) {
        CanalEntry.EventType eventType = rowChange.getEventType();

        if(!binlog.accept(eventType.toString())) {
            return;
        }

        for(CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            Map<String,Object> message = new HashMap<>();
            message.put("type", eventType.toString());
            message.put("schema", schema);
            message.put("table", table);
            message.put("ts", ts);

            if (pavingData){
                for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                    message.put("after_" + column.getName(), column.getValue());
                }
                for (CanalEntry.Column column : rowData.getBeforeColumnsList()){
                    message.put("before_" + column.getName(), column.getValue());
                }
            } else {
                message.put("before", processColumnList(rowData.getBeforeColumnsList()));
                message.put("after", processColumnList(rowData.getAfterColumnsList()));
            }

            binlog.process(message);
        }

    }

    private String processColumnList(List<CanalEntry.Column> columnList) {
        Map<String,Object> map = new HashMap<>();
        for (CanalEntry.Column column : columnList) {
            map.put(column.getName(), column.getValue());
        }
        return JSON.toJSONString(map);
    }

    public void setPavingData(boolean pavingData) {
        this.pavingData = pavingData;
    }

    @Override
    public void interrupt() {
        logger.info("BinlogEventSink is interrupted");
    }

}
