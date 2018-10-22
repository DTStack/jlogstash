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

import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.exception.InitializeException;
import com.dtstack.jlogstash.util.BlobClobUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("serial")
public class Jdbc extends BaseInput {

    private static final Logger logger = LoggerFactory.getLogger(JdbcClientHelper.class);

    @Required(required = true)
    private String jdbcConnectionString;

    @Required(required = true)
    private String jdbcDriverClass;

    @Required(required = true)
    private String jdbcDriverLibrary;

    private Integer jdbcFetchSize;

    @Required(required = true)
    private String jdbcUser;

    @Required(required = true)
    private String jdbcPassword;

    @Required(required = true)
    private String statement;

    private String parameters;

    private volatile boolean stop;

    private List<String> blobFields;

    private List<String> clobFields;

    private boolean needConvertBlob;

    private boolean needConvertClob;

    private Connection connection;

    @SuppressWarnings("rawtypes")
    public Jdbc(Map config) {
        super(config);
    }

    @Override
    public void prepare() {
        if (blobFields != null && blobFields.size() > 0) {
            needConvertBlob = true;
        }
        if (clobFields != null && clobFields.size() > 0) {
            needConvertClob = true;
        }
        connection = initConn(); // 创建连接
    }

    @Override
    public void emit() {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = connection.prepareStatement(statement, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            if (jdbcFetchSize != null) {
                preparedStatement.setFetchSize(jdbcFetchSize);
            }

            resultSet = preparedStatement.executeQuery();

            // 获取列名
            List<String> columnNameList = new ArrayList<String>();
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int count = rsmd.getColumnCount();
            for (int i = 1; i <= count; i++) {
                columnNameList.add(rsmd.getColumnName(i));
            }

            // 逐行读取
            while (!stop && resultSet.next()) {
                Map<String, Object> rowMap = new HashMap<String, Object>();
                for (String columnName : columnNameList) {
                    rowMap.put(columnName, resultSet.getObject(columnName));
                }

                handleBlob(rowMap);
                handleClob(rowMap);

                process(rowMap);
            }
        } catch (SQLException e) {
            logger.error("read failed", e);
        } finally {
            JdbcClientHelper.releaseConnection(connection, preparedStatement, resultSet);
        }
    }

    private void handleClob(Map<String, Object> rowMap) {
        if (needConvertClob) {
            for (String clobField : clobFields) {
                Object obj = rowMap.get(clobField);
                if (obj != null && obj instanceof Clob) {
                    Clob clob = (Clob) obj;
                    String content = BlobClobUtil.convertClob2String(clob);
                    rowMap.put(clobField, content);
                }
            }
        }
    }

    private void handleBlob(Map<String, Object> rowMap) {
        if (needConvertBlob) {
            for (String blobField : blobFields) {
                Object obj = rowMap.get(blobField);
                if (obj != null && obj instanceof Blob) {
                    Blob blob = (Blob) obj;
                    byte[] bytes = BlobClobUtil.convertBlob2Bytes(blob);
                    rowMap.put(blobField, bytes);
                }
            }
        }
    }

    @Override
    public void release() {
        stop();
        connection = null;
    }

    private void stop() {
        stop = true;
    }

    private Connection initConn() {
        ConnectionConfig connectionConfig = new ConnectionConfig();
        connectionConfig.setJdbc_connection_string(jdbcConnectionString);
        connectionConfig.setJdbc_driver_class(jdbcDriverClass);
        connectionConfig.setJdbc_driver_library(jdbcDriverLibrary);
        connectionConfig.setJdbc_user(jdbcUser);
        connectionConfig.setJdbc_password(jdbcPassword);

        try {
            return JdbcClientHelper.getConnection(connectionConfig);
        } catch (Exception e) {
            throw new InitializeException("get jdbc connection failed", e);
        }
    }

}
