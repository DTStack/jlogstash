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

package com.dtstack.jlogstash;


import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TestMysqlDistributeTable {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        String driver = "com.mysql.jdbc.Driver";
        Class.forName(driver);
        String url = "jdbc:mysql://172.16.10.45:3306/db.nanqi.0725";
        String user = "dtstack";
        String pass = "abc123";


        Connection conn = DriverManager.getConnection(url, user, pass);
        for (int j = 1; j < 200; j++) {


            for (int i = 0; i < 12; i++) {
                System.out.println("tb.user" + StringUtils.leftPad(i + "", 4, "0"));
                Statement statement = conn.createStatement();

//            String sql = "CREATE TABLE `tb.user` " +
//                    "                    (" +
//                    "`tb.id` int(11) NOT NULL,\n" +
//                    "  `name` varchar(255)"+
//                    "                    ) ENGINE=InnoDB ";

                String sql = "insert into `tb.user` values(111,'toutian')";
                sql = sql.replace("tb.user", "tb.user" + StringUtils.leftPad(i + "", 4, "0"));
                statement.execute(sql);
                statement.close();
            }
        }
//        stmt = conn.prepareCall(callProc);
//        stmt.execute();

    }
}
