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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import java.sql.SQLException;

public class TestHive {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration(false);
        conf.set("dfs.ha.namenodes.ns1", "nn1,nn2");
        conf.set("fs.defaultFS", "hdfs://ns1");
        conf.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("dfs.namenode.rpc-address.ns1.nn2","dtstack2:9000");
        conf.set("dfs.namenode.rpc-address.ns1.nn1", "dtstack1:9000");
        conf.set("dfs.nameservices", "ns1");
        conf.set("fs.hdfs.impl.disable.cache", "true");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        JobConf jobConf = new JobConf(conf);

        FileOutputFormat.setOutputPath(jobConf, new Path("/tmp/zhaotoutian.orc"));
        OrcOutputFormat outputFormat = new OrcOutputFormat();
        outputFormat.getRecordWriter(null, jobConf, "/tmp/zhaotoutian.orc", Reporter.NULL);

    }
}
