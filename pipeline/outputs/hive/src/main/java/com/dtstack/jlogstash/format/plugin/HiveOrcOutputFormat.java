/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.jlogstash.format.plugin;


import com.dtstack.jlogstash.format.CompressEnum;
import com.dtstack.jlogstash.format.HiveOutputFormat;
import com.dtstack.jlogstash.format.util.DateUtil;
import com.dtstack.jlogstash.utils.ExceptionUtil;
import com.dtstack.jlogstash.format.util.HiveUtil;
import com.dtstack.jlogstash.format.util.HostUtil;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormatBak;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @author haisi
 */
public class HiveOrcOutputFormat extends HiveOutputFormat {

	public HiveOrcOutputFormat(Configuration conf, String outputFilePath, List<String> columnNames,
                               List<String> columnTypes, String compress, String writeMode, Charset charset) {
	   this.conf = conf;
	   this.outputFilePath = outputFilePath;
	   this.columnNames = columnNames;
	   this.columnTypes = columnTypes;
	   this.compress = compress;
	   this.writeMode = writeMode;
	   this.charset = charset;
	}

	private static Logger logger = LoggerFactory.getLogger(HiveOrcOutputFormat.class);

    private static final long serialVersionUID = 1L;

    private  OrcSerde orcSerde;
    private  StructObjectInspector inspector;
    private  List<ObjectInspector> columnTypeList;


    @Override
    public void configure() {
    	super.configure();
        this.orcSerde = new OrcSerde();
        this.outputFormat = new OrcOutputFormatBak();

        this.columnTypeList = Lists.newArrayList();
        for(String columnType : columnTypes) {
            this.columnTypeList.add(HiveUtil.columnTypeToObjectInspetor(columnType));
        }
        this.inspector = ObjectInspectorFactory
                .getStandardStructObjectInspector(this.columnNames, this.columnTypeList);

        Class<? extends CompressionCodec> codecClass = null;
        if(CompressEnum.NONE.name().equalsIgnoreCase(compress)){
            codecClass = null;
        } else if(CompressEnum.GZIP.name().equalsIgnoreCase(compress)){
            codecClass = org.apache.hadoop.io.compress.GzipCodec.class;
        } else if (CompressEnum.BZIP2.name().equalsIgnoreCase(compress)) {
            codecClass = org.apache.hadoop.io.compress.BZip2Codec.class;
        } else if(CompressEnum.SNAPPY.name().equalsIgnoreCase(compress)) {
            //todo 等需求明确后支持 需要用户安装SnappyCodec
            codecClass = org.apache.hadoop.io.compress.SnappyCodec.class;
        } else {
            throw new IllegalArgumentException("Unsupported compress format: " + compress);
        }

        if(codecClass != null){
            FileOutputFormat.setOutputCompressorClass(jobConf, codecClass);
        }
    }

    @Override
    public void open() throws IOException {
        super.open();
        fileName = String.format("%s-%d-%s.orc", HostUtil.getHostName(), Thread.currentThread().getId(), UUID.randomUUID().toString());
        tmpPath = String.format("%s/%s/%s", outputFilePath, DATA_SUBDIR, fileName);
        finishedPath = String.format("%s/%s", outputFilePath, fileName);
        logger.info("hive tmpPath:{} finishedPath:{}",tmpPath, finishedPath);
        FileOutputFormat.setOutputPath(jobConf, new Path(tmpPath));
        this.recordWriter = this.outputFormat.getRecordWriter(null, jobConf, tmpPath, Reporter.NULL);
    }

    @Override
    public void close() throws IOException {
        super.close();
        Path pathOfFinished =new Path(finishedPath);
        if (FileSystem.get(jobConf).exists(pathOfFinished)){
            String trashPath = String.format("%s/%s/%s", outputFilePath, TRASH_SUBDIR, fileName);
            FileSystem.get(jobConf).rename(pathOfFinished, new Path(trashPath));
        }
        Path pathOfTmp =new Path(tmpPath);
        if (FileSystem.get(jobConf).listStatus(pathOfTmp)[0].getLen() == 0){
            return;
        }
        FileSystem.get(jobConf).rename(pathOfTmp, pathOfFinished);
    }

    @Override
    public Object[] convert2Record(Map<String, Object> row) throws Exception {
        Object[] record = new Object[this.columnSize];
        for(int i = 0; i < this.columnSize; i++) {
            Object column = row.get(this.columnNames.get(i));
            if(column == null) {
                record[i] = null;
                continue;
            }
            String rowData = column.toString();
            if(StringUtils.isBlank(rowData)){
                record[i] = null;
                continue;
            }
            Object field = null;
            try {
                switch(this.columnTypes.get(i).toUpperCase()) {
                    case "TINYINT":
                        field = Byte.valueOf(rowData);
                        break;
                    case "SMALLINT":
                        field = Short.valueOf(rowData);
                        break;
                    case "INT":
                        field = Integer.valueOf(rowData);
                        break;
                    case "BIGINT":
                        if (column instanceof Timestamp){
                            field=((Timestamp) column).getTime();
                            break;
                        }
                        BigInteger data = new BigInteger(rowData);
                        if (data.compareTo(new BigInteger(String.valueOf(Long.MAX_VALUE))) > 0){
                            field = data;
                        } else {
                            field = Long.valueOf(rowData);
                        }
                        break;
                    case "FLOAT":
                        field = Float.valueOf(rowData);
                        break;
                    case "DOUBLE":
                        field = Double.valueOf(rowData);
                        break;
                    case "DECIMAL":
                        HiveDecimal hiveDecimal = HiveDecimal.create(new BigDecimal(rowData));
                        HiveDecimalWritable hiveDecimalWritable = new HiveDecimalWritable(hiveDecimal);
                        field = hiveDecimalWritable;
                        break;
                    case "STRING":
                    case "VARCHAR":
                    case "CHAR":
                        if (column instanceof Timestamp){
                            SimpleDateFormat fm = DateUtil.getDateTimeFormatter();
                            field = fm.format(column);
                        }else {
                            field = rowData;
                        }
                        break;
                    case "BOOLEAN":
                        field = Boolean.valueOf(rowData);
                        break;
                    case "DATE":
                        field = DateUtil.columnToDate(column, null);
                        break;
                    case "TIMESTAMP":
                        field = DateUtil.columnToTimestamp(column, null);
                        break;
                    case "BINARY":
                        field = new BytesWritable(rowData.getBytes());
                        break;
                    default:
                        field = rowData;

                }
            } catch (Exception e) {
                throw new Exception("field convert error:"+ ExceptionUtil.getStackTrace(e)+", columnName=" + this.columnNames.get(i) +
                        ", columnType=" + this.columnTypes.get(i) +
                        ", fieldData=" + rowData, e);
            }

            record[i] = field;
        }
        return record;
    }

    @SuppressWarnings("unchecked")
	@Override
    public void writeRecord(Object[] record) throws Exception {
        super.writeRecord(record);

        this.recordWriter.write(NullWritable.get(), this.orcSerde.serialize(Arrays.asList(record), this.inspector));
    }
}
