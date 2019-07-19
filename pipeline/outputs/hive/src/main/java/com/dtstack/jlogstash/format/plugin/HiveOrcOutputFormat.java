package com.dtstack.jlogstash.format.plugin;


import com.dtstack.jlogstash.format.CompressEnum;
import com.dtstack.jlogstash.format.HiveOutputFormat;
import com.dtstack.jlogstash.format.util.DateUtil;
import com.dtstack.jlogstash.format.util.HiveUtil;
import com.dtstack.jlogstash.format.util.HostUtil;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
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
        this.outputFormat = new OrcOutputFormat();
        
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
        String pathStr = String.format("%s/%s-%d-%s.orc", outputFilePath, HostUtil.getHostName(),Thread.currentThread().getId(),UUID.randomUUID().toString());
        logger.info("hive path:{}",pathStr);
        FileOutputFormat.setOutputPath(jobConf, new Path(pathStr));
        this.recordWriter = this.outputFormat.getRecordWriter(null, jobConf, pathStr, Reporter.NULL);
    }

    @SuppressWarnings("unchecked")
	@Override
    public void writeRecord(Map<String,Object> row) throws IOException {
        Object[] record = new Object[this.columnSize];
        for(int i = 0; i < this.columnSize; i++) {
            Object data = row.get(this.columnNames.get(i));
            if(data == null) {
                data = "";
            }
            String rowData = data.toString();
            Object field = null;
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
                    field = Long.valueOf(rowData);
                    break;
                case "FLOAT":
                    field = Float.valueOf(rowData);
                    break;
                case "DOUBLE":
                    field = Double.valueOf(rowData);
                    break;
                case "STRING":
                case "VARCHAR":
                case "CHAR":
                    field = rowData;
                    break;
                case "BOOLEAN":
                    field = Boolean.valueOf(rowData);
                    break;
                case "DATE":
                    field = DateUtil.columnToDate(data);
                    break;
                case "TIMESTAMP":
                    java.sql.Date d = DateUtil.columnToDate(data);
                    field = new java.sql.Timestamp(d.getTime());
                    break;
                default:
                    field = rowData;

            }
            record[i] = field;
        }
        this.recordWriter.write(NullWritable.get(), this.orcSerde.serialize(Arrays.asList(record), this.inspector));
    }
}
