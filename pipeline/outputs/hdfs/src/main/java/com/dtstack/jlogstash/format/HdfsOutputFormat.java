package com.dtstack.jlogstash.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author sishu.yss
 *
 */
public abstract class HdfsOutputFormat implements  OutputFormat {

	public static String slash = "/";
    protected static final int NEWLINE = 10;
    protected Charset charset;
    protected String writeMode;
    protected boolean overwrite;
    protected String compress;
    protected List<String> columnNames;
    protected int columnSize;
    protected List<String> columnTypes;
    protected  String outputFileDir;
    protected  FileOutputFormat<?, ?> outputFormat;
    protected  JobConf jobConf;
    protected  Configuration conf;
    protected  Map<String, String> columnNameTypeMap;
    protected  Map<String, Integer> columnNameIndexMap;
    protected  RecordWriter recordWriter;
    protected String fileName;


    public static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure() {
        columnSize = this.columnNames.size();
        jobConf = new JobConf(conf);
    }

    public abstract void writeRecord(Map<String,Object> row) throws IOException;

    @Override
    public void close() throws IOException {
        RecordWriter<?, ?> rw = this.recordWriter;
        if(rw != null) {
            rw.close(Reporter.NULL);
        }
    }
    
}
