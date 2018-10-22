package com.dtstack.jlogstash.outputs;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.format.HdfsOutputFormat;
import com.dtstack.jlogstash.format.StoreEnum;
import com.dtstack.jlogstash.format.plugin.HdfsOrcOutputFormat;
import com.dtstack.jlogstash.format.plugin.HdfsTextOutputFormat;
import com.dtstack.jlogstash.render.Formatter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 
 * @author sishu.yss
 *
 */
public class Hdfs extends BaseOutput{
	
	private static final long serialVersionUID = -6012196822223887479L;
	
	private static Logger logger = LoggerFactory.getLogger(Hdfs.class);

	private static String hadoopConf = System.getenv("HADOOP_CONF_DIR");
	
	@Required(required = true)
	private static String path ;//模板配置
	
	private static String store = "TEXT";
	
	private static String writeMode = "APPEND";
	
	private static String compression = "NONE";
	
	private static String charsetName = "UTF-8";
	
	private static Charset charset;
	
	private static String delimiter = "\001";
	
	public static String timezone;
	
	public static int interval = 5*60*1000;//millseconds
	
	public static int bufferSize = 1024;//bytes
	
    private ExecutorService executor;

	@Required(required = true)
	private static List<String> schema;//["name:varchar"]
	
	private static List<String> columns;
	
	private static List<String> columnTypes;
	
	private static String hadoopUserName = "root";
	
	private static Configuration configuration;
	
	private Map<String,HdfsOutputFormat> hdfsOutputFormats = Maps.newConcurrentMap();
	
	private Lock lock = new ReentrantLock();
	
	private AtomicBoolean lockBoolean = new AtomicBoolean(true);
	
	public Hdfs(Map config) {
		super(config);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void prepare() {
		// TODO Auto-generated method stub
		try {
			formatSchema();
			setHadoopConfiguration();
			process();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("",e);
			System.exit(-1);
		}
	}

	public void process(){
		executor = Executors.newSingleThreadExecutor();
		executor.submit(new Runnable(){

			@Override
			public void run() {
				// TODO Auto-generated method stub
				try {
					while(true){
						Thread.sleep(interval);
						try{
							lock.lockInterruptibly();
							lockBoolean.set(false);
							release();
							logger.warn("hdfs commit again...");
						}finally{
							lock.unlock();
							lockBoolean.set(true);
						}
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					logger.error("",e);
				}
			}
		});
	}
	
	@Override
	protected void emit(Map event) {
		// TODO Auto-generated method stub
		try{
			String realPath = Formatter.format(event, path, timezone);
			getHdfsOutputFormat(realPath).writeRecord(event);
		}catch(Exception e){
			this.addFailedMsg(event);
			logger.error("",e);
		}
	}
	
	public HdfsOutputFormat getHdfsOutputFormat(String realPath) throws IOException{
		if(!lockBoolean.get()){
			try {
				lock.lockInterruptibly();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				logger.error("",e);
			}finally{
				lock.unlock();
			}
		}
		HdfsOutputFormat hdfsOutputFormat = hdfsOutputFormats.get(realPath);
		if(hdfsOutputFormat == null){
			if(StoreEnum.TEXT.name().equals(store)){
				hdfsOutputFormat = new HdfsTextOutputFormat(configuration,realPath, columns, columnTypes, compression, writeMode, charset, delimiter);
			}else if(StoreEnum.ORC.name().equals(store)){
				hdfsOutputFormat = new HdfsOrcOutputFormat(configuration,realPath, columns, columnTypes, compression, writeMode, charset);
			}
			hdfsOutputFormat.configure();
			hdfsOutputFormat.open();
			hdfsOutputFormats.put(realPath, hdfsOutputFormat);
		}
		return hdfsOutputFormat;
	}
	
	
	@Override
	public void sendFailedMsg(Object msg){
		emit((Map) msg);
	}
	
	@Override
	public synchronized void release(){
		Set<Map.Entry<String, HdfsOutputFormat>> entrys = hdfsOutputFormats.entrySet();
		for(Map.Entry<String, HdfsOutputFormat> entry:entrys){
			try {
				entry.getValue().close();
				hdfsOutputFormats.remove(entry.getKey());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("",e);
			}
		}
	}
	
	private void formatSchema(){
		if(columns == null){
			synchronized(Hdfs.class){
				if(columns == null){
					charset = Charset.forName(charsetName);
					columns = Lists.newArrayList();
					columnTypes = Lists.newArrayList();
			        for(String sche:schema){
			        	String[] sc = sche.split(":");
			        	columns.add(sc[0]);
			        	columnTypes.add(sc[1]);
			        }
				}
			}
		}
	}
	
	private void setHadoopConfiguration() throws Exception{
		if(configuration == null){
			synchronized(Hdfs.class){
				if(configuration == null){
					System.setProperty("HADOOP_USER_NAME", hadoopUserName);
					configuration = new Configuration();
		    		configuration.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
		            File[] xmlFileList = new File(hadoopConf).listFiles(new FilenameFilter() {
		                @Override
		                public boolean accept(File dir, String name) {
		                    if(name.endsWith(".xml"))
		                        return true;
		                    return false;
		                }
		            });

		            if(xmlFileList != null) {
		                for(File xmlFile : xmlFileList) {
		                	configuration.addResource(xmlFile.toURI().toURL());
		                }
		            }
				}
			}
			
		}
	}
	
	public static void main(String[] args) throws Exception{
		Hdfs.hadoopConf = "/Users/sishuyss/ysq/dtstack/rdos-web-all/conf/hadoop";
		Hdfs.hadoopUserName = "admin";
		Hdfs.path = "/test13/%{table}";
		Hdfs.store = "TEXT";
		Hdfs.interval= 2000;
//		Hdfs.compression="GZIP";
		List<String> sche = Lists.newArrayList("table:varchar","op_type:varchar","op_ts:varchar","current_ts:varchar","pos:varchar","before:varchar","after:varchar");
		Hdfs.schema = sche;
		
//		for(int i =0;i<1;i++){
//			new Thread(new Runnable(){
//
//				@Override
//				public void run() {
//					// TODO Auto-generated method stub
//					
//					Hdfs hdfs = new Hdfs(Maps.newConcurrentMap());
//					hdfs.prepare();
//					for(int i=0;i<100000;i++){
//						Map<String,Object> event = Maps.newConcurrentMap();
//						event.put("table", "TEST.T");
//						event.put("op_type", "U");
//						event.put("op_ts", "2017-09-05 09:47:45.040103");
//						event.put("current_ts", "2017-09-05T17:47:52.868000");
//						event.put("before", "{\"ID\":12,\"NAME\":\"dsasasdas\"}");
//						event.put("after", "{\"ID\":12,\"NAME\":\"1234455\"}");
//						hdfs.emit(event);	
//					}
//				}
//			}).start();
//		}
	}
}
