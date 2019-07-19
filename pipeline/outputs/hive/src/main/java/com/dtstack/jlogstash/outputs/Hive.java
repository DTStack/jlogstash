package com.dtstack.jlogstash.outputs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.format.HiveOutputFormat;
import com.dtstack.jlogstash.format.StoreEnum;
import com.dtstack.jlogstash.format.TableInfo;
import com.dtstack.jlogstash.format.plugin.HiveOrcOutputFormat;
import com.dtstack.jlogstash.format.plugin.HiveTextOutputFormat;
import com.dtstack.jlogstash.format.util.HiveConverter;
import com.dtstack.jlogstash.format.util.HiveUtil;
import com.google.common.collect.Maps;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author sishu.yss
 */
public class Hive extends BaseOutput {

    private static final long serialVersionUID = -6012196822223887479L;

    private static Logger logger = LoggerFactory.getLogger(Hive.class);

    private static String hadoopConf = System.getenv("HADOOP_CONF_DIR");

    private static String hadoopUserName;

    private Configuration configuration;

    private static Map<String, Object> hadoopConfigMap;

    private static String path;

    private boolean autoCreateTable = false;

    private static String store = "ORC";

    private static String writeMode = "APPEND";

    private static String compression = "NONE";

    private static String charsetName = "UTF-8";

    private Charset charset;

    private static String delimiter = "\001";

    @Required(required = true)
    private static String jdbcUrl;

    private String database;

    @Required(required = true)
    private static String username;

    @Required(required = true)
    private static String password;

    @Required(required = true)
    private static String analyticalRules;

    /**
     * 间隔 interval 时间对 outputFormat 进行一次 close，触发输出文件的合并
     */
    public static int interval = 60 * 60 * 1000;

    private long lastTime = System.currentTimeMillis();

    /**
     * 字节数量超过 bufferSize 时，outputFormat 进行一次 close，触发输出文件的合并
     */
    public static int bufferSize = 128 * 1024 * 1024;

    private AtomicLong dataSize = new AtomicLong(0L);

    private ScheduledExecutorService executor;

    private Map<String, TableInfo> tableInfos;

    @Required(required = true)
    private static String tablesColumn;

    private Map<String, HiveOutputFormat> hdfsOutputFormats = Maps.newConcurrentMap();

    private static Map<String, TableInfo> tableCache = new HashMap<>();

    private Lock lock = new ReentrantLock();

    private HiveUtil hiveUtil;

    static {
        Thread.currentThread().setContextClassLoader(null);
    }

    public Hive(Map config) {
        super(config);
        hiveUtil = new HiveUtil(jdbcUrl, username, password,writeMode);
    }

    @Override
    public void prepare() {
        try {
            charset = Charset.forName(charsetName);
            formatSchema();
            setHadoopConfiguration();
            process();
            if (Thread.currentThread().getContextClassLoader() == null) {
                Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
            }
        } catch (Exception e) {
            logger.error("", e);
            System.exit(-1);
        }
    }

    public void process() {
        executor = new ScheduledThreadPoolExecutor(1);
        executor.scheduleWithFixedDelay(() -> {
            if ((System.currentTimeMillis() - lastTime >= interval)
                    || dataSize.get() >= bufferSize) {
                try {
                    lock.lockInterruptibly();
                    release();
                    resetWriteStrategy();
                    logger.warn("hdfs commit again...");
                } catch (InterruptedException e) {
                    logger.error("{}", e);
                } finally {
                    lock.unlock();
                }
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    private void resetWriteStrategy(){
        dataSize.set(0L);
        lastTime = System.currentTimeMillis();
    }

    @Override
    protected void emit(Map event) {
        try {
            String tablePath = HiveConverter.regaxByRules(event, path);

            try {
                lock.lockInterruptibly();
                getHdfsOutputFormat(tablePath, event).writeRecord(event);
                dataSize.addAndGet(ObjectSizeCalculator.getObjectSize(event));
            } catch (Throwable e) {
                throw e;
            } finally {
                lock.unlock();
            }
        } catch (Throwable e) {
            this.addFailedMsg(event);
            logger.error("", e);
        }
    }

    public HiveOutputFormat getHdfsOutputFormat(String tablePath, Map event) throws IOException {
        HiveOutputFormat hdfsOutputFormat = hdfsOutputFormats.get(tablePath);
        TableInfo tableInfo = checkCreateTable(tablePath, event);
        if (hdfsOutputFormat == null) {
            if (StoreEnum.TEXT.name().equalsIgnoreCase(tableInfo.getStore())) {
                hdfsOutputFormat = new HiveTextOutputFormat(configuration, tableInfo.getPath(), tableInfo.getColumns(), tableInfo.getColumnTypes(), compression, writeMode, charset, tableInfo.getDelimiter());
            } else if (StoreEnum.ORC.name().equalsIgnoreCase(tableInfo.getStore())) {
                hdfsOutputFormat = new HiveOrcOutputFormat(configuration, tableInfo.getPath(), tableInfo.getColumns(), tableInfo.getColumnTypes(), compression, writeMode, charset);
            } else {
                throw new UnsupportedOperationException("The hdfs store type is unsupported, please use (" + StoreEnum.listStore() + ")");
            }
            hdfsOutputFormat.configure();
            hdfsOutputFormat.open();
            hdfsOutputFormats.put(tablePath, hdfsOutputFormat);

        }
        return hdfsOutputFormat;
    }

    private TableInfo checkCreateTable(String tablePath, Map event){
        if (!tableCache.containsKey(tablePath)){
            synchronized (Hive.class) {
                if (!tableCache.containsKey(tablePath)) {

                    logger.info("tablePath:{} even:{}", tablePath, event);

                    String tableName = tablePath;
                    if (autoCreateTable) {
                        tableName = MapUtils.getString(event, "table");
                    }
                    TableInfo tableInfo = tableInfos.get(tableName);
                    tableInfo.setTablePath(tablePath);
                    hiveUtil.createHiveTableWithTableInfo(tableInfo);
                    tableCache.put(tablePath, tableInfo);
                    return tableInfo;
                }
            }
        }
        return tableCache.get(tablePath);
    }


    @Override
    public void sendFailedMsg(Object msg) {
        emit((Map) msg);
    }

    @Override
    public synchronized void release() {
        Set<Map.Entry<String, HiveOutputFormat>> entrys = hdfsOutputFormats.entrySet();
        for (Map.Entry<String, HiveOutputFormat> entry : entrys) {
            try {
                entry.getValue().close();
                hdfsOutputFormats.remove(entry.getKey());
            } catch (Exception e) {
                logger.error("", e);
            }
        }
    }

    private void formatSchema() {
        tableInfos = new HashMap<String, TableInfo>();
        if (jdbcUrl.contains(";principal=")){
            String[] jdbcStr = jdbcUrl.split(";principal=");
            jdbcUrl = jdbcStr[0];
        }
        int anythingIdx = StringUtils.indexOf(jdbcUrl, '?');
        if (anythingIdx != -1) {
            database = StringUtils.substring(jdbcUrl, StringUtils.lastIndexOf(jdbcUrl, '/') + 1, anythingIdx);
        } else {
            database = StringUtils.substring(jdbcUrl, StringUtils.lastIndexOf(jdbcUrl, '/') + 1);
        }
        JSONObject tableColumnJson = JSON.parseObject(tablesColumn);
        for (Map.Entry<String, Object> entry : tableColumnJson.entrySet()) {
            String tableName = entry.getKey();
            List<Map<String, Object>> tableColumns = (List<Map<String, Object>>) entry.getValue();
            TableInfo tableInfo = new TableInfo(tableColumns.size());
            tableInfo.setDatabase(database);
            tableInfo.setTableName(tableName);
            for (Map<String, Object> column : tableColumns) {
                tableInfo.addColumnAndType(MapUtils.getString(column, HiveUtil.TABLE_COLUMN_KEY), MapUtils.getString(column, HiveUtil.TABLE_COLUMN_TYPE));
            }
            String createTableSql = HiveUtil.getCreateTableHql(tableColumns, delimiter, store);
            tableInfo.setCreateTableSql(createTableSql);

            logger.info("TableInfo:{}", tableInfo);

            tableInfos.put(tableName, tableInfo);
        }
        if (StringUtils.isBlank(analyticalRules)) {
            path = tableInfos.entrySet().iterator().next().getValue().getTableName();
        } else {
            path = analyticalRules;
            autoCreateTable = true;
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
        if (configuration == null) {
            synchronized (Hive.class) {
                if (configuration == null) {
                    configuration = new Configuration();
                    configuration.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
                    File[] xmlFileList = new File(hadoopConf).listFiles(new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                            if (name.endsWith(".xml")) {
                                return true;
                            }
                            return false;
                        }
                    });

                    if (xmlFileList != null) {
                        for (File xmlFile : xmlFileList) {
                            configuration.addResource(xmlFile.toURI().toURL());
                        }
                    }
                }
            }

        }
    }
}
