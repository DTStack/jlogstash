package com.dtstack.jlogstash.inputs;

import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.date.DateParser;
import com.dtstack.jlogstash.date.FormatParser;
import com.dtstack.jlogstash.exception.ExceptionUtil;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MongoDB输入源
 *
 * @author zxb
 * @version 1.0.0
 *          2017年03月19日 17:15
 * @since Jdk1.6
 */
public class MongoDBTest extends BaseInput {

    private static final Logger logger = LoggerFactory.getLogger(MongoDBTest.class);

    private static final DateParser dateParser = new FormatParser("yyyy-MM-dd HH:mm:ss.SSS", null, null);

    private static ObjectMapper objectMapper = new ObjectMapper();

    /**
     * MongoDB连接URI
     */
    @Required(required = true)
    private String uri;

    /**
     * database名称
     */
    @Required(required = true)
    private String db_name;

    /**
     * collection名称
     */
    @Required(required = true)
    private String collection;

    /**
     * Filter语句
     */
    private String filter;

    /**
     * binFields，将会把binFields转换为byte[]
     */
    private List<String> bin_fields;

    /**
     * 元数据文件路径，元数据文件存储since_time
     */
    private String last_run_metadata_path = System.getProperty("java.io.tmpdir") + System.getProperty("line.separator") + ".last_run_metadata_path";

    /**
     * 增量抽取的起始时间
     */
    private volatile String since_time;

    /**
     * 可选值：_id，time。是基于_id还是基于时间戳字段
     */
    private String since_type = "id";

    /**
     * 增量基于的字段
     */
    private String since_column = "_id";

    private String since_column_format = "yyyy-MM-dd HH:mm:ss.SSS";

    /**
     * 增量抽取间隔，单位ms
     */
    private Integer interval;

    private ScheduleThread scheduleThread;

    private boolean convertBin = false;

    private Object since_date_time;

    private MongoClient mongoClient;

    private MongoDatabase database;

    private MongoCollection<Document> coll;

    private Document filterDocument;

    public MongoDBTest(Map config) {
        super(config);
    }

    @Override
    public void prepare() {
        // 获取是否需要转换Binary
        if (bin_fields != null && bin_fields.size() > 0) {
            convertBin = true;
        }

        // 准备since_time
        prepareSinceTime();

        // 将filter查询语句转换为Document对象
        filterDocument = parseFilterDocument(filter);

        // 连接client
        mongoClient = new MongoClient(new MongoClientURI(uri));
        database = mongoClient.getDatabase(db_name);
        coll = database.getCollection(collection);
    }

    private void prepareSinceTime() {
        if (StringUtils.isEmpty(since_time)) {
            since_time = readFromFile();
        }

        if (StringUtils.isNotEmpty(since_time)) {
            if ("_id".equals(since_type)) {
                since_date_time = new ObjectId(dateParser.parse(since_time).toDate());
            } else {
                since_date_time = dateParser.parse(since_time);
            }
        }
    }

    /**
     * 将filter语句转换为Document对象
     *
     * @param filter filter语句
     * @return
     */
    private Document parseFilterDocument(String filter) {
        Document document = null;
        Map<String, Object> filterMap = null;

        if (StringUtils.isNotEmpty(filter)) {
            try {
                filterMap = objectMapper.readValue(filter, Map.class);
            } catch (IOException e) {
                throw new IllegalArgumentException(String.format("filter语句读取解析失败:%s\n%s", filter, ExceptionUtil.getErrorMessage(e)));
            }
        }

        if (filterMap != null) {
            document = new Document(filterMap);
        } else {
            document = new Document();
        }
        return document;
    }

    @Override
    public void emit() {
        scheduleThread = new ScheduleThread();
        scheduleThread.setDaemon(false);
        scheduleThread.start();
    }

    @Override
    public void release() {
        scheduleThread.stopThread();

        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    private void convertBin2ByteArr(Document document) {
        // 获取其中的二进制字段转换为字节数组
        try {
            for (String binField : bin_fields) {
                Binary binary = (Binary) document.get(binField);
                if (binary != null) {
                    document.put(binField, binary.getData());
                }
            }
        } catch (Exception e) {
            logger.error(String.format("将BsonBinary转换为byte[]出错：%s", document), e);
        }
    }

    private String readFromFile() {
        File file = new File(last_run_metadata_path);
        if (file.exists()) {
            try {
                FileInputStream input = new FileInputStream(file);
                Yaml yaml = new Yaml();
                return (String) yaml.load(input);
            } catch (FileNotFoundException e) {
                logger.error(String.format("file not found, path: %s", last_run_metadata_path), e);
            }
        }
        return null;
    }

    private void updateSinceTime() {
        if (since_date_time == null) {
            return;
        }

        if (since_date_time instanceof ObjectId) {
            ObjectId objectId = (ObjectId) since_date_time;
            long timestamp = objectId.getTimestamp() + interval;
            since_date_time = new ObjectId(new Date(timestamp));
        } else {
            DateTime dateTime = (DateTime) since_date_time;
            since_date_time = dateTime.plus(interval);
        }

        File file = new File(last_run_metadata_path);
        Yaml yaml = new Yaml();
        try {
            yaml.dump(since_date_time, new FileWriter(file));
        } catch (IOException e) {
            logger.error("save since_date_time error!", e);
        }
    }

    /**
     * 数据增量调度
     */
    class ScheduleThread extends Thread {

        private volatile boolean stopFlag = false;

        @Override
        public void run() {
            while (!stopFlag) {
                if (since_date_time != null) {
                    Map<String, Object> condition = new HashMap<String, Object>();
                    condition.put("$lt", since_date_time);
                    filterDocument.put(since_column, condition);
                }

                coll.find(filterDocument).forEach(new Block<Document>() {
                    @Override
                    public void apply(final Document document) {
                        if (convertBin) {
                            convertBin2ByteArr(document);
                        }
                        process(document);
                    }
                });

                if (interval == null) {
                    stopThread();
                    continue;
                }

                updateSinceTime();

                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    logger.error("Interrupted", e);
                }
            }
        }

        public void stopThread() {
            stopFlag = true;
        }
    }
}
