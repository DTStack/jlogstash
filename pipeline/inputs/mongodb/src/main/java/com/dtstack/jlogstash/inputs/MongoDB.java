package com.dtstack.jlogstash.inputs;

import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.exception.ExceptionUtil;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
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
public class MongoDB extends BaseInput {

    private static final Logger logger = LoggerFactory.getLogger(MongoDB.class);

    private static ObjectMapper objectMapper = new ObjectMapper();

    // 配置字段
    /**
     * MongoDB连接URI
     */
    @Required(required = true)
    private String uri;

    /**
     * database名称
     */
    @Required(required = true)
    private String dbName;

    /**
     * collection名称
     */
    @Required(required = true)
    private String collection;

    /**
     * Filter语句
     */
    private String query;

    /**
     * 增量抽取的起始时间
     */
    private volatile String sinceTime;

    /**
     * 可选值：_id，time。是基于_id还是基于时间戳字段
     */
    private String since_type = "id";

    /**
     * 增量基于的字段
     */
    private String since_column = "_id";

    private String lastRunMetadataPath;

    private Integer interval;

    // 需要转换为byte[]的Binary对象
    private List<String> binaryFields;

    private boolean needConvertBin;

    /**
     * 是否调度
     */
    private boolean schedule = false;

    // 属性字段
    private MongoClient mongoClient;

    private MongoDatabase database;

    private MongoCollection<Document> coll;

    private StartTime startTime;

    private Scheduler scheduler;

    private Document queryDocument;

    public MongoDB(Map config) {
        super(config);
    }

    @Override
    public void prepare() {
        // 初始化增量的开始时间
        startTime = new StartTime(sinceTime, lastRunMetadataPath);

        // 获取用户自定义的筛选条件
        queryDocument = parseQueryDocument(query);

        // 获取是否需要转换Binary对象为byte[]
        if (binaryFields != null && binaryFields.size() > 0) {
            needConvertBin = true;
        }

        // 连接client
        mongoClient = new MongoClient(new MongoClientURI(uri));
        database = mongoClient.getDatabase(dbName);
        coll = database.getCollection(collection);
    }

    @Override
    public void emit() {
        Task task = new InputMongoDBTask();
        if (schedule) {
            scheduler = new SimpleScheduler(task, interval.longValue()); // 可以扩展实现
            scheduler.start();
        } else {
            task.execute();
        }
    }

    @Override
    public void release() {
        if (scheduler != null) {
            scheduler.stop();
        }

        startTime.persist();

        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    private Long getNextDuration() {
        if (scheduler != null) {
            return scheduler.getNextDuration();
        }
        return null;
    }

    /**
     * 获取过滤条件
     *
     * @return
     */
    private Document getFilterDoc() {
        DateTime start = startTime.get();
        if (start == null) {
            return queryDocument;
        }

        DateTime end = start.plus(getNextDuration());

        Map<String, Object> condition = new HashMap<String, Object>();
        if ("id".equals(since_type)) {
            condition.put("$gte", new ObjectId(start.toDate()));
            condition.put("$lt", new ObjectId(end.toDate()));
        } else {
            condition.put("$gte", start);
            condition.put("$lt", end);
        }
        queryDocument.put(since_column, condition);
        return queryDocument;
    }

    private DateTime getLastDateTime(Object lastDateValue) {
        if (lastDateValue == null) {
            return null;
        }

        // ObjectId类型
        if ("id".equals(since_type)) {
            ObjectId objectId = (ObjectId) lastDateValue;
            return new DateTime(objectId.getDate());
        } else {
            Class<?> clazz = lastDateValue.getClass();
            if (String.class.isAssignableFrom(clazz)) {
                // TODO format
            } else if (BsonTimestamp.class.isAssignableFrom(clazz)) {
                // TODO convert
            }
        }
        return null;
    }

    /**
     * 将filter语句转换为Document对象
     *
     * @param query query语句
     * @return
     */
    private Document parseQueryDocument(String query) {
        Document document = null;
        Map<String, Object> queryMap = null;

        if (StringUtils.isNotEmpty(query)) {
            try {
                queryMap = objectMapper.readValue(query, Map.class);
            } catch (IOException e) {
                throw new IllegalArgumentException(String.format("parse filter cause error :%s\n%s", query, ExceptionUtil.getErrorMessage(e)));
            }
        }

        if (queryMap != null) {
            document = new Document(queryMap);
        } else {
            document = new Document();
        }
        return document;
    }

    class InputMongoDBTask implements Task {
        @Override
        public void execute() {
            // 获取过滤条件
            Document filterDoc = getFilterDoc();
            if (logger.isDebugEnabled()) {
                logger.debug("timestamp ：{}", startTime);
                logger.debug("filter cause ：{}", filterDoc);
            }

            // 查找数据
            Document document = null;
            MongoCursor<Document> cursor = null;
            try {
                cursor = coll.find(filterDoc).iterator();
                while (cursor.hasNext()) {
                    document = cursor.next();
                    handleBinary(document);
                    process(document);
                }

                if (document != null) {
                    DateTime dateTime = getLastDateTime(document.get(since_column));
                    startTime.updateIfNull(dateTime);
                }

                // 更新起始时间
                startTime.add(getNextDuration());
            } finally {
                if (cursor != null) {
                    cursor.close();
                }
            }
        }

    }

    private void handleBinary(Document document) {
        if (needConvertBin) {
            for (String binaryField : binaryFields) {
                Object object = document.get(binaryField);
                if (object != null && object instanceof Binary) {
                    Binary binary = (Binary) object;
                    document.put(binaryField, binary.getData());
                }
            }
        }
    }
}
