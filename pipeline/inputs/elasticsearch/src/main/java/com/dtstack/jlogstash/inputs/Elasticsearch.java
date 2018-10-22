package com.dtstack.jlogstash.inputs;

import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.exception.InitializeException;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Elasticsearch 2.3.2 java 读取
 *
 * @author zxb
 * @version 1.0.0
 *          2017年03月23日 18:34
 * @since Jdk1.6
 */
public class Elasticsearch extends BaseInput {

    private static final Logger logger = LoggerFactory.getLogger(Elasticsearch.class);

    /**
     * If set, include Elasticsearch document information such as index, type, and the id in the event
     */
    private boolean docinfo;

    /**
     * List of document metadata to move to the docinfo_target field
     */
    private List<String> docinfo_fields = Arrays.asList("_index", "_type", "_id");

    private String docinfo_target = "@metadata";

    @Required(required = true)
    private List<String> hosts;

    private String cluster = "elasticsearch";

    private boolean sniff = true;

    private String index = "logstash-*";

    private String type;

    private String query = "{\"query\": {\"match_all\":{}},\"sort\" : [\"_doc\"]}";

    private Integer scroll = 5;

    private Integer size = 1000;

    private boolean ssl;

    private List<String> tags;

    private String user;

    private String password;

    private TransportClient client;

    private volatile boolean stop;

    private boolean indexMetadata;

    private boolean typeMetadata;

    private boolean idMetadata;

    public Elasticsearch(Map config) {
        super(config);
    }

    public void prepare() {
        ElasticsearchSourceConfig esConfig = new ElasticsearchSourceConfig(cluster, hosts, sniff);
        try {
            client = (TransportClient) EsClientHelper.initClient(esConfig);
        } catch (UnknownHostException e) {
            throw new InitializeException(String.format("init elasticsearch client error, unknown host %s", hosts), e);
        }

        if (docinfo_fields.contains("_index")) {
            indexMetadata = true;
        }
        if (docinfo_fields.contains("_type")) {
            typeMetadata = true;
        }
        if (docinfo_fields.contains("_id")) {
            idMetadata = true;
        }
    }

    public void emit() {
        // 构建查询
        Scroll esScroll = new Scroll(new TimeValue(scroll, TimeUnit.MINUTES));
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setScroll(esScroll).setQuery(query).setSize(size);
        if (StringUtils.isNotEmpty(type)) {
            searchRequestBuilder.setTypes(type);
        }

        if (logger.isDebugEnabled()) {
            logger.debug(searchRequestBuilder.toString());
        }

        String scrollId = null;
        try {
            SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
            scrollId = searchResponse.getScrollId();

            long total = searchResponse.getHits().getTotalHits();
            // 处理第一次search查询
            int messageCount = submitMessage(searchResponse);

            // 计算剩余的批次
            long remainTotal = total - messageCount;
            long batchNum = remainTotal % size == 0 ? remainTotal / size : (remainTotal / size) + 1;

            for (long i = 0; !stop && i < batchNum; i++) {
                // 按批查询数据
                SearchScrollRequestBuilder scrollRequestBuilder = client.prepareSearchScroll(scrollId).setScroll(esScroll);
                searchResponse = scrollRequestBuilder.execute().actionGet();
                submitMessage(searchResponse);
            }
        } catch (Exception e) {
            logger.error("query error", e);
        } finally {
            if (StringUtils.isNotEmpty(scrollId)) {
                client.prepareClearScroll().addScrollId(scrollId).execute().actionGet();
            }
        }
    }

    public void release() {
        EsClientHelper.releaseClient(client);
    }

    /**
     * 处理响应结果
     *
     * @param searchResponse
     * @return 处理后的结果
     * @Title 处理查询响应结果
     * @category 处理查询响应结果
     * @Description: 循环遍历解析searchResponse，并处理高亮数据，将数据最终以List<Map
     * <String,Object>>形式装载并返回。 返回结果太多可能会导致内存消耗剧增
     */
    private int submitMessage(SearchResponse searchResponse) throws InterruptedException {

		/* 判断是否有响应结果 */
        SearchHits hits = searchResponse.getHits();

		/* 遍历结果集 */
        SearchHit[] hitsArr = hits.getHits();
        int len = hitsArr.length;
        for (int i = 0; i < len; i++) {
            SearchHit hit = hitsArr[i];
            Map<String, Object> row = hit.getSource(); // 一整行数据

            // 添加doc元数据信息
            if (docinfo) {
                Map<String, String> docInfoMap = new HashMap<String, String>();

                if (indexMetadata) {
                    docInfoMap.put("_index", hit.getIndex());
                }
                if (typeMetadata) {
                    docInfoMap.put("_type", hit.getType());
                }
                if (idMetadata) {
                    docInfoMap.put("_id", hit.getId());
                }

                row.put(docinfo_target, docInfoMap);
            }

            process(row);
        }
        return len;
    }
}
