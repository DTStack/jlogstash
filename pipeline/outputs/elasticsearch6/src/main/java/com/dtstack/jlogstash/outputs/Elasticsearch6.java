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
package com.dtstack.jlogstash.outputs;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Iterator;
import java.util.HashSet;
import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.mapper.MapperException;
import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.render.Formatter;


@SuppressWarnings("serial")
public class Elasticsearch6 extends BaseOutput {
    private static final Logger logger = LoggerFactory.getLogger(Elasticsearch6.class);
    
    @Required(required=true)
    public static String index;
    
    public static String indexTimezone =null;

    public static String documentId;
    
    public static String documentType = "logs";
    
    public static String cluster;
    
    @Required(required=true)
    public static List<String> hosts;
    
    private static boolean sniff=true;

    private static Set<String> protectionKeySet;
    
    private static int bulkActions = 20000; 
    
    private static int bulkSize = 15;
    
    private static int  flushInterval = 5;//seconds
    
    private static int	concurrentRequests = 1;
        
    private BulkProcessor bulkProcessor;
    
    private TransportClient esclient;

    private static String protectionKeys = "message";

    private AtomicBoolean isClusterOn = new AtomicBoolean(true);
    
    private ExecutorService executor;
    
	public Elasticsearch6(Map config) {
        super(config);
    }

    public void prepare() {
    	try {
            protectionKeySet = new HashSet<>(Arrays.asList(protectionKeys.split(",")));
    		executor = Executors.newSingleThreadExecutor();
             this.initESClient();

            } catch (Exception e) {
                logger.error(e.getMessage());
                System.exit(1);
            }
    }


    private void initESClient() throws NumberFormatException,
            UnknownHostException {
    	    	
        Builder builder  =Settings.builder().put("client.transport.sniff", sniff);  
        if(StringUtils.isNotBlank(cluster)){
        	builder.put("cluster.name", cluster);
        }
        Settings settings = builder.build();
        esclient = new PreBuiltTransportClient(settings);
        TransportAddress[] addresss = new TransportAddress[hosts.size()];
        for (int i=0;i<hosts.size();i++) {
        	String host = hosts.get(i);
            String[] hp = host.split(":");
            String h = null, p = null;
            if (hp.length == 2) {
                h = hp[0];
                p = hp[1];
            } else if (hp.length == 1) {
                h = hp[0];
                p = "9300";
            }
            addresss[i] = new TransportAddress(
                    InetAddress.getByName(h), Integer.parseInt(p));
        }
        esclient.addTransportAddresses(addresss);
        executor.submit(new ClusterMonitor(esclient));
        bulkProcessor = BulkProcessor
                .builder(esclient, new BulkProcessor.Listener() {

                    @Override
                    public void afterBulk(long arg0, BulkRequest arg1,
                                          BulkResponse arg2) {
                    	
                        List<DocWriteRequest> requests = arg1.requests();
                        int toberetry = 0;
                        int totalFailed = 0;
                        for (BulkItemResponse item : arg2.getItems()) {
                            if (item.isFailed()) {
                                switch (item.getFailure().getStatus()) {
                                    case TOO_MANY_REQUESTS:
                                        if (totalFailed == 0) {
                                            logger.error("too many request {}:{}",item.getIndex(),item.getFailureMessage());
                                        }
                                        addFailedMsg(((IndexRequest) requests.get(item.getItemId())).sourceAsMap());
                                        break;
                                    case SERVICE_UNAVAILABLE:
                                        if (toberetry == 0) {
                                            logger.error("sevice unavaible cause {}:{}",item.getIndex(),item.getFailureMessage());
                                        }
                                        addFailedMsg(((IndexRequest) requests.get(item.getItemId())).sourceAsMap());
                                        break;
                                    default:
                                        Map<String, Object> sourceEvent = ((IndexRequest) requests.get(item.getItemId()))
                                                .sourceAsMap();
                                        logger.warn("bulk error,fail status={}, message={}, sourceEvent={}",
                                                item.getFailure().getStatus(), item.getFailure().getMessage(), sourceEvent);
                                        logger.error("bulk error", item.getFailure().getCause());
                                        doError(sourceEvent, item.getFailure().getCause());
                                        break;
                                }
                                totalFailed++;
                            }
                        }
                        

                        if (totalFailed > 0) {
                            logger.info(totalFailed + " doc failed, "
                                    + toberetry + " need to retry");
                        } else {
                            logger.debug("no failed docs");
                        }


                    }

                    @Override
                    public void afterBulk(long arg0, BulkRequest arg1,
                                          Throwable arg2) {
                        logger.error("bulk got exception:", arg2);
                        
                        for(DocWriteRequest request : arg1.requests()){
                        	addFailedMsg(request);
                        }
                        
                    }

                    @Override
                    public void beforeBulk(long arg0, BulkRequest arg1) {
                        logger.info("executionId: " + arg0);
                        logger.info("numberOfActions: "
                                + arg1.numberOfActions());
                    }
                })
                .setBulkActions(bulkActions)
                .setBulkSize(new ByteSizeValue(bulkSize, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(flushInterval))
                .setConcurrentRequests(concurrentRequests).build();
    }

    public void doEmit(Map event) {
        String _index = Formatter.format(event, index, indexTimezone);
        String _indexType = Formatter.format(event, documentType, indexTimezone);
        IndexRequest indexRequest;
        if (StringUtils.isBlank(documentId)) {
            indexRequest = new IndexRequest(_index, _indexType).source(event);
        } else {
            String _id = Formatter.format(event, documentId, indexTimezone);
            if(Formatter.isFormat(_id)){
                indexRequest = new IndexRequest(_index, _indexType).source(event);
            }else{
                indexRequest = new IndexRequest(_index, _indexType, _id).source(event);
            }
        }
        this.bulkProcessor.add(indexRequest);
    }

	public void emit(Map event) {

        logger.info("event enter,event={}", event);
        try {
            checkNeedWait();
            doEmit(event);
        } catch (Exception e) {
            logger.warn("emit error, event={}", event);
            logger.error("emit error", e);

            doError(event, e);

        }
    }

    public void doError(Map event, Throwable e) {
        if (!(e instanceof MapperException) && (e instanceof ElasticsearchException || e instanceof IOException)) {
            doErrorFirst(event);
        } else {
            doErrorCandidate(event);
        }
    }

    public void doErrorFirst(Map<String, Object> event) {

        logger.error("doErrorFirst event ={}", event);

        addFailedMsg(event);
    }

    public void doErrorCandidate(Map<String, Object> event) {

        boolean flag = false;
        for (Map.Entry<String, Object> entry : event.entrySet()) {
            if (!protectionKeySet.contains(entry.getKey())) {
                flag = true;
                break;
            }
        }

        if (flag == false) {
            logger.error("size equal protectionKeySet, not save, event={}", event);
            return;
        }

        Map<String, Object> newEvent = new HashMap();
        for (Iterator<String> s = protectionKeySet.iterator(); s.hasNext();) {
            String k = s.next();
            if (event.containsKey(k)) {
                newEvent.put(k, event.get(k));
            }
        }

        addFailedMsg(newEvent);

        logger.info("doErrorCandidate end,newEvent={}", newEvent);
    }

    @Override
    public void addFailedMsg(Object msg) {
        if (msg instanceof Map) {
            super.addFailedMsg(msg);
            return;
        }

        throw new IllegalArgumentException("addFailedMsg only accept Map instance");
    }

    @Override
    public void sendFailedMsg(Object msg) {

        try {

            checkNeedWait();

            Map<String, Object> event = (Map) msg;

            // 加入时间戳，用于计算耗时
            // event.put(esCreatedTimeKey, Public.getTimeStamp(DateTimeZone.UTC));

            logger.error("sendFailedMsg,msg={}", msg);
            emit(event);

        } catch (Exception e) {
            logger.error("sendFailedMsg error", e);

            if (!(e instanceof MapperException) && (e instanceof ElasticsearchException || e instanceof IOException)) {
                addFailedMsg(msg);
            }
        }

    }


    @Override
    public void release(){
    	if(bulkProcessor!=null)bulkProcessor.close();
    }

    public void checkNeedWait() {
        while (!isClusterOn.get()) {// 等待集群可用
            try {
                logger.warn("wait cluster avaliable...");
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                logger.error("", e);
            }
        }

    }


    class ClusterMonitor implements Runnable{
    	
    	private TransportClient transportClient;
    	
    	public ClusterMonitor(TransportClient client) {
    		this.transportClient = client;
		}

		@Override
		public void run() {
			while(true) {
	    	    try {
	    	        logger.debug("getting es cluster health.");
	    	        ActionFuture<ClusterHealthResponse> healthFuture = transportClient.admin().cluster().health(Requests.clusterHealthRequest());
	    	        ClusterHealthResponse healthResponse = healthFuture.get(5, TimeUnit.SECONDS);
	    	        logger.debug("Get num of node:{}", healthResponse.getNumberOfNodes());
	    	        logger.debug("Get cluster health:{} ", healthResponse.getStatus());
	    	        isClusterOn.getAndSet(true);
	    	    } catch(Throwable t) {
	    	        if(t instanceof NoNodeAvailableException){//集群不可用
	    	        	logger.error("the cluster no node avaliable.");
	    	        	isClusterOn.getAndSet(false);
                    }else{
	    	        	isClusterOn.getAndSet(true);
                    }
	    	    }
	    	    try {
	    	        Thread.sleep(1000);//FIXME
	    	    } catch (InterruptedException ie) { 
	    	    	ie.printStackTrace(); 
	    	    }
	    	}
		}	
    }
}
