package com.dtstack.jlogstash.inputs;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * ES client工具类
 *
 * @author zxb
 * @version 1.0.0
 *          2016年12月23日 15:38
 * @since Jdk1.6
 */
public class EsClientHelper {

    private static final Logger logger = LoggerFactory.getLogger(EsClientHelper.class);


    /**
     * 初始化并创建ES client
     *
     * @param config ES源配置
     * @return
     */
    public static Client initClient(ElasticsearchSourceConfig config) throws UnknownHostException {
        // construct settings
        Settings.Builder builder = Settings.settingsBuilder();
        if (StringUtils.isNotEmpty(config.getCluster())) {
            builder.put("cluster.name", config.getCluster());
        }
        builder.put("client.transport.sniff", config.isSniff());
        Settings settings = builder.build();

        // build client
        TransportClient client = TransportClient.builder().settings(settings).build();

        // add node ip list
        List<String> hostList = config.getHosts();
        for (String host : hostList) {
            if (StringUtils.isNotEmpty(host)) {
                String[] hostAndPort = host.split(":");
                String hostAddr = hostAndPort[0];
                Integer port = 9300;
                if (hostAndPort.length > 1) {
                    try {
                        port = Integer.valueOf(hostAndPort[1]);
                    } catch (NumberFormatException e) {
                        logger.error(String.format("parse port '%s' error, use default port 9300", hostAndPort), e);
                    }
                }
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostAddr), port));
            }
        }
        return client;
    }

    /**
     * 释放ES Client
     *
     * @param client
     */
    public static void releaseClient(Client client) {
        if (client != null) {
            client.close();
        }
    }
}
