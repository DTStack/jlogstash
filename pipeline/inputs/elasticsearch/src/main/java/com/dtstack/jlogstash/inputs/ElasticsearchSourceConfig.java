package com.dtstack.jlogstash.inputs;

import java.util.List;

/**
 * ES数据源配置
 *
 * @author zxb
 * @version 1.0.0
 *          2017年03月24日 09:06
 * @since Jdk1.6
 */
public class ElasticsearchSourceConfig {

    private String cluster;

    private List<String> hosts;

    private boolean sniff;

    public ElasticsearchSourceConfig(String cluster, List<String> hosts, boolean sniff) {
        this.cluster = cluster;
        this.hosts = hosts;
        this.sniff = sniff;
    }

    public ElasticsearchSourceConfig() {
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public List<String> getHosts() {
        return hosts;
    }

    public void setHosts(List<String> hosts) {
        this.hosts = hosts;
    }

    public boolean isSniff() {
        return sniff;
    }

    public void setSniff(boolean sniff) {
        this.sniff = sniff;
    }
}
