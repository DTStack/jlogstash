package com.dtstack.jlogstash.inputs;

import java.util.HashMap;
import java.util.Map;

/**
 * 包含data及其sequence和identityStream。还有batch。
 * copy from https://github.com/elastic/java-lumber
 *
 */
public class Message implements Comparable<Message> {
    private int sequence;
    private String identityStream;
    private Map data;
    private Batch batch;

    public Message(int sequence, Map map) {
        setSequence(sequence);
        setData(map);
    }

    public int getSequence() {
        return sequence;
    }

    public void setSequence(int sequence) {
        this.sequence = sequence;
    }

    public Map getData() {
        return data;
    }

    public void setData(Map data) {
        this.data = data;
    }

    @Override
    public int compareTo(Message o) {
        return Integer.compare(this.getSequence(), o.getSequence());
    }

    public Batch getBatch() {
        return batch;
    }

    public void setBatch(Batch batch) {
        this.batch = batch;
    }

    public String getIdentityStream() {
        if(this.identityStream == null) {
            Map beatsData = (HashMap<String, String>) this.getData().get("beat");

            if(beatsData != null) {
                String id = (String) beatsData.get("id");
                String resourceId = (String) beatsData.get("resource_id");

                if(id != null && resourceId != null) {
                    this.identityStream = id + "-" + resourceId;
                } else {
                    this.identityStream = (String) beatsData.get("name") + "-" + (String) beatsData.get("source");
                }
            }
        }

        return this.identityStream;
    }
}