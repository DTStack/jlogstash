package com.dtstack.jlogstash.utils;

public enum DataType {
    LIST("list")
    ,STRING("string")
    ,SET("set")
    ,HASH("hash")
    ,SORTEDSET("sorted_set")
    ,CHANNEL("channel")
    ,CHANNEL_PATTERN("channel_pattern");

    private String dataType;

    DataType(String dataType){
        this.dataType = dataType;
    }

    public String getDataType() {
        return dataType;
    }
}
