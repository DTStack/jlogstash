package com.dtstack.jlogstash.inputs;


import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;


public class BinlogPosUtil {

    private static final Logger logger = LoggerFactory.getLogger(BinlogEventSink.class);

    private static final Gson gson = new Gson();

    private BinlogPosUtil() {
        // Where did we come from? And why is the universe the way it is?
    }

    public static void savePos(String id, EntryPosition entryPosition) throws IOException {
        try(JsonWriter jsonWriter = new JsonWriter(new PrintWriter(new FileOutputStream(id)))) {
            gson.toJson(entryPosition, EntryPosition.class, jsonWriter);
        }
    }

    public static EntryPosition readPos(String id) throws IOException {
        try(JsonReader jsonReader = new JsonReader(new InputStreamReader(new FileInputStream(id)))) {
            return gson.fromJson(jsonReader, EntryPosition.class);
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println(readPos("shit.json"));
    }


}
