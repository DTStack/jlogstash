package com.dtstack.jlogstash;

import java.io.IOException;

/**
 * Created by sishu.yss on 2018/11/17.
 */
public class Test {

    public static void main(String[] args) throws IOException {
        String[] cmd = { "sh", "-c", "ls > /Users/sishuyss/1.txt" };
        Runtime.getRuntime().exec(cmd);
    }
}
