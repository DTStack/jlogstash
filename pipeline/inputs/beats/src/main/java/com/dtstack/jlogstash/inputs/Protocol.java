package com.dtstack.jlogstash.inputs;


/**
 * copy from https://github.com/elastic/java-lumber
 *
 */
public class Protocol {
    public static final byte VERSION_1 = '1';
    public static final byte VERSION_2 = '2';

    public static final byte CODE_WINDOW_SIZE = 'W';
    public static final byte CODE_JSON_FRAME = 'J';
    public static final byte CODE_COMPRESSED_FRAME = 'C';
    public static final byte CODE_FRAME = 'D';

    public static boolean isVersion2(byte versionRead) {
        if(Protocol.VERSION_2 == versionRead){
            return true;
        } else {
            return false;
        }
    }
}