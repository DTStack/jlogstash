package com.dtstack.jlogstash.format.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: haisi
 * @date 2019-06-18 14:44
 */

public class HiveConverter {

    private static Logger logger = LoggerFactory.getLogger(HiveConverter.class);

    private static Pattern pat1 = Pattern.compile( "\\$\\{.*?\\}");

    /**
     *
     * @param path
     * @return
     */
    public static String regaxByRules(Map output, String path) {
        try {
            Matcher mat1 = pat1.matcher(path);
            while (mat1.find()) {
                String pkey =  mat1.group();
                String key = pkey.substring(2, pkey.length() - 1);
                Object value = output.get(key);
                if(value !=null){
                    path =  path.replace(pkey,value.toString());
                }
            }
        } catch (Exception e) {
            logger.error("parser path rules is fail", e);
        }
        return path;
    }

}

