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

public class PathConverterUtil {

    private static Logger logger = LoggerFactory.getLogger(PathConverterUtil.class);

    private static Pattern pat1 = Pattern.compile("\\$\\{.*?\\}");

    private static String KEY_TABLE = "table";

    /**
     * @param path
     * @return
     */
    public static String regaxByRules(Map output, String path, Map<String, String> distributeTableMapping) {
        try {
            Matcher mat1 = pat1.matcher(path);
            while (mat1.find()) {
                String pkey = mat1.group();
                String key = pkey.substring(2, pkey.length() - 1);
                String value = output.get(key).toString();
                if (KEY_TABLE.equals(key)) {
                    value = distributeTableMapping.getOrDefault(value, value);
                }
                if (value != null) {
                    //.在sql中会视为db.table的分隔符，需要单独过滤特殊字符 '.'
                    path = path.replace(pkey, value).replace(".", "_");
                }
            }
        } catch (Exception e) {
            logger.error("parser path rules is fail", e);
        }
        return path;
    }

}

