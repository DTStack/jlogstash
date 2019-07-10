package com.dtstack.jlogstash.format.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: haisi
 * @date 2019-06-18 14:44
 */

public class HiveConverter {

    private static Logger logger = LoggerFactory.getLogger(HiveConverter.class);

    /**
     * path的参数格式，如$.user.id,若不是,返回原path
     * 解析成功返回解析后字符串,解析失败则返回null
     *
     * @param output
     * @param path
     * @return
     */
    public static String parseJson(Map output, String path) {
        try {
            String[] keySs = path.split("\\/");
            String result = "";
            for (String keYs : keySs) {
                final char c = '$';
                if (keYs.length() == 0) {
                    continue;
                }
                if (keYs.charAt(0) != c) {
                    result += String.format("/%s", keYs);
                    continue;
                }
                String[] keys = keYs.split("\\.");
                int len = keys.length;
                Object obj = output;
                boolean isObjList = false;
                for (int i = 1; i < len; i++) {
                    String key = keys[i];
                    int index = 0;
                    if (key.contains("[") && key.contains("]")) {
                        int leftIndex = key.indexOf("[");
                        int rightIndex = key.indexOf("]");
                        index = Integer.parseInt(key.substring(leftIndex + 1, rightIndex));
                        key = key.substring(0, key.indexOf("["));
                        isObjList = true;
                    }
                    if (isObjList) {
                        obj = ((Map) obj).get(key);
                    }
                    if (obj instanceof Map && !isObjList) {
                        obj = ((Map) obj).get(key);
                    } else if (obj instanceof List) {
                        obj = ((List) obj).get(index);
                        isObjList = false;
                    } else {
                        break;
                    }
                }
                if (obj instanceof Integer) {
                    result += String.format("/%d", obj);
                } else if (obj instanceof String) {
                    result += String.format("/%s", obj);
                } else {
                    throw new UnsupportedOperationException("unsupported this format");
                }
            }
            return result;
        } catch (Exception e) {
            logger.error("", e);
        }
        return path;
    }

    /**
     * nanqi_{$.table}
     * 使用正则把{$.table}分离出来
     * 并且使用上面的parseJson函数提取拼接
     *
     * @param path
     * @return
     */
    public static String regaxByRules(Map output, String path) {
        StringBuilder res = new StringBuilder();
        String p = "";
        try {
            String pattern = "\\{\\$.*?}";
            Pattern pat1 = Pattern.compile(pattern);
            Matcher mat1 = pat1.matcher(path);
            while (mat1.find()) {
                String newPath = parseJson(output, mat1.group().substring(1, mat1.group().length() - 1));
                res.append(newPath);
                String tPath = String.valueOf(res).replace("/", "");
                p = path.replace(mat1.group(), tPath);
            }
            return p;
        } catch (Exception e) {
            logger.error("", e);
        }
        return path;
    }
}

