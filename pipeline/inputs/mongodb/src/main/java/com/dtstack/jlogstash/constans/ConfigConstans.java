package com.dtstack.jlogstash.constans;

/**
 * @author zxb
 * @version 1.0.0
 *          2017年03月23日 09:19
 * @since Jdk1.6
 */
public class ConfigConstans {

    public static final String DEFAULT_META_FILE_PATH = System.getProperty("java.io.tmpdir") + System.getProperty("line.separator") + ".last_run_metadata_path";

    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    private ConfigConstans() {
    }
}
