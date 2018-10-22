package com.dtstack.jlogstash.inputs;

import com.dtstack.jlogstash.constans.ConfigConstans;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 起始时间戳
 *
 * @author zxb
 * @version 1.0.0
 *          2017年03月22日 14:52
 * @since Jdk1.6
 */
public class StartTime {

    /**
     * 元数据文件路径
     */
    private String metadataPath;

    /**
     * 起始时间
     */
    private DateTime sinceTime;

    private SimpleDateFormat sdf = new SimpleDateFormat(ConfigConstans.DEFAULT_DATE_FORMAT);

    public StartTime(String since_time, String last_run_metadata_path) {
        this.metadataPath = last_run_metadata_path;
        if (StringUtils.isEmpty(last_run_metadata_path)) {
            this.metadataPath = ConfigConstans.DEFAULT_META_FILE_PATH;
        }
        init(since_time);
    }

    public void updateIfNull(DateTime dateTime) {
        if (sinceTime == null) {
            sinceTime = dateTime;
        }
    }

    /**
     * 增加时间
     *
     * @param increment
     */
    public void add(Long increment) {
        if (increment != null) {
            sinceTime = sinceTime.plus(increment);
        }
    }

    /**
     * 获取起始时间
     *
     * @return
     */
    public DateTime get() {
        if (sinceTime == null) {
            return null;
        }
        return new DateTime(sinceTime.getMillis());
    }

    /**
     * 初始化上次增量时间
     */
    private void init(String since_time) {
        String sinceTimeStr = null;
        if (StringUtils.isEmpty(since_time)) {
            File file = new File(metadataPath);
            if (file.exists()) {
                // 从文件读取时间
                FileInputStream input = null;
                Yaml yaml = new Yaml();
                try {
                    input = new FileInputStream(file);
                    sinceTimeStr = (String) yaml.load(input);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } finally {
                    if (input != null) {
                        try {
                            input.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        } else {
            sinceTimeStr = since_time;
        }

        if (StringUtils.isNotEmpty(sinceTimeStr)) {
            try {
                sinceTime = new DateTime(sdf.parse(sinceTimeStr));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 持久化增量时间
     */
    public void persist() {
        if (sinceTime != null) {
            // 写入到文件
            File file = new File(metadataPath);
            // 从文件读取时间
            FileWriter fileWriter = null;
            Yaml yaml = new Yaml();
            try {
                fileWriter = new FileWriter(file);
                yaml.dump(file, fileWriter);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (fileWriter != null) {
                    try {
                        fileWriter.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        if (sinceTime != null) {
            return sinceTime.toString(ConfigConstans.DEFAULT_DATE_FORMAT);
        }
        return null;
    }
}
