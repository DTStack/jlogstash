package com.dtstack.jlogstash.filters;

import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.date.UnixMSParser;
import com.dtstack.jlogstash.render.Formatter;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.math.NumberUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 逻辑从outputplugin-performance复制
 * 将指定间隔内的消息数量输出到指定文件
 * Date: 2017/3/23
 * Company: www.dtstack.com
 * @ahthor xuchao
 */

public class Performance extends BaseFilter {

    private static Logger logger = LoggerFactory.getLogger(Performance.class);

    private static AtomicLong eventNumber = new AtomicLong(0);

    private static int interval = 30;//seconds

    /**清理文件执行间隔时间*/
    private static long monitorFileInterval = 10 * 60;//seconds

    private static String timeZone = "UTC";

    @Required(required=true)
    private static String path;

    /**key:文件路径, value:保留天数*/
    private static Map<String, String> monitorPath;

    private Map<String, String> fileTimeFormatMap = Maps.newHashMap();

    private Map<String, String> fileNameRegMap = Maps.newHashMap();

    private Map<String, String> pathDicMap = Maps.newHashMap();

    private static ExecutorService executor = Executors.newSingleThreadExecutor();

    private static ScheduledExecutorService scheduleExecutor = Executors.newScheduledThreadPool(1);

    public Performance(Map<String,Object> config) {
        super(config);
    }

    public void prepare() {
        compileTimeInfo();
        executor.submit(new PerformanceEventRunnable());

        if(monitorPath != null && monitorPath.size() != 0){
            scheduleExecutor.scheduleWithFixedDelay(new ExpiredFileDelRunnable(), 0, monitorFileInterval, TimeUnit.SECONDS);
        }
    }

    protected Map filter(Map event) {
        eventNumber.getAndIncrement();
        return event;
    }

    public void compileTimeInfo(){

        if(monitorPath == null || monitorPath.size() == 0){
            logger.info("not setting monitorPath");
            return;
        }

        Pattern filePattern = Pattern.compile("^(.*)(/|\\\\)([^/\\\\]*(\\%\\{\\+?(.*?)\\})\\S+)$");

        for(Map.Entry<String, String> tmp : monitorPath.entrySet()){

            Matcher matcher = filePattern.matcher(tmp.getKey());
            if(!matcher.find()){
                logger.error("input path:{} can not matcher to the pattern.");
                continue;
            }

            String fileName = matcher.group(3);
            String timeStr = matcher.group(4);
            String timeFormat = matcher.group(5);
            String fileNamePattern = fileName.replace(timeStr, "(\\S+)");

            pathDicMap.put(tmp.getKey(), matcher.group(1));
            fileNameRegMap.put(tmp.getKey(), fileNamePattern);
            fileTimeFormatMap.put(tmp.getKey(), timeFormat);
        }

    }

    class PerformanceEventRunnable implements Runnable{

        @Override
        public void run() {
            while(true){
                BufferedWriter bufferedWriter = null;
                FileWriter fw = null;
                try {
                    Thread.sleep(interval*1000);
                    StringBuilder sb = new StringBuilder();
                    long number =eventNumber.getAndSet(0);
                    DateTime dateTime =new UnixMSParser().parse(String.valueOf(Calendar.getInstance().getTimeInMillis()));
                    sb.append(dateTime.toString()).append(" ").append(number).append(System.getProperty("line.separator"));
                    String newPath = Formatter.format(new HashMap<String,Object>(), path, timeZone);
                    fw = new FileWriter(newPath,true);
                    bufferedWriter = new BufferedWriter(fw);
                    bufferedWriter.write(sb.toString());
                    bufferedWriter.flush();
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }finally{
                    try{
                        if (bufferedWriter != null)bufferedWriter.close();
                        if (fw!=null)fw.close();
                    }catch(Exception e){
                        logger.error(e.getMessage());
                    }
                }
            }
        }
    }



    class ExpiredFileDelRunnable implements Runnable{

        @Override
        public void run() {
            for(Map.Entry<String, String> tmp : monitorPath.entrySet()){
                String dicFileStr = pathDicMap.get(tmp.getKey());
                if(dicFileStr == null){
                    continue;
                }

                File dicFile = new File(dicFileStr);
                if(!dicFile.exists() || !dicFile.isDirectory()){
                    continue;
                }

                int maxSaveDay = NumberUtils.toInt(tmp.getValue(), 0);
                String timeFormat = fileTimeFormatMap.get(tmp.getKey());
                String fileNamePattern = fileNameRegMap.get(tmp.getKey());

                delExpiredFile(dicFile, timeFormat, fileNamePattern, maxSaveDay);
            }
        }

    }

    public void delExpiredFile(File dic, String timeFormat, String fileNamePattern, int maxSaveDay){
        if(!dic.isDirectory()){
            logger.error("invalid file dictory:{}.", dic.getPath());
            return;
        }

        DateTimeFormatter formatter = DateTimeFormat.forPattern(timeFormat).
                withZone(DateTimeZone.forID(timeZone));

        DateTime expiredTime = new DateTime();
        expiredTime = expiredTime.plusDays(0-maxSaveDay);
        expiredTime = formatter.parseDateTime(expiredTime.toString(formatter));

        Pattern pattern = Pattern.compile(fileNamePattern);
        for(String fileName : dic.list()){
            Matcher matcher = pattern.matcher(fileName);
            if(matcher.find()){
                String timeStr = matcher.group(1);
                DateTime fileDateTime = formatter.parseDateTime(timeStr);

                if(fileDateTime.isBefore(expiredTime.getMillis())){
                    File deleFile = new File(dic, fileName);
                    if(deleFile.exists()){
                        logger.info("delete expired file:{}.", fileName);
                        deleFile.delete();
                    }
                }
            }
        }
    }
}
