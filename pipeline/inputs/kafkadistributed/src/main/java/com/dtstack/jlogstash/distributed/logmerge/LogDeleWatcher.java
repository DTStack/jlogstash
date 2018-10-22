package com.dtstack.jlogstash.distributed.logmerge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Reason:
 * Date: 2017/1/13
 * Company: www.dtstack.com
 *
 * @ahthor xuchao
 */

public class LogDeleWatcher  implements Callable {

    private static final Logger logger = LoggerFactory.getLogger(LogDeleWatcher.class);

    private boolean isRunning = false;

    private ExecutorService executorService;

    private LogPool logPool;

    /**删除数据检查间隔时间*/
    private static int SLEEP_TIME = 1 * 60 * 1000;

    public LogDeleWatcher(LogPool logPool){
        this.logPool = logPool;
    }

    @Override
    public Object call() throws Exception {
        while (isRunning){
            Thread.sleep(SLEEP_TIME);
            logger.warn("begin to watch time out log...");
            logPool.deleteLog();
        }
        return null;
    }

    public void startup(){
        this.isRunning = true;
        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(this);
        logger.info("log pool delete watcher is start up success, SLEEP_TIME:{}", SLEEP_TIME);
    }

    public void shutdown(){
        this.isRunning = false;
        executorService.shutdown();
        logger.info("log pool delete watcher is shutdown");
    }


}
