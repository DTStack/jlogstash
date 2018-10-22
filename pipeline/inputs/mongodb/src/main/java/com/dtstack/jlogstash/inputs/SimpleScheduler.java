package com.dtstack.jlogstash.inputs;

/**
 * 简单调度器，按频率调度
 *
 * @author zxb
 * @version 1.0.0
 *          2017年03月22日 15:26
 * @since Jdk1.6
 */
public class SimpleScheduler extends Scheduler {

    /**
     * 间隔频率
     */
    private Long interval;

    /**
     * 是否停止
     */
    private volatile boolean stop = false;

    public SimpleScheduler(Task task) {
        super(task);
    }

    public SimpleScheduler(Task task, Long interval) {
        super(task);
        this.interval = interval;
    }

    @Override
    public void start() {
        while (!stop) {
            task.execute();

            // interval为空时直接退出
            if (interval == null) {
                break;
            }

            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void stop() {
        stop = true;
    }

    @Override
    public Long getNextDuration() {
        return interval;
    }
}
