package com.dtstack.jlogstash.inputs;

/**
 * 任务调度器
 *
 * @author zxb
 * @version 1.0.0
 *          2017年03月22日 14:51
 * @since Jdk1.6
 */
public abstract class Scheduler {

    protected Task task;

    /**
     * 初始化构造
     *
     * @param task
     */
    public Scheduler(Task task) {
        this.task = task;
    }

    /**
     * 开始调度
     */
    public abstract void start();

    /**
     * 结束调度
     */
    public abstract void stop();

    /**
     * 获取本次执行与下次执行的时间间隔
     *
     * @return
     */
    public abstract Long getNextDuration();
}
