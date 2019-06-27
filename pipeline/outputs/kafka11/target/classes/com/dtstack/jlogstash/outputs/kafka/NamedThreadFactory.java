package com.dtstack.jlogstash.outputs.kafka;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: haisi
 * @date 2019-06-25 10:44
 */
public class NamedThreadFactory  implements ThreadFactory {

    public static final AtomicInteger poolNumber = new AtomicInteger(1);

    final AtomicInteger threadNumber = new AtomicInteger(1);
    final ThreadGroup group;
    final String namePrefix;
    final boolean isDaemon;


    public NamedThreadFactory(){
        this("pool");
    }

    public NamedThreadFactory(String name) {
        this(name,false);
    }

    public NamedThreadFactory(String preffix, boolean daemon){
        SecurityManager s = System.getSecurityManager();
        group = (s!=null) ? s.getThreadGroup() : Thread.currentThread()
                .getThreadGroup();
        namePrefix = preffix + "-"+ poolNumber.getAndIncrement()+"-thread-";
        isDaemon = daemon;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(group,r,namePrefix+threadNumber.getAndIncrement(),0);
        t.setDaemon(this.isDaemon);
        if (t.getPriority()!=Thread.NORM_PRIORITY){
            t.setPriority(Thread.NORM_PRIORITY);
        }
        return t;
    }

    public static void main(String[] args) {

    }
}
