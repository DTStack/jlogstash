package com.dtstack.jlogstash.outputs.util.flow;

import com.google.common.util.concurrent.RateLimiter;

/**
 * Created by daguan on 17/6/15.
 */
public class FlowControlShiper {

    private RateLimiter rateLimiter;

    public FlowControlShiper(Threshold threshold) {
        rateLimiter = RateLimiter.create(threshold.getValue());
    }

    public double acquire() {
        return rateLimiter.acquire();
    }

    public double acquire(int permits) {
        return rateLimiter.acquire(permits);
    }

}
