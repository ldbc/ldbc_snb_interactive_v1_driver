package com.ldbc.driver.temporal;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ManualTimeSource implements TimeSource {
    private final TemporalUtil temporalUtil = new TemporalUtil();
    private AtomicLong nowAsMilli = new AtomicLong(0);

    public ManualTimeSource(long nowAsMilli) {
        this.nowAsMilli = new AtomicLong(nowAsMilli);
    }

    public void setNowFromMilli(long ms) {
        nowAsMilli.set(ms);
    }

    @Override
    public long nanoSnapshot() {
        return TimeUnit.MILLISECONDS.toNanos(nowAsMilli());
    }

    @Override
    public long nowAsMilli() {
        return nowAsMilli.get();
    }
}