package com.ldbc.driver.temporal;

import java.util.concurrent.TimeUnit;

public class ManualTimeSource implements TimeSource {
    private long nowAsMilli = 0;

    public ManualTimeSource(long nowAsMilli) {
        this.nowAsMilli = nowAsMilli;
    }

    public void setNowFromMilli(long ms) {
        nowAsMilli = ms;
    }

    @Override
    public Time now() {
        return Time.fromNano(Temporal.convert(nowAsMilli(), TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS));
    }

    @Override
    public long nowAsMilli() {
        return nowAsMilli;
    }
}