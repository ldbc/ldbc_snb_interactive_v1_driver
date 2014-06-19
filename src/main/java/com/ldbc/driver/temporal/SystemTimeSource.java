package com.ldbc.driver.temporal;

import java.util.concurrent.TimeUnit;

public class SystemTimeSource implements TimeSource {
    @Override
    public Time now() {
        return Time.fromNano(Temporal.convert(nowAsMilli(), TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS));
    }

    @Override
    public long nowAsMilli() {
        return System.currentTimeMillis();
    }
}