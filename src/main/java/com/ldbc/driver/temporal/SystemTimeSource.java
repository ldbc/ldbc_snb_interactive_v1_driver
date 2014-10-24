package com.ldbc.driver.temporal;

public class SystemTimeSource implements TimeSource {

    @Override
    public long nanoSnapshot() {
        return System.nanoTime();
    }

    @Override
    public long nowAsMilli() {
        return System.currentTimeMillis();
    }
}