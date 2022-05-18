package org.ldbcouncil.snb.driver.temporal;

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
