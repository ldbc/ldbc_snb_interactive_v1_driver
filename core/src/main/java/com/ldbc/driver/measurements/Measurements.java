package com.ldbc.driver.measurements;

public interface Measurements
{
    public long getMedian();

    public long getMean();

    public long getPercentile90();

    public long getPercentile95();

    public long getPercentile99();

    public long getMin();

    public long getMax();

    public long getCount();
}
