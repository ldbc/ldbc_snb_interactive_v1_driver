package com.ldbc.driver.measurements;

public interface Metric
{
    public void addMeasurement( long value );

    public String getName();

    public double getMean();

    public long getPercentile( double percentile );

    public long getMin();

    public long getMax();

    public long getCount();

    public String toPrettyString();
}
