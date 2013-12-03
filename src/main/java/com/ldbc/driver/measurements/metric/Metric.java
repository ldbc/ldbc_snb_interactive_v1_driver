package com.ldbc.driver.measurements.metric;

public interface Metric
{
    public void addMeasurement( long value );

    public String getName();

    public String getUnit();

    public long getCount();
}
