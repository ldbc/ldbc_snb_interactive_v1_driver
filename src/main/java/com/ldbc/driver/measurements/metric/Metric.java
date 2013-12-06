package com.ldbc.driver.measurements.metric;

public interface Metric
{
    public void addMeasurement( long value );

    public String name();

    public String unit();

    public long count();
}
