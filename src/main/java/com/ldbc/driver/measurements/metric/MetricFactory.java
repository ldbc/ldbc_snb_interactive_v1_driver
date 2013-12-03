package com.ldbc.driver.measurements.metric;


public interface MetricFactory<M extends Metric>
{
    public M create( String name );
}
