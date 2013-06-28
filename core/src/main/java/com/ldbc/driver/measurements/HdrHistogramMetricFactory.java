package com.ldbc.driver.measurements;

public class HdrHistogramMetricFactory implements MetricFactory
{
    private final long highestExpectedValue;

    public HdrHistogramMetricFactory( long highestExpectedValue )
    {
        this.highestExpectedValue = highestExpectedValue;
    }

    @Override
    public Metric create( String name )
    {
        return new HdrHistogramMetric( name, highestExpectedValue );
    }
}
