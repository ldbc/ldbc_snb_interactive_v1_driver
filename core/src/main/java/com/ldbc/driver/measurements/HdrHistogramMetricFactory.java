package com.ldbc.driver.measurements;

import com.ldbc.driver.util.Duration;

public class HdrHistogramMetricFactory implements MetricFactory
{
    private final Duration highestExpectedDuration;

    public HdrHistogramMetricFactory( Duration highestExpectedDuration )
    {
        this.highestExpectedDuration = highestExpectedDuration;
    }

    @Override
    public Metric create( String name )
    {
        return new HdrHistogramMetric( name, highestExpectedDuration );
    }
}
