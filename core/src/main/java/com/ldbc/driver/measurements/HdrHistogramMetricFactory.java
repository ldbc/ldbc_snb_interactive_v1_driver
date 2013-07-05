package com.ldbc.driver.measurements;

public class HdrHistogramMetricFactory implements MetricFactory
{
    private final long highestExpectedValue;
    private final int significantFigures;

    public HdrHistogramMetricFactory( long highestExpectedValue, int significantFigures )
    {
        this.highestExpectedValue = highestExpectedValue;
        this.significantFigures = significantFigures;
    }

    @Override
    public Metric create( String name )
    {
        return new HdrHistogramMetric( name, highestExpectedValue, significantFigures );
    }
}
