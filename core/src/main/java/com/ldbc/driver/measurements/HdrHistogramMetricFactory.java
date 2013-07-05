package com.ldbc.driver.measurements;

public class HdrHistogramMetricFactory implements MetricFactory
{
    private final long highestExpectedValue;
    private final int significantFigures;
    private final String unit;

    public HdrHistogramMetricFactory( String unit, long highestExpectedValue, int significantFigures )
    {
        this.highestExpectedValue = highestExpectedValue;
        this.significantFigures = significantFigures;
        this.unit = unit;
    }

    @Override
    public Metric create( String name )
    {
        return new HdrHistogramMetric( name, unit, highestExpectedValue, significantFigures );
    }
}
