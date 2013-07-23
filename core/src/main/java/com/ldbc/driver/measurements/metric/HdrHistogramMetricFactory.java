package com.ldbc.driver.measurements.metric;


public class HdrHistogramMetricFactory implements MetricFactory<HdrHistogramMetric>
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
    public HdrHistogramMetric create( String name )
    {
        return new HdrHistogramMetric( name, unit, highestExpectedValue, significantFigures );
    }
}
