package com.ldbc.driver.measurements.metric;


public class HdrHistogramMetricFactory implements MetricFactory<ContinuousMetric>
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
    public ContinuousMetric create( String name )
    {
        return new ContinuousMetric( name, unit, highestExpectedValue, significantFigures );
    }
}
