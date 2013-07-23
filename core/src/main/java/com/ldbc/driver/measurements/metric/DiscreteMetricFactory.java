package com.ldbc.driver.measurements.metric;

public class DiscreteMetricFactory implements MetricFactory<DiscreteMetric>
{
    private final String unit;

    public DiscreteMetricFactory( String unit )
    {
        this.unit = unit;
    }

    @Override
    public DiscreteMetric create( String name )
    {
        return new DiscreteMetric( name, unit );
    }
}
