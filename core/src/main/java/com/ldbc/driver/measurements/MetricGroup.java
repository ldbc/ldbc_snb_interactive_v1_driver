package com.ldbc.driver.measurements;

import java.util.HashMap;
import java.util.Map;

public class MetricGroup
{
    private final String name;
    private final MetricFactory metricFactory;
    private final Map<String, Metric> metrics;

    public MetricGroup( String name, MetricFactory metricFactory )
    {
        this.name = name;
        this.metricFactory = metricFactory;
        this.metrics = new HashMap<String, Metric>();
    }

    public String getName()
    {
        return name;
    }

    public Metric getMetric( String name )
    {
        return metrics.get( name );
    }

    public Metric getOrCreateMetric( String name )
    {
        if ( false == metrics.containsKey( name ) )
        {
            metrics.put( name, metricFactory.create( name ) );
        }
        return metrics.get( name );
    }

    public Iterable<Metric> getMetrics()
    {
        return metrics.values();
    }
}
