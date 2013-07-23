package com.ldbc.driver.measurements;

import java.util.HashMap;
import java.util.Map;

import com.ldbc.driver.measurements.metric.Metric;
import com.ldbc.driver.measurements.metric.MetricFactory;

public class MetricGroup<M extends Metric>
{
    private final String name;
    private final MetricFactory<M> metricFactory;
    private final Map<String, M> metrics;

    public MetricGroup( String name, MetricFactory<M> metricFactory )
    {
        this.name = name;
        this.metricFactory = metricFactory;
        this.metrics = new HashMap<String, M>();
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

    public Iterable<M> getMetrics()
    {
        return metrics.values();
    }
}
