package com.ldbc.driver.measurements.formatters;

import com.ldbc.driver.measurements.MetricGroup;
import com.ldbc.driver.measurements.metric.Metric;

public interface MetricFormatter<M extends Metric>
{
    public String format( M... metrics );

    public String format( MetricGroup<M>... metricGroups );
}
