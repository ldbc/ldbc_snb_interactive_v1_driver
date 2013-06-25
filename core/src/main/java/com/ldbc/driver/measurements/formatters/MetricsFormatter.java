package com.ldbc.driver.measurements.formatters;

import com.ldbc.driver.measurements.Metric;
import com.ldbc.driver.measurements.MetricGroup;

public interface MetricsFormatter
{
    public String format( Metric... metrics );

    public String format( MetricGroup... metricGroups );
}
