package com.ldbc.driver.measurements.formatters;

import com.ldbc.driver.measurements.metric.OperationMetrics;

public interface OperationMetricsFormatter
{
    public String format( Iterable<OperationMetrics> metrics );
}
