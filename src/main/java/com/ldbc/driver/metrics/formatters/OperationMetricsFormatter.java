package com.ldbc.driver.metrics.formatters;

import com.ldbc.driver.metrics.OperationMetrics;

public interface OperationMetricsFormatter
{
    public String format( Iterable<OperationMetrics> metrics );
}
