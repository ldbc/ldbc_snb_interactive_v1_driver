package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.runtime.metrics.OperationMetrics;

public interface OperationMetricsFormatter
{
    public String format( Iterable<OperationMetrics> metrics );
}
