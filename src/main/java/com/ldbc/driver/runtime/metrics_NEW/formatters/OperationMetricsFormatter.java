package com.ldbc.driver.runtime.metrics_NEW.formatters;

import com.ldbc.driver.runtime.metrics_NEW.OperationMetrics;

public interface OperationMetricsFormatter
{
    public String format( Iterable<OperationMetrics> metrics );
}
