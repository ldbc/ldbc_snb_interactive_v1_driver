package com.ldbc.driver.runtime.metrics;

public interface WorkloadMetricsFormatter
{
    String format( WorkloadResultsSnapshot workloadResultsSnapshot );
}
