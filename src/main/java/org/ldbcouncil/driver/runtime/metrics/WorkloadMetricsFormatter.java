package org.ldbcouncil.driver.runtime.metrics;

public interface WorkloadMetricsFormatter
{
    String format( WorkloadResultsSnapshot workloadResultsSnapshot );
}
