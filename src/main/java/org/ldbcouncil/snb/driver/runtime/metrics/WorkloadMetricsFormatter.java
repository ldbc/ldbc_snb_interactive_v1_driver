package org.ldbcouncil.snb.driver.runtime.metrics;

public interface WorkloadMetricsFormatter
{
    String format( WorkloadResultsSnapshot workloadResultsSnapshot );
}
