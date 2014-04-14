package com.ldbc.driver.runtime.metrics;

public interface OperationMetricsFormatter {
    String format(WorkloadResultsSnapshot workloadResultsSnapshot);
}
