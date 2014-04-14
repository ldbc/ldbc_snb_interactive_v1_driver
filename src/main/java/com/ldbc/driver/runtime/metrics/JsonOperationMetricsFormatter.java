package com.ldbc.driver.runtime.metrics;

public class JsonOperationMetricsFormatter implements OperationMetricsFormatter {
    @Override
    public String format(WorkloadResultsSnapshot workloadResultsSnapshot) {
        return workloadResultsSnapshot.toJson();
    }
}
