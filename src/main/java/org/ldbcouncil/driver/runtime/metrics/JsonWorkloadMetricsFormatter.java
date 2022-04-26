package org.ldbcouncil.driver.runtime.metrics;

public class JsonWorkloadMetricsFormatter implements WorkloadMetricsFormatter {
    @Override
    public String format(WorkloadResultsSnapshot workloadResultsSnapshot) {
        return workloadResultsSnapshot.toJson();
    }
}
