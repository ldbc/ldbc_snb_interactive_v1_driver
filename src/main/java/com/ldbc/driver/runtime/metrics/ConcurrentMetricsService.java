package com.ldbc.driver.runtime.metrics;

public interface ConcurrentMetricsService {
    void submitOperationResult(
            int operationType,
            long scheduledStartTimeAsMilli,
            long actualStartTimeAsMilli,
            long runDurationAsNano,
            int resultCode
    ) throws MetricsCollectionException;

    WorkloadStatusSnapshot status() throws MetricsCollectionException;

    WorkloadResultsSnapshot results() throws MetricsCollectionException;

    void shutdown() throws MetricsCollectionException;
}
