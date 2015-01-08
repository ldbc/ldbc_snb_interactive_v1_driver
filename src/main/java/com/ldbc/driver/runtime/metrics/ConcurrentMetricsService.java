package com.ldbc.driver.runtime.metrics;

public interface ConcurrentMetricsService {
    void shutdown() throws MetricsCollectionException;

    ConcurrentMetricsServiceWriter getWriter() throws MetricsCollectionException;

    public interface ConcurrentMetricsServiceWriter {
        void submitOperationResult(
                int operationType,
                long scheduledStartTimeAsMilli,
                long actualStartTimeAsMilli,
                long runDurationAsNano,
                int resultCode
        ) throws MetricsCollectionException;

        WorkloadStatusSnapshot status() throws MetricsCollectionException;

        WorkloadResultsSnapshot results() throws MetricsCollectionException;
    }
}
