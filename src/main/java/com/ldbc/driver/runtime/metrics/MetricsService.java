package com.ldbc.driver.runtime.metrics;

public interface MetricsService
{
    void shutdown() throws MetricsCollectionException;

    MetricsServiceWriter getWriter() throws MetricsCollectionException;

    interface MetricsServiceWriter
    {
        void submitOperationResult(
                int operationType,
                long scheduledStartTimeAsMilli,
                long actualStartTimeAsMilli,
                long runDurationAsNano,
                int resultCode,
                long originalStartTime) throws MetricsCollectionException;

        WorkloadStatusSnapshot status() throws MetricsCollectionException;

        WorkloadResultsSnapshot results() throws MetricsCollectionException;
    }
}
