package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResult;

public interface ConcurrentMetricsService {
    void submitOperationResult(OperationResult operationResult) throws MetricsCollectionException;

    WorkloadStatus status() throws MetricsCollectionException;

    WorkloadResultsSnapshot results() throws MetricsCollectionException;

    void shutdown() throws MetricsCollectionException;
}
