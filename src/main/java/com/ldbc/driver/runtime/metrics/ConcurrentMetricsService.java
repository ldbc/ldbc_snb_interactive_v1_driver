package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResultReport;

public interface ConcurrentMetricsService {
    void submitOperationResult(OperationResultReport operationResultReport) throws MetricsCollectionException;

    WorkloadStatus status() throws MetricsCollectionException;

    WorkloadResultsSnapshot results() throws MetricsCollectionException;

    void shutdown() throws MetricsCollectionException;
}
