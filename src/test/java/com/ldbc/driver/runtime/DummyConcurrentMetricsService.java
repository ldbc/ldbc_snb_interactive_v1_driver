package com.ldbc.driver.runtime;

import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.metrics.WorkloadResultsSnapshot;
import com.ldbc.driver.runtime.metrics.WorkloadStatus;

public class DummyConcurrentMetricsService implements ConcurrentMetricsService {

    @Override
    public void submitOperationResult(OperationResultReport operationResultReport) throws MetricsCollectionException {

    }

    @Override
    public WorkloadStatus status() throws MetricsCollectionException {
        return null;
    }

    @Override
    public WorkloadResultsSnapshot results() throws MetricsCollectionException {
        return null;
    }

    @Override
    public void shutdown() throws MetricsCollectionException {

    }
}
