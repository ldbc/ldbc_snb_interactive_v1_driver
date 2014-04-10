package com.ldbc.driver.runtime;

import com.ldbc.driver.OperationResult;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.metrics.WorkloadResults;
import com.ldbc.driver.runtime.metrics.WorkloadStatus;

public class DummyConcurrentMetricsService implements ConcurrentMetricsService {

    @Override
    public void submitOperationResult(OperationResult operationResult) throws MetricsCollectionException {

    }

    @Override
    public WorkloadStatus status() throws MetricsCollectionException {
        return null;
    }

    @Override
    public WorkloadResults results() throws MetricsCollectionException {
        return null;
    }

    @Override
    public void shutdown() throws MetricsCollectionException {

    }
}
