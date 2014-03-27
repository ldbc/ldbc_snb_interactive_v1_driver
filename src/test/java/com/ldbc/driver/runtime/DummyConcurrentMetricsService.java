package com.ldbc.driver.runtime;

import com.ldbc.driver.OperationResult;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.metrics.formatters.OperationMetricsFormatter;

import java.io.OutputStream;

class DummyConcurrentMetricsService implements ConcurrentMetricsService {

    @Override
    public void submitOperationResult(OperationResult operationResult) throws MetricsCollectionException {

    }

    @Override
    public void export(OperationMetricsFormatter metricsFormatter, OutputStream outputStream) throws MetricsCollectionException {

    }

    @Override
    public String status() throws MetricsCollectionException {
        return null;
    }

    @Override
    public void shutdown() throws MetricsCollectionException {

    }
}
