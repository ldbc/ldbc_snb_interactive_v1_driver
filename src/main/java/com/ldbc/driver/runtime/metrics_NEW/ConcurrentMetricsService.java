package com.ldbc.driver.runtime.metrics_NEW;

import com.ldbc.driver.OperationResult;
import com.ldbc.driver.runtime.metrics_NEW.formatters.OperationMetricsFormatter;

import java.io.OutputStream;

public interface ConcurrentMetricsService {
    void submitOperationResult(OperationResult operationResult) throws MetricsCollectionException;

    void export(OperationMetricsFormatter metricsFormatter, OutputStream outputStream) throws MetricsCollectionException;

    String status() throws MetricsCollectionException;

    void shutdown() throws MetricsCollectionException;
}
