package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.metrics.WorkloadResultsSnapshot;
import com.ldbc.driver.runtime.metrics.WorkloadStatusSnapshot;

import java.util.ArrayList;
import java.util.List;

public class DummyCollectingConcurrentMetricsService implements ConcurrentMetricsService {
    private List<OperationResultReport> operationResultReports = new ArrayList<>();

    @Override
    public void submitOperationResult(OperationResultReport operationResultReport) throws MetricsCollectionException {
        operationResultReports.add(operationResultReport);
    }

    public List<OperationResultReport> operationResultReports() {
        return operationResultReports;
    }

    @Override
    public WorkloadStatusSnapshot status() throws MetricsCollectionException {
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
