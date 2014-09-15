package com.ldbc.driver.runtime;

import com.google.common.collect.Lists;
import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.runtime.metrics.*;
import com.ldbc.driver.temporal.Time;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DummyCountingConcurrentMetricsService implements ConcurrentMetricsService {
    private long count = 0;
    private final Map<String, OperationMetricsSnapshot> metrics;

    public DummyCountingConcurrentMetricsService(){
        metrics = new HashMap<>();
        metrics.put("default",new OperationMetricsSnapshot(null,null,0,null,null,null));
    }

    @Override
    synchronized public void submitOperationResult(OperationResultReport operationResultReport) throws MetricsCollectionException {
        count++;
    }

    @Override
    public WorkloadStatusSnapshot status() throws MetricsCollectionException {
        return new WorkloadStatusSnapshot(null, count, null, 0);
    }

    @Override
    public WorkloadResultsSnapshot results() throws MetricsCollectionException {
        return new WorkloadResultsSnapshot(metrics, Time.fromMilli(0), Time.fromMilli(0), count, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() throws MetricsCollectionException {

    }
}
