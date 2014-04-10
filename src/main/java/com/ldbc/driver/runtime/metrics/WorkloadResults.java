package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

import java.util.List;

public interface WorkloadResults {
    OperationMetrics metricsFor(String operationType);

    List<OperationMetrics> metricsForAllOperations();

    Time startTime();

    Time finishTime();

    Duration totalRunDuration();

    long totalOperationCount();
}
