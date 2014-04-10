package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResult;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class ThreadedQueuedConcurrentMetricsServiceTests {
    @Test
    public void shouldReturnCorrectMeasurements() throws WorkloadException, MetricsCollectionException {
        ConcurrentMetricsService metricsService = new ThreadedQueuedConcurrentMetricsService(new ConcurrentErrorReporter(), TimeUnit.MILLISECONDS);

        OperationResult operationResult1 = new OperationResult(1, "result one");
        operationResult1.setOperationType("type one");
        operationResult1.setScheduledStartTime(Time.fromMilli(1));
        operationResult1.setActualStartTime(Time.fromMilli(2));
        operationResult1.setRunDuration(Duration.fromMilli(1));

        OperationResult operationResult2 = new OperationResult(2, "result two");
        operationResult2.setOperationType("type one");
        operationResult2.setScheduledStartTime(Time.fromMilli(1));
        operationResult2.setActualStartTime(Time.fromMilli(8));
        operationResult2.setRunDuration(Duration.fromMilli(3));

        OperationResult operationResult3 = new OperationResult(2, "result three");
        operationResult3.setOperationType("type two");
        operationResult3.setScheduledStartTime(Time.fromMilli(1));
        operationResult3.setActualStartTime(Time.fromMilli(11));
        operationResult3.setRunDuration(Duration.fromMilli(5));

        metricsService.submitOperationResult(operationResult1);
        metricsService.submitOperationResult(operationResult2);
        metricsService.submitOperationResult(operationResult3);

        assertThat(metricsService.results().startTime(), equalTo(Time.fromMilli(2)));
        assertThat(metricsService.results().finishTime(), equalTo(Time.fromMilli(16)));

        metricsService.shutdown();
    }
}
