package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResult;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class MetricsManagerTest {
    private TimeSource TIME_SOURCE = new SystemTimeSource();

    @Test
    public void shouldReturnCorrectMeasurements() throws WorkloadException, MetricsCollectionException {
        MetricsManager metricsManager = new MetricsManager(TIME_SOURCE, TimeUnit.NANOSECONDS);

        OperationResult operationResult1 = new OperationResult(1, "result one");
        operationResult1.setOperationType("type one");
        operationResult1.setScheduledStartTime(Time.fromNano(1));
        operationResult1.setActualStartTime(Time.fromNano(2));
        operationResult1.setRunDuration(Duration.fromNano(1));

        OperationResult operationResult2 = new OperationResult(2, "result two");
        operationResult2.setOperationType("type one");
        operationResult2.setScheduledStartTime(Time.fromNano(1));
        operationResult2.setActualStartTime(Time.fromNano(8));
        operationResult2.setRunDuration(Duration.fromNano(3));

        OperationResult operationResult3 = new OperationResult(2, "result three");
        operationResult3.setOperationType("type two");
        operationResult3.setScheduledStartTime(Time.fromNano(1));
        operationResult3.setActualStartTime(Time.fromNano(11));
        operationResult3.setRunDuration(Duration.fromNano(5));

        metricsManager.measure(operationResult1);
        metricsManager.measure(operationResult2);
        metricsManager.measure(operationResult3);

        assertThat(metricsManager.startTime(), equalTo(Time.fromNano(2)));
        assertThat(metricsManager.finishTime(), equalTo(Time.fromNano(16)));
    }
}
