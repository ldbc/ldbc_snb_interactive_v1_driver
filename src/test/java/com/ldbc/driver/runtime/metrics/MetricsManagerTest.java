package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.OperationResultReportTestHelper;
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
        Time initialTime = Time.fromMilli(0);
        MetricsManager metricsManager = new MetricsManager(
                TIME_SOURCE,
                TimeUnit.MILLISECONDS,
                initialTime,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION
        );

        OperationResultReport operationResultReport1 = OperationResultReportTestHelper.create(1, "result one");
        OperationResultReportTestHelper.setOperationType(operationResultReport1, "type one");
        OperationResultReportTestHelper.setScheduledStartTime(operationResultReport1, Time.fromMilli(1));
        OperationResultReportTestHelper.setActualStartTime(operationResultReport1, Time.fromMilli(2));
        OperationResultReportTestHelper.setRunDuration(operationResultReport1, Duration.fromMilli(1));

        OperationResultReport operationResultReport2 = OperationResultReportTestHelper.create(2, "result two");
        OperationResultReportTestHelper.setOperationType(operationResultReport2, "type one");
        OperationResultReportTestHelper.setScheduledStartTime(operationResultReport2, Time.fromMilli(1));
        OperationResultReportTestHelper.setActualStartTime(operationResultReport2, Time.fromMilli(8));
        OperationResultReportTestHelper.setRunDuration(operationResultReport2, Duration.fromMilli(3));

        OperationResultReport operationResultReport3 = OperationResultReportTestHelper.create(2, "result three");
        OperationResultReportTestHelper.setOperationType(operationResultReport3, "type two");
        OperationResultReportTestHelper.setScheduledStartTime(operationResultReport3, Time.fromMilli(1));
        OperationResultReportTestHelper.setActualStartTime(operationResultReport3, Time.fromMilli(11));
        OperationResultReportTestHelper.setRunDuration(operationResultReport3, Duration.fromMilli(5));

        metricsManager.measure(operationResultReport1);
        metricsManager.measure(operationResultReport2);
        metricsManager.measure(operationResultReport3);

        assertThat(metricsManager.startTime(), equalTo(Time.fromMilli(0)));
        assertThat(metricsManager.latestFinishTime(), equalTo(Time.fromMilli(16)));
    }
}
