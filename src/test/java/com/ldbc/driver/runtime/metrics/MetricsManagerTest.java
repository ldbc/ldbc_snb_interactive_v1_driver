package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.OperationResultReportTestHelper;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationInstances;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class MetricsManagerTest {
    private final TimeSource timeSource = new SystemTimeSource();
    private final boolean recordStartTimeDelayLatency = true;

    @Test
    public void shouldReturnCorrectMeasurements() throws WorkloadException, MetricsCollectionException {
        Time initialTime = Time.fromMilli(0);
        MetricsManager metricsManager = new MetricsManager(
                timeSource,
                TimeUnit.MILLISECONDS,
                initialTime,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION_AS_MILLI,
                recordStartTimeDelayLatency
        );

        Operation<?> operation1 = DummyLdbcSnbInteractiveOperationInstances.read1();
        operation1.setScheduledStartTimeAsMilli(Time.fromMilli(1));
        OperationResultReport operationResultReport1 = OperationResultReportTestHelper.create(1, "result one", operation1);
        OperationResultReportTestHelper.setActualStartTime(operationResultReport1, Time.fromMilli(2));
        OperationResultReportTestHelper.setRunDuration(operationResultReport1, Duration.fromMilli(1));

        Operation<?> operation2 = DummyLdbcSnbInteractiveOperationInstances.read1();
        operation2.setScheduledStartTimeAsMilli(Time.fromMilli(1));
        OperationResultReport operationResultReport2 = OperationResultReportTestHelper.create(2, "result two", operation2);
        OperationResultReportTestHelper.setActualStartTime(operationResultReport2, Time.fromMilli(8));
        OperationResultReportTestHelper.setRunDuration(operationResultReport2, Duration.fromMilli(3));

        Operation<?> operation3 = DummyLdbcSnbInteractiveOperationInstances.read2();
        operation3.setScheduledStartTimeAsMilli(Time.fromMilli(1));
        OperationResultReport operationResultReport3 = OperationResultReportTestHelper.create(2, "result three", operation3);
        OperationResultReportTestHelper.setActualStartTime(operationResultReport3, Time.fromMilli(11));
        OperationResultReportTestHelper.setRunDuration(operationResultReport3, Duration.fromMilli(5));

        metricsManager.measure(operationResultReport1);
        metricsManager.measure(operationResultReport2);
        metricsManager.measure(operationResultReport3);

        assertThat(metricsManager.startTimeAsMilli(), equalTo(Time.fromMilli(0)));
        assertThat(metricsManager.latestFinishTimeAsMilli(), equalTo(Time.fromMilli(16)));
    }
}
