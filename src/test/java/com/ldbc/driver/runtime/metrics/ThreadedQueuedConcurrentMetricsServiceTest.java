package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.OperationResultReportTestHelper;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class ThreadedQueuedConcurrentMetricsServiceTest {
    TimeSource TIME_SOURCE = new SystemTimeSource();
    Time INITIAL_START_TIME = Time.fromMilli(0);

    @Test
    public void shouldNotAcceptOperationResultsAfterShutdownWhenBlockingQueueIsUsed() throws WorkloadException, MetricsCollectionException {
        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(
                TIME_SOURCE,
                new ConcurrentErrorReporter(),
                TimeUnit.MILLISECONDS,
                INITIAL_START_TIME);

        metricsService.shutdown();
        boolean exceptionThrown = false;
        try {
            shouldReturnCorrectMeasurements(metricsService);
        } catch (MetricsCollectionException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
    }

    @Test
    public void shouldNotAcceptOperationResultsAfterShutdownWhenNonBlockingQueueIsUsed() throws WorkloadException, MetricsCollectionException {
        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingNonBlockingQueue(
                TIME_SOURCE,
                new ConcurrentErrorReporter(),
                TimeUnit.MILLISECONDS,
                INITIAL_START_TIME);

        metricsService.shutdown();
        boolean exceptionThrown = false;
        try {
            shouldReturnCorrectMeasurements(metricsService);
        } catch (MetricsCollectionException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
    }

    @Test
    public void shouldReturnCorrectMeasurementsWhenBlockingQueueIsUsed() throws WorkloadException, MetricsCollectionException {
        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(
                TIME_SOURCE,
                new ConcurrentErrorReporter(),
                TimeUnit.MILLISECONDS,
                INITIAL_START_TIME);

        try {
            shouldReturnCorrectMeasurements(metricsService);
        } finally {
            metricsService.shutdown();
        }
    }

    @Test
    public void shouldReturnCorrectMeasurementsWhenNonBlockingQueueIsUsed() throws WorkloadException, MetricsCollectionException {
        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(
                TIME_SOURCE,
                new ConcurrentErrorReporter(),
                TimeUnit.MILLISECONDS,
                INITIAL_START_TIME);

        try {
            shouldReturnCorrectMeasurements(metricsService);
        } finally {
            metricsService.shutdown();
        }
    }

    public void shouldReturnCorrectMeasurements(ConcurrentMetricsService metricsService) throws WorkloadException, MetricsCollectionException {
        assertThat(metricsService.results().startTime(), equalTo(INITIAL_START_TIME));
        assertThat(metricsService.results().latestFinishTime(), is(INITIAL_START_TIME));

        OperationResultReport operationResultReport1 = OperationResultReportTestHelper.create(1, "result one");
        OperationResultReportTestHelper.setOperationType(operationResultReport1, "type one");
        OperationResultReportTestHelper.setScheduledStartTime(operationResultReport1, Time.fromMilli(1));
        OperationResultReportTestHelper.setActualStartTime(operationResultReport1, Time.fromMilli(2));
        OperationResultReportTestHelper.setRunDuration(operationResultReport1, Duration.fromMilli(1));

        metricsService.submitOperationResult(operationResultReport1);

        assertThat(metricsService.results().startTime(), equalTo(INITIAL_START_TIME));
        assertThat(metricsService.results().latestFinishTime(), equalTo(Time.fromMilli(3)));

        OperationResultReport operationResultReport2 = OperationResultReportTestHelper.create(2, "result two");
        OperationResultReportTestHelper.setOperationType(operationResultReport2, "type one");
        OperationResultReportTestHelper.setScheduledStartTime(operationResultReport2, Time.fromMilli(1));
        OperationResultReportTestHelper.setActualStartTime(operationResultReport2, Time.fromMilli(8));
        OperationResultReportTestHelper.setRunDuration(operationResultReport2, Duration.fromMilli(3));

        metricsService.submitOperationResult(operationResultReport2);

        assertThat(metricsService.results().startTime(), equalTo(INITIAL_START_TIME));
        assertThat(metricsService.results().latestFinishTime(), equalTo(Time.fromMilli(11)));

        OperationResultReport operationResultReport3 = OperationResultReportTestHelper.create(2, "result three");
        OperationResultReportTestHelper.setOperationType(operationResultReport3, "type two");
        OperationResultReportTestHelper.setScheduledStartTime(operationResultReport3, Time.fromMilli(1));
        OperationResultReportTestHelper.setActualStartTime(operationResultReport3, Time.fromMilli(11));
        OperationResultReportTestHelper.setRunDuration(operationResultReport3, Duration.fromMilli(5));

        metricsService.submitOperationResult(operationResultReport3);

        assertThat(metricsService.results().startTime(), equalTo(INITIAL_START_TIME));
        assertThat(metricsService.results().latestFinishTime(), equalTo(Time.fromMilli(16)));
    }
}
