package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.OperationResultReportTestHelper;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.scheduling.ErrorReportingTerminatingExecutionDelayPolicy;
import com.ldbc.driver.runtime.scheduling.ExecutionDelayPolicy;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.CsvFileWriter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationInstances;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ThreadedQueuedConcurrentMetricsServiceTest {
    private TimeSource timeSource = new SystemTimeSource();
    private Time INITIAL_START_TIME = Time.fromMilli(0);

    @Test
    public void shouldNotAcceptOperationResultsAfterShutdownWhenBlockingQueueIsUsed() throws WorkloadException, MetricsCollectionException {
        doShouldNotAcceptOperationResultsAfterShutdownWhenBlockingQueueIsUsed(true);
        doShouldNotAcceptOperationResultsAfterShutdownWhenBlockingQueueIsUsed(false);
    }

    public void doShouldNotAcceptOperationResultsAfterShutdownWhenBlockingQueueIsUsed(boolean recordStartTimeDelayLatency) throws WorkloadException, MetricsCollectionException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        Duration toleratedExecutionDelayDuration = ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION;
        ExecutionDelayPolicy executionDelayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(
                timeSource,
                toleratedExecutionDelayDuration,
                errorReporter);
        CsvFileWriter csvResultsLogWriter = null;
        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(
                timeSource,
                new ConcurrentErrorReporter(),
                TimeUnit.MILLISECONDS,
                INITIAL_START_TIME,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION,
                recordStartTimeDelayLatency,
                executionDelayPolicy,
                csvResultsLogWriter);

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
        doShouldNotAcceptOperationResultsAfterShutdownWhenNonBlockingQueueIsUsed(true);
        doShouldNotAcceptOperationResultsAfterShutdownWhenNonBlockingQueueIsUsed(false);
    }

    public void doShouldNotAcceptOperationResultsAfterShutdownWhenNonBlockingQueueIsUsed(boolean recordStartTimeDelayLatency) throws WorkloadException, MetricsCollectionException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        Duration toleratedExecutionDelayDuration = ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION;
        ExecutionDelayPolicy executionDelayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(
                timeSource,
                toleratedExecutionDelayDuration,
                errorReporter);
        CsvFileWriter csvResultsLogWriter = null;
        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingNonBlockingQueue(
                timeSource,
                new ConcurrentErrorReporter(),
                TimeUnit.MILLISECONDS,
                INITIAL_START_TIME,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION,
                recordStartTimeDelayLatency,
                executionDelayPolicy,
                csvResultsLogWriter);

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
        doShouldReturnCorrectMeasurementsWhenBlockingQueueIsUsed(true);
        doShouldReturnCorrectMeasurementsWhenBlockingQueueIsUsed(false);
    }

    public void doShouldReturnCorrectMeasurementsWhenBlockingQueueIsUsed(boolean recordStartTimeDelayLatency) throws WorkloadException, MetricsCollectionException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        Duration toleratedExecutionDelayDuration = ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION;
        ExecutionDelayPolicy executionDelayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(
                timeSource,
                toleratedExecutionDelayDuration,
                errorReporter);
        CsvFileWriter csvResultsLogWriter = null;
        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(
                timeSource,
                new ConcurrentErrorReporter(),
                TimeUnit.MILLISECONDS,
                INITIAL_START_TIME,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION,
                recordStartTimeDelayLatency,
                executionDelayPolicy,
                csvResultsLogWriter);
        try {
            shouldReturnCorrectMeasurements(metricsService);
        } finally {
            System.out.println(errorReporter.toString());
            metricsService.shutdown();
        }
    }

    @Test
    public void shouldReturnCorrectMeasurementsWhenNonBlockingQueueIsUsed() throws WorkloadException, MetricsCollectionException {
        doShouldReturnCorrectMeasurementsWhenNonBlockingQueueIsUsed(true);
        doShouldReturnCorrectMeasurementsWhenNonBlockingQueueIsUsed(false);
    }

    public void doShouldReturnCorrectMeasurementsWhenNonBlockingQueueIsUsed(boolean recordStartTimeDelayLatency) throws WorkloadException, MetricsCollectionException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        Duration toleratedExecutionDelayDuration = ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION;
        ExecutionDelayPolicy executionDelayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(
                timeSource,
                toleratedExecutionDelayDuration,
                errorReporter);
        CsvFileWriter csvResultsLogWriter = null;
        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(
                timeSource,
                new ConcurrentErrorReporter(),
                TimeUnit.MILLISECONDS,
                INITIAL_START_TIME,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION,
                recordStartTimeDelayLatency,
                executionDelayPolicy,
                csvResultsLogWriter);
        try {
            shouldReturnCorrectMeasurements(metricsService);
        } finally {
            System.out.println(errorReporter.toString());
            metricsService.shutdown();
        }
    }

    public void shouldReturnCorrectMeasurements(ConcurrentMetricsService metricsService) throws WorkloadException, MetricsCollectionException {
        assertThat(metricsService.results().startTime(), equalTo(INITIAL_START_TIME));
        assertThat(metricsService.results().latestFinishTime(), is(INITIAL_START_TIME));

        Operation<?> operation1 = DummyLdbcSnbInteractiveOperationInstances.read1();
        operation1.setScheduledStartTime(Time.fromMilli(1));
        OperationResultReport operationResultReport1 = OperationResultReportTestHelper.create(1, "result one", operation1);
        OperationResultReportTestHelper.setActualStartTime(operationResultReport1, Time.fromMilli(2));
        OperationResultReportTestHelper.setRunDuration(operationResultReport1, Duration.fromMilli(1));

        metricsService.submitOperationResult(operationResultReport1);

        assertThat(metricsService.results().startTime(), equalTo(INITIAL_START_TIME));
        assertThat(metricsService.results().latestFinishTime(), equalTo(Time.fromMilli(3)));

        Operation<?> operation2 = DummyLdbcSnbInteractiveOperationInstances.read1();
        operation2.setScheduledStartTime(Time.fromMilli(1));
        OperationResultReport operationResultReport2 = OperationResultReportTestHelper.create(2, "result two", operation2);
        OperationResultReportTestHelper.setActualStartTime(operationResultReport2, Time.fromMilli(8));
        OperationResultReportTestHelper.setRunDuration(operationResultReport2, Duration.fromMilli(3));

        metricsService.submitOperationResult(operationResultReport2);

        assertThat(metricsService.results().startTime(), equalTo(INITIAL_START_TIME));
        assertThat(metricsService.results().latestFinishTime(), equalTo(Time.fromMilli(11)));

        Operation<?> operation3 = DummyLdbcSnbInteractiveOperationInstances.read2();
        operation3.setScheduledStartTime(Time.fromMilli(1));
        OperationResultReport operationResultReport3 = OperationResultReportTestHelper.create(2, "result three", operation3);
        OperationResultReportTestHelper.setActualStartTime(operationResultReport3, Time.fromMilli(11));
        OperationResultReportTestHelper.setRunDuration(operationResultReport3, Duration.fromMilli(5));

        metricsService.submitOperationResult(operationResultReport3);

        WorkloadResultsSnapshot results = metricsService.results();
        assertThat(results.startTime(), equalTo(INITIAL_START_TIME));
        assertThat(results.latestFinishTime(), equalTo(Time.fromMilli(16)));
    }
}
