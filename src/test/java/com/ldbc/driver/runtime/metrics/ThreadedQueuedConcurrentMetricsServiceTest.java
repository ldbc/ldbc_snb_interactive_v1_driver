package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.OperationResultReportTestHelper;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.scheduling.ErrorReportingTerminatingExecutionDelayPolicy;
import com.ldbc.driver.runtime.scheduling.ExecutionDelayPolicy;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.csv.SimpleCsvFileWriter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationInstances;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ThreadedQueuedConcurrentMetricsServiceTest {
    private final TemporalUtil temporalUtil = new TemporalUtil();
    private TimeSource timeSource = new SystemTimeSource();

    @Test
    public void shouldNotAcceptOperationResultsAfterShutdownWhenBlockingQueueIsUsed() throws WorkloadException, MetricsCollectionException {
        doShouldNotAcceptOperationResultsAfterShutdownWhenBlockingQueueIsUsed(true);
        doShouldNotAcceptOperationResultsAfterShutdownWhenBlockingQueueIsUsed(false);
    }

    public void doShouldNotAcceptOperationResultsAfterShutdownWhenBlockingQueueIsUsed(boolean recordStartTimeDelayLatency) throws WorkloadException, MetricsCollectionException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        long toleratedExecutionDelayDuration = ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION_AS_MILLI;
        ExecutionDelayPolicy executionDelayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(
                timeSource,
                toleratedExecutionDelayDuration,
                errorReporter);
        SimpleCsvFileWriter csvResultsLogWriter = null;
        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(
                timeSource,
                new ConcurrentErrorReporter(),
                TimeUnit.MILLISECONDS,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
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
        long toleratedExecutionDelayDuration = ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION_AS_MILLI;
        ExecutionDelayPolicy executionDelayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(
                timeSource,
                toleratedExecutionDelayDuration,
                errorReporter);
        SimpleCsvFileWriter csvResultsLogWriter = null;
        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingNonBlockingQueue(
                timeSource,
                new ConcurrentErrorReporter(),
                TimeUnit.MILLISECONDS,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
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
        long toleratedExecutionDelayDuration = ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION_AS_MILLI;
        ExecutionDelayPolicy executionDelayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(
                timeSource,
                toleratedExecutionDelayDuration,
                errorReporter);
        SimpleCsvFileWriter csvResultsLogWriter = null;
        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(
                timeSource,
                new ConcurrentErrorReporter(),
                TimeUnit.MILLISECONDS,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
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
        long toleratedExecutionDelayDuration = ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION_AS_MILLI;
        ExecutionDelayPolicy executionDelayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(
                timeSource,
                toleratedExecutionDelayDuration,
                errorReporter);
        SimpleCsvFileWriter csvResultsLogWriter = null;
        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(
                timeSource,
                new ConcurrentErrorReporter(),
                TimeUnit.MILLISECONDS,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
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
        assertThat(metricsService.results().startTimeAsMilli(), equalTo(-1l));
        assertThat(metricsService.results().latestFinishTimeAsMilli(), is(-1l));

        // scheduled: 1, actual: 2, duration: 1
        Operation<?> operation1 = DummyLdbcSnbInteractiveOperationInstances.read1();
        operation1.setScheduledStartTimeAsMilli(1l);
        operation1.setTimeStamp(1l);
        OperationResultReport operationResultReport1 = OperationResultReportTestHelper.create(1, "result one", operation1);
        OperationResultReportTestHelper.setActualStartTime(operationResultReport1, 2l);
        OperationResultReportTestHelper.setRunDuration(operationResultReport1, temporalUtil.convert(1, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS));

        metricsService.submitOperationResult(operationResultReport1);

        assertThat(metricsService.results().startTimeAsMilli(), equalTo(2l));
        assertThat(metricsService.results().latestFinishTimeAsMilli(), equalTo(3l));

        Operation<?> operation2 = DummyLdbcSnbInteractiveOperationInstances.read1();
        operation2.setScheduledStartTimeAsMilli(1l);
        operation2.setTimeStamp(1l);
        OperationResultReport operationResultReport2 = OperationResultReportTestHelper.create(2, "result two", operation2);
        OperationResultReportTestHelper.setActualStartTime(operationResultReport2, 8l);
        OperationResultReportTestHelper.setRunDuration(operationResultReport2, temporalUtil.convert(3, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS));

        metricsService.submitOperationResult(operationResultReport2);

        assertThat(metricsService.results().startTimeAsMilli(), equalTo(2l));
        assertThat(metricsService.results().latestFinishTimeAsMilli(), equalTo(11l));

        Operation<?> operation3 = DummyLdbcSnbInteractiveOperationInstances.read2();
        operation3.setScheduledStartTimeAsMilli(1l);
        operation3.setTimeStamp(1l);
        OperationResultReport operationResultReport3 = OperationResultReportTestHelper.create(2, "result three", operation3);
        OperationResultReportTestHelper.setActualStartTime(operationResultReport3, 11l);
        OperationResultReportTestHelper.setRunDuration(operationResultReport3, temporalUtil.convert(5, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS));

        metricsService.submitOperationResult(operationResultReport3);

        WorkloadResultsSnapshot results = metricsService.results();
        assertThat(results.startTimeAsMilli(), equalTo(2l));
        assertThat(results.latestFinishTimeAsMilli(), equalTo(16l));
    }
}
