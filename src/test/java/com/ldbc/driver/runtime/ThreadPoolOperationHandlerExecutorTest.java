package com.ldbc.driver.runtime;

import com.ldbc.driver.*;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.executor.OperationHandlerExecutor;
import com.ldbc.driver.runtime.executor.OperationHandlerExecutorException;
import com.ldbc.driver.runtime.executor.ThreadPoolOperationHandlerExecutor;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.ErrorReportingTerminatingExecutionDelayPolicy;
import com.ldbc.driver.runtime.scheduling.ExecutionDelayPolicy;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.workloads.dummy.NothingOperation;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ThreadPoolOperationHandlerExecutorTest {
    TimeSource TIME_SOURCE = new SystemTimeSource();

    @Test
    public void executorShouldReturnExpectedResult() throws OperationHandlerExecutorException, ExecutionException, InterruptedException, OperationException {
        // Given
        Duration toleratedDelay = Duration.fromMilli(100);
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ExecutionDelayPolicy delayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(TIME_SOURCE, toleratedDelay, errorReporter);
        Spinner spinner = new Spinner(TIME_SOURCE, Spinner.DEFAULT_SLEEP_DURATION_10_MILLI, delayPolicy);
        ConcurrentCompletionTimeService completionTimeService = new DummyConcurrentCompletionTimeService();
        ConcurrentMetricsService metricsService = new DummyConcurrentMetricsService();

        int threadCount = 1;
        OperationHandlerExecutor executor = new ThreadPoolOperationHandlerExecutor(threadCount);

        Operation<?> operation = new NothingOperation();
        operation.setScheduledStartTime(TIME_SOURCE.now().plus(Duration.fromMilli(200)));
        OperationHandler<?> handler = new OperationHandler<Operation<Integer>>() {
            @Override
            protected OperationResultReport executeOperation(Operation operation) throws DbException {
                return operation.buildResult(1, 42);
            }
        };

        // When
        handler.init(TIME_SOURCE, spinner, operation, completionTimeService, errorReporter, metricsService);

        // Then
        Future<OperationResultReport> handlerFuture = executor.execute(handler);
        Integer handlerResult = (Integer) handlerFuture.get().operationResult();
        assertThat(handlerResult, is(42));
        executor.shutdown(Duration.fromSeconds(1));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
    }


    @Test
    public void executorShouldReturnAllResults() throws OperationHandlerExecutorException, ExecutionException, InterruptedException, OperationException {
        // Given
        Duration toleratedDelay = Duration.fromMilli(100);
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ExecutionDelayPolicy delayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(TIME_SOURCE, toleratedDelay, errorReporter);
        Spinner spinner = new Spinner(TIME_SOURCE, Spinner.DEFAULT_SLEEP_DURATION_10_MILLI, delayPolicy);
        ConcurrentCompletionTimeService completionTimeService = new DummyConcurrentCompletionTimeService();
        ConcurrentMetricsService metricsService = new DummyConcurrentMetricsService();

        int threadCount = 1;
        OperationHandlerExecutor executor = new ThreadPoolOperationHandlerExecutor(threadCount);

        Operation<?> operation1 = new NothingOperation();
        operation1.setScheduledStartTime(TIME_SOURCE.now().plus(Duration.fromMilli(100)));
        Operation<?> operation2 = new NothingOperation();
        operation2.setScheduledStartTime(operation1.scheduledStartTime().plus(Duration.fromMilli(100)));
        OperationHandler<?> handler1 = new OperationHandler<Operation<Integer>>() {
            @Override
            protected OperationResultReport executeOperation(Operation operation) throws DbException {
                return operation.buildResult(1, 1);
            }
        };
        OperationHandler<?> handler2 = new OperationHandler<Operation<Integer>>() {
            @Override
            protected OperationResultReport executeOperation(Operation operation) throws DbException {
                return operation.buildResult(1, 2);
            }
        };

        // When
        handler1.init(TIME_SOURCE, spinner, operation1, completionTimeService, errorReporter, metricsService);
        handler2.init(TIME_SOURCE, spinner, operation2, completionTimeService, errorReporter, metricsService);

        // Then
        Future<OperationResultReport> handlerFuture1 = executor.execute(handler1);
        Future<OperationResultReport> handlerFuture2 = executor.execute(handler2);

        Integer handlerResult1 = (Integer) handlerFuture1.get().operationResult();
        assertThat(handlerResult1, is(1));
        Integer handlerResult2 = (Integer) handlerFuture2.get().operationResult();
        assertThat(handlerResult2, is(2));

        executor.shutdown(Duration.fromSeconds(1));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
    }

    @Test
    public void executorShouldThrowExceptionIfShutdownMultipleTimes() throws OperationHandlerExecutorException, ExecutionException, InterruptedException, OperationException {
        // Given
        Duration toleratedDelay = Duration.fromMilli(100);
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ExecutionDelayPolicy delayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(TIME_SOURCE, toleratedDelay, errorReporter);
        Spinner spinner = new Spinner(TIME_SOURCE, Spinner.DEFAULT_SLEEP_DURATION_10_MILLI, delayPolicy);
        ConcurrentCompletionTimeService completionTimeService = new DummyConcurrentCompletionTimeService();
        ConcurrentMetricsService metricsService = new DummyConcurrentMetricsService();

        int threadCount = 1;
        OperationHandlerExecutor executor = new ThreadPoolOperationHandlerExecutor(threadCount);

        Operation<?> operation = new NothingOperation();
        operation.setScheduledStartTime(TIME_SOURCE.now().plus(Duration.fromMilli(200)));
        OperationHandler<?> handler = new OperationHandler<Operation<Integer>>() {
            @Override
            protected OperationResultReport executeOperation(Operation operation) throws DbException {
                return operation.buildResult(1, 42);
            }
        };

        // When
        handler.init(TIME_SOURCE, spinner, operation, completionTimeService, errorReporter, metricsService);

        // Then
        Future<OperationResultReport> handlerFuture = executor.execute(handler);
        Integer handlerResult = (Integer) handlerFuture.get().operationResult();
        assertThat(handlerResult, is(42));
        executor.shutdown(Duration.fromSeconds(1));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

        boolean exceptionThrown = false;
        try {
            executor.shutdown(Duration.fromSeconds(1));
        } catch (OperationHandlerExecutorException e) {
            exceptionThrown = true;
        }

        assertThat(exceptionThrown, is(true));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
    }
}
