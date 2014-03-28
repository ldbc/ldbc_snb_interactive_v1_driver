package com.ldbc.driver.runtime;

import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.scheduling.*;
import com.ldbc.driver.runtime.executor.OperationHandlerExecutor;
import com.ldbc.driver.runtime.executor.OperationHandlerExecutorException;
import com.ldbc.driver.runtime.executor.ThreadPoolOperationHandlerExecutor;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ThreadPoolOperationHandlerExecutorTests {

    @Test
    public void executorShouldReturnExpectedResult() throws OperationHandlerExecutorException, ExecutionException, InterruptedException {
        // Given
        Duration toleratedDelay = Duration.fromMilli(100);
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ExecutionDelayPolicy delayPolicy = new ErrorReportingExecutionDelayPolicy(toleratedDelay, errorReporter);
        Spinner spinner = new Spinner(delayPolicy);
        ConcurrentCompletionTimeService completionTimeService = new DummyConcurrentCompletionTimeService();
        ConcurrentMetricsService metricsService = new DummyConcurrentMetricsService();
        CompletionTimeValidator completionTimeValidator = new AlwaysValidCompletionTimeValidator();

        int threadCount = 1;
        OperationHandlerExecutor executor = new ThreadPoolOperationHandlerExecutor(threadCount);

        Operation<?> operation = new Operation<Integer>() {
        };
        operation.setScheduledStartTime(Time.now().plus(Duration.fromMilli(200)));
        OperationHandler<?> handler = new OperationHandler<Operation<Integer>>() {
            @Override
            protected OperationResult executeOperation(Operation operation) throws DbException {
                return operation.buildResult(1, 42);
            }
        };

        // When
        handler.init(spinner, operation, completionTimeService, errorReporter, metricsService, completionTimeValidator);

        // Then
        Future<OperationResult> handlerFuture = executor.execute(handler);
        Integer handlerResult = (Integer) handlerFuture.get().result();
        assertThat(handlerResult, is(42));
        executor.shutdown(Duration.fromSeconds(1));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
    }


    @Test
    public void executorShouldReturnAllResults() throws OperationHandlerExecutorException, ExecutionException, InterruptedException {
        // Given
        Duration toleratedDelay = Duration.fromMilli(100);
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ExecutionDelayPolicy delayPolicy = new ErrorReportingExecutionDelayPolicy(toleratedDelay, errorReporter);
        Spinner spinner = new Spinner(delayPolicy);
        ConcurrentCompletionTimeService completionTimeService = new DummyConcurrentCompletionTimeService();
        ConcurrentMetricsService metricsService = new DummyConcurrentMetricsService();
        CompletionTimeValidator completionTimeValidator = new AlwaysValidCompletionTimeValidator();

        int threadCount = 1;
        OperationHandlerExecutor executor = new ThreadPoolOperationHandlerExecutor(threadCount);

        Operation<?> operation1 = new Operation<Integer>() {
        };
        operation1.setScheduledStartTime(Time.now().plus(Duration.fromMilli(100)));
        Operation<?> operation2 = new Operation<Integer>() {
        };
        operation2.setScheduledStartTime(operation1.scheduledStartTime().plus(Duration.fromMilli(100)));
        OperationHandler<?> handler1 = new OperationHandler<Operation<Integer>>() {
            @Override
            protected OperationResult executeOperation(Operation operation) throws DbException {
                return operation.buildResult(1, 1);
            }
        };
        OperationHandler<?> handler2 = new OperationHandler<Operation<Integer>>() {
            @Override
            protected OperationResult executeOperation(Operation operation) throws DbException {
                return operation.buildResult(1, 2);
            }
        };

        // When
        handler1.init(spinner, operation1, completionTimeService, errorReporter, metricsService, completionTimeValidator);
        handler2.init(spinner, operation2, completionTimeService, errorReporter, metricsService, completionTimeValidator);

        // Then
        Future<OperationResult> handlerFuture1 = executor.execute(handler1);
        Future<OperationResult> handlerFuture2 = executor.execute(handler2);

        Integer handlerResult1 = (Integer) handlerFuture1.get().result();
        assertThat(handlerResult1, is(1));
        Integer handlerResult2 = (Integer) handlerFuture2.get().result();
        assertThat(handlerResult2, is(2));

        executor.shutdown(Duration.fromSeconds(1));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
    }

    @Test
    public void executorShouldThrowExceptionIfShutdownMultipleTimes() throws OperationHandlerExecutorException, ExecutionException, InterruptedException {
        // Given
        Duration toleratedDelay = Duration.fromMilli(100);
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ExecutionDelayPolicy delayPolicy = new ErrorReportingExecutionDelayPolicy(toleratedDelay, errorReporter);
        Spinner spinner = new Spinner(delayPolicy);
        ConcurrentCompletionTimeService completionTimeService = new DummyConcurrentCompletionTimeService();
        ConcurrentMetricsService metricsService = new DummyConcurrentMetricsService();
        CompletionTimeValidator completionTimeValidator = new AlwaysValidCompletionTimeValidator();

        int threadCount = 1;
        OperationHandlerExecutor executor = new ThreadPoolOperationHandlerExecutor(threadCount);

        Operation<?> operation = new Operation<Integer>() {
        };
        operation.setScheduledStartTime(Time.now().plus(Duration.fromMilli(200)));
        OperationHandler<?> handler = new OperationHandler<Operation<Integer>>() {
            @Override
            protected OperationResult executeOperation(Operation operation) throws DbException {
                return operation.buildResult(1, 42);
            }
        };

        // When
        handler.init(spinner, operation, completionTimeService, errorReporter, metricsService, completionTimeValidator);

        // Then
        Future<OperationResult> handlerFuture = executor.execute(handler);
        Integer handlerResult = (Integer) handlerFuture.get().result();
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
