package com.ldbc.driver.runtime;

import com.ldbc.driver.*;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.LoggingExecutionDelayPolicy;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.testutils.NothingOperation;
import org.junit.Test;

import java.util.concurrent.*;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ExecutorTest {
    TimeSource TIME_SOURCE = new SystemTimeSource();

    @Test
    public void shouldRunOperationHandlerAndReturnExpectedResultWithoutError() throws InterruptedException, ExecutionException, CompletionTimeException, OperationException {
        ConcurrentCompletionTimeService concurrentCompletionTimeService = new DummyConcurrentCompletionTimeService();
        ConcurrentMetricsService metricsService = new DummyConcurrentMetricsService();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        OperationHandler<Operation<Integer>> operationHandler = new OperationHandler<Operation<Integer>>() {
            @Override
            protected OperationResultReport executeOperation(Operation<Integer> operation) throws DbException {
                return operation.buildResult(0, 42);
            }
        };

        Operation<?> operation = new NothingOperation();
        operation.setScheduledStartTime(TIME_SOURCE.now().plus(Duration.fromSeconds(1)));
        Spinner spinner = new Spinner(TIME_SOURCE, Spinner.DEFAULT_SLEEP_DURATION_10_MILLI, new LoggingExecutionDelayPolicy(Duration.fromSeconds(1)));
        operationHandler.init(TIME_SOURCE, spinner, operation, concurrentCompletionTimeService, errorReporter, metricsService);

        int threadCount = 1;
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        ExecutorService threadPoolExecutorService = Executors.newFixedThreadPool(threadCount, threadFactory);
        CompletionService<OperationResultReport> operationHandlerCompletionPool = new ExecutorCompletionService<OperationResultReport>(threadPoolExecutorService);

        operationHandlerCompletionPool.submit(operationHandler);

        Future<OperationResultReport> operationHandlerFuture = operationHandlerCompletionPool.take();

        OperationResultReport operationResultReport = operationHandlerFuture.get();

        assertThat((Integer) operationResultReport.operationResult(), is(42));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
    }

    @Test
    public void shouldRunOperationHandlerAndThrowExpectedException() throws InterruptedException, ExecutionException, CompletionTimeException, OperationException {
        ConcurrentCompletionTimeService concurrentCompletionTimeService = new DummyConcurrentCompletionTimeService();
        ConcurrentMetricsService metricsService = new DummyConcurrentMetricsService();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        OperationHandler<Operation<Integer>> operationHandler = new OperationHandler<Operation<Integer>>() {
            @Override
            protected OperationResultReport executeOperation(Operation<Integer> operation) throws DbException {
                throw new DbException("OperationHandler threw exception on purpose");
            }
        };

        Operation<?> operation = new NothingOperation();
        operation.setScheduledStartTime(TIME_SOURCE.now().plus(Duration.fromSeconds(1)));
        Spinner spinner = new Spinner(TIME_SOURCE, Spinner.DEFAULT_SLEEP_DURATION_10_MILLI, new LoggingExecutionDelayPolicy(Duration.fromSeconds(1)));
        operationHandler.init(TIME_SOURCE, spinner, operation, concurrentCompletionTimeService, errorReporter, metricsService);

        int threadCount = 1;
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        ExecutorService threadPoolExecutorService = Executors.newFixedThreadPool(threadCount, threadFactory);
        CompletionService<OperationResultReport> operationHandlerCompletionPool = new ExecutorCompletionService<OperationResultReport>(threadPoolExecutorService);

        operationHandlerCompletionPool.submit(operationHandler);

        Future<OperationResultReport> operationHandlerFuture = operationHandlerCompletionPool.take();

        operationHandlerFuture.get();
        assertThat(errorReporter.errorEncountered(), is(true));
    }

    @Test
    public void shouldRunOperationHandlerAndThrowInterruptedExceptionWhenExecutorServiceShutdownAbruptly() throws InterruptedException, ExecutionException, CompletionTimeException, OperationException {
        ConcurrentCompletionTimeService concurrentCompletionTimeService = new DummyConcurrentCompletionTimeService();
        ConcurrentMetricsService metricsService = new DummyConcurrentMetricsService();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        OperationHandler<Operation<Integer>> operationHandler = new OperationHandler<Operation<Integer>>() {
            @Override
            protected OperationResultReport executeOperation(Operation<Integer> operation) throws DbException {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new DbException("OperationHandler was interrupted unexpectedly");
                }
                throw new DbException("OperationHandler threw exception on purpose");
            }
        };

        Operation<?> operation = new NothingOperation();
        operation.setScheduledStartTime(TIME_SOURCE.now().plus(Duration.fromSeconds(1)));
        Spinner spinner = new Spinner(TIME_SOURCE, Spinner.DEFAULT_SLEEP_DURATION_10_MILLI, new LoggingExecutionDelayPolicy(Duration.fromSeconds(1)));
        operationHandler.init(TIME_SOURCE, spinner, operation, concurrentCompletionTimeService, errorReporter, metricsService);

        int threadCount = 1;
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        ExecutorService threadPoolExecutorService = Executors.newFixedThreadPool(threadCount, threadFactory);
        CompletionService<OperationResultReport> operationHandlerCompletionPool = new ExecutorCompletionService<OperationResultReport>(threadPoolExecutorService);

        operationHandlerCompletionPool.submit(operationHandler);

        threadPoolExecutorService.shutdownNow();

        Future<OperationResultReport> operationHandlerFuture = operationHandlerCompletionPool.take();

        operationHandlerFuture.get();
        assertThat(errorReporter.errorEncountered(), is(true));
    }

    @Test
    public void shouldRunTaskAndReturnExpectedResultWithoutError() throws InterruptedException, ExecutionException {
        int threadCount = 1;
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        ExecutorService threadPoolExecutorService = Executors.newFixedThreadPool(threadCount, threadFactory);
        CompletionService<Integer> operationHandlerCompletionPool = new ExecutorCompletionService<Integer>(threadPoolExecutorService);

        ErrorableCallable task = new ErrorableCallable(false);

        operationHandlerCompletionPool.submit(task);

        Future<Integer> taskFuture = operationHandlerCompletionPool.take();

        Integer taskResult = taskFuture.get();

        assertThat(taskResult, is(1));
    }

    @Test
    public void shouldRunTaskAndThrowExpectedException() throws InterruptedException, ExecutionException {
        int threadCount = 1;
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        ExecutorService threadPoolExecutorService = Executors.newFixedThreadPool(threadCount, threadFactory);
        CompletionService<Integer> operationHandlerCompletionPool = new ExecutorCompletionService<Integer>(threadPoolExecutorService);

        ErrorableCallable task = new ErrorableCallable(true);

        operationHandlerCompletionPool.submit(task);

        Future<Integer> taskFuture = operationHandlerCompletionPool.take();

        boolean threwExpectedException = false;
        try {
            taskFuture.get();
        } catch (ExecutionException e) {
            threwExpectedException = true;
            assertThat(e.getCause(), instanceOf(ErrorableCallableException.class));
        } catch (InterruptedException e) {
            threwExpectedException = false;
        } catch (CancellationException e) {
            threwExpectedException = false;
        } catch (Exception e) {
            threwExpectedException = false;
        }

        assertThat(threwExpectedException, is(true));
    }

    @Test
    public void shouldRunTaskAndThrowInterruptedExceptionWhenExecutorServiceShutdownAbruptly() throws InterruptedException, ExecutionException {
        int threadCount = 1;
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        ExecutorService threadPoolExecutorService = Executors.newFixedThreadPool(threadCount, threadFactory);
        CompletionService<Integer> operationHandlerCompletionPool = new ExecutorCompletionService<Integer>(threadPoolExecutorService);

        ErrorableCallable task = new ErrorableCallable(true);

        operationHandlerCompletionPool.submit(task);

        threadPoolExecutorService.shutdownNow();

        Future<Integer> taskFuture = operationHandlerCompletionPool.take();

        boolean threwExpectedException = false;
        try {
            taskFuture.get();
        } catch (ExecutionException e) {
            threwExpectedException = true;
            assertThat(e.getCause(), instanceOf(InterruptedException.class));
        } catch (InterruptedException e) {
            threwExpectedException = false;
        } catch (CancellationException e) {
            threwExpectedException = false;
        } catch (Exception e) {
            threwExpectedException = false;
        }

        assertThat(threwExpectedException, is(true));
    }

    class ErrorableCallable implements Callable<Integer> {
        private final boolean causeError;

        public ErrorableCallable(boolean causeError) {
            this.causeError = causeError;
        }

        public Integer call() throws Exception {
            Thread.sleep(1000);
            if (causeError)
                throw new ErrorableCallableException("ErrorableCallable errored");
            return 1;
        }
    }

    class ErrorableCallableException extends Exception {
        public ErrorableCallableException(String message) {
            super(message);
        }
    }

}