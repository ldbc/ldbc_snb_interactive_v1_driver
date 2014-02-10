package com.ldbc.driver.runner;

import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.temporal.Duration;
import org.junit.Test;

import java.util.concurrent.*;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ThreadPoolOperationHandlerExecutorTests {
    @Test
    public void shouldRunOperationHandlerAndReturnExpectedResultWithoutError() throws InterruptedException, ExecutionException {
        OperationHandler<Operation<Integer>> operationHandler = new OperationHandler<Operation<Integer>>() {
            @Override
            protected OperationResult executeOperation(Operation<Integer> operation) throws DbException {
                return operation.buildResult(0, 42);
            }
        };

        Operation<?> operation = new Operation<Integer>() {
        };
        Spinner spinner = new Spinner(new ErrorLoggingOperationSchedulingPolicy(Duration.fromSeconds(1), true));
        operationHandler.init(spinner, operation);

        int threadCount = 1;
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        ExecutorService threadPoolExecutorService = Executors.newFixedThreadPool(threadCount, threadFactory);
        CompletionService<OperationResult> operationHandlerCompletionPool = new ExecutorCompletionService<OperationResult>(threadPoolExecutorService);

        operationHandlerCompletionPool.submit(operationHandler);

        Future<OperationResult> operationHandlerFuture = operationHandlerCompletionPool.take();

        OperationResult operationResult = operationHandlerFuture.get();

        assertThat((Integer) operationResult.result(), is(42));
    }

    @Test
    public void shouldRunOperationHandlerAndThrowExpectedException() throws InterruptedException, ExecutionException {
        OperationHandler<Operation<Integer>> operationHandler = new OperationHandler<Operation<Integer>>() {
            @Override
            protected OperationResult executeOperation(Operation<Integer> operation) throws DbException {
                throw new DbException("OperationHandler threw exception on purpose");
            }
        };

        Operation<?> operation = new Operation<Integer>() {
        };
        Spinner spinner = new Spinner(new ErrorLoggingOperationSchedulingPolicy(Duration.fromSeconds(1), true));
        operationHandler.init(spinner, operation);

        int threadCount = 1;
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        ExecutorService threadPoolExecutorService = Executors.newFixedThreadPool(threadCount, threadFactory);
        CompletionService<OperationResult> operationHandlerCompletionPool = new ExecutorCompletionService<OperationResult>(threadPoolExecutorService);

        operationHandlerCompletionPool.submit(operationHandler);

        Future<OperationResult> operationHandlerFuture = operationHandlerCompletionPool.take();

        boolean threwExpectedException = false;
        try {
            operationHandlerFuture.get();
        } catch (ExecutionException e) {
            threwExpectedException = true;
            assertThat(e.getCause(), instanceOf(DbException.class));
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
    public void shouldRunOperationHandlerAndThrowInterruptedExceptionWhenExecutorServiceShutdownAbruptly() throws InterruptedException, ExecutionException {
        OperationHandler<Operation<Integer>> operationHandler = new OperationHandler<Operation<Integer>>() {
            @Override
            protected OperationResult executeOperation(Operation<Integer> operation) throws DbException {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new DbException("OperationHandler was interrupted unexpectedly");
                }
                throw new DbException("OperationHandler threw exception on purpose");
            }
        };

        Operation<?> operation = new Operation<Integer>() {
        };
        Spinner spinner = new Spinner(new ErrorLoggingOperationSchedulingPolicy(Duration.fromSeconds(1), true));
        operationHandler.init(spinner, operation);

        int threadCount = 1;
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        ExecutorService threadPoolExecutorService = Executors.newFixedThreadPool(threadCount, threadFactory);
        CompletionService<OperationResult> operationHandlerCompletionPool = new ExecutorCompletionService<OperationResult>(threadPoolExecutorService);

        operationHandlerCompletionPool.submit(operationHandler);

        threadPoolExecutorService.shutdownNow();

        Future<OperationResult> operationHandlerFuture = operationHandlerCompletionPool.take();

        boolean threwExpectedException = false;
        try {
            operationHandlerFuture.get();
        } catch (ExecutionException e) {
            threwExpectedException = true;
            assertThat(e.getCause(), instanceOf(DbException.class));
            assertThat(e.getCause().getMessage(), is("OperationHandler was interrupted unexpectedly"));
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