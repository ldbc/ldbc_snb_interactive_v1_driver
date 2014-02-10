package com.ldbc.driver.runner;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.temporal.Duration;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ThreadPoolOperationHandlerExecutor implements OperationHandlerExecutor {
    private final Duration POLL_TIMEOUT = Duration.fromMilli(100);

    private final ExecutorService threadPool;
    private final CompletionService<OperationResult> operationHandlerCompletionPool;

    private final AtomicLong retrievedResults = new AtomicLong(0);
    private final AtomicLong submittedHandlers = new AtomicLong(0);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private final ConcurrentErrorReporter concurrentErrorReporter;

    public ThreadPoolOperationHandlerExecutor(int threadCount, ConcurrentErrorReporter concurrentErrorReporter) {
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        this.threadPool = Executors.newFixedThreadPool(threadCount, threadFactory);
        this.operationHandlerCompletionPool = new ExecutorCompletionService<OperationResult>(threadPool);
        this.concurrentErrorReporter = concurrentErrorReporter;
    }

    @Override
    public final void execute(OperationHandler<?> operationHandler) {
        operationHandlerCompletionPool.submit(operationHandler);
        submittedHandlers.incrementAndGet();
    }

    @Override
    public final OperationResult nextOperationResultNonBlocking() throws OperationHandlerExecutorException {
        try {
            Future<OperationResult> operationResultFuture = operationHandlerCompletionPool.poll(
                    POLL_TIMEOUT.asMilli(), TimeUnit.MILLISECONDS);
            if (null == operationResultFuture) return null;
            OperationResult operationResult = operationResultFuture.get();
            retrievedResults.incrementAndGet();
            return operationResult;
        } catch (ExecutionException e) {
            Throwable realCause = e.getCause();
            String errMsg = String.format("Operation handler threw an exception\n%s", ConcurrentErrorReporter.stackTraceToString(realCause));
            concurrentErrorReporter.reportError(this, errMsg);
            throw new OperationHandlerExecutorException(errMsg, realCause.getCause());
        } catch (InterruptedException e) {
            String errMsg = "Operation handler was interrupted";
            concurrentErrorReporter.reportError(this, errMsg);
            throw new OperationHandlerExecutorException(errMsg, e.getCause());
        } catch (CancellationException e) {
            String errMsg = "Operation handler was cancelled";
            concurrentErrorReporter.reportError(this, errMsg);
            throw new OperationHandlerExecutorException(errMsg, e.getCause());
        } catch (Exception e) {
            String errMsg = "Error encountered while trying to do non-blocking retrieval of next completed operation handler";
            concurrentErrorReporter.reportError(this, errMsg);
            throw new OperationHandlerExecutorException(errMsg, e.getCause());
        }
    }

    @Override
    public final OperationResult nextOperationResultBlocking() throws OperationHandlerExecutorException {
        try {
            if (submittedHandlers.get() == retrievedResults.get()) return null;
            Future<OperationResult> operationResultFuture = operationHandlerCompletionPool.take();
            OperationResult operationResult = operationResultFuture.get();
            retrievedResults.incrementAndGet();
            return operationResult;
        } catch (ExecutionException e) {
            Throwable realCause = e.getCause();
            String errMsg = String.format("Operation handler threw an exception\n%s", ConcurrentErrorReporter.stackTraceToString(realCause));
            concurrentErrorReporter.reportError(this, errMsg);
            throw new OperationHandlerExecutorException(errMsg, realCause.getCause());
        } catch (InterruptedException e) {
            String errMsg = "Operation handler was interrupted";
            concurrentErrorReporter.reportError(this, errMsg);
            throw new OperationHandlerExecutorException(errMsg, e.getCause());
        } catch (CancellationException e) {
            String errMsg = "Operation handler was cancelled";
            concurrentErrorReporter.reportError(this, errMsg);
            throw new OperationHandlerExecutorException(errMsg, e.getCause());
        } catch (Exception e) {
            String errMsg = "Error encountered while trying to do blocking retrieval of next completed operation handler";
            concurrentErrorReporter.reportError(this, errMsg);
            throw new OperationHandlerExecutorException(errMsg, e.getCause());
        }
    }

    @Override
    public final void shutdown() throws OperationHandlerExecutorException {
        if (true == shutdown.get()) return;
        try {
            threadPool.shutdown();
            shutdown.set(true);
        } catch (SecurityException e) {
            throw new OperationHandlerExecutorException("Error encountered while trying to shutdown", e.getCause());
        }
    }
}
