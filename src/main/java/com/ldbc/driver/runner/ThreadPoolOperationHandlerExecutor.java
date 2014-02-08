package com.ldbc.driver.runner;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.util.temporal.Duration;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */

public class ThreadPoolOperationHandlerExecutor implements OperationHandlerExecutor {
    private final Duration POLL_TIMEOUT = Duration.fromMilli(100);

    private final ExecutorService threadPool;
    private final CompletionService<OperationResult> operationHandlerCompletionPool;

    private AtomicLong retrievedResults = new AtomicLong(0);
    private AtomicLong submittedHandlers = new AtomicLong(0);

    private AtomicBoolean shutdown = new AtomicBoolean(false);

    public ThreadPoolOperationHandlerExecutor(int threadCount) {
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        this.threadPool = Executors.newFixedThreadPool(threadCount, threadFactory);
        this.operationHandlerCompletionPool = new ExecutorCompletionService<OperationResult>(threadPool);
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
            OperationResult operationResult;
            operationResult = operationResultFuture.get();
            retrievedResults.incrementAndGet();
            return operationResult;
        } catch (Exception e) {
            throw new OperationHandlerExecutorException("Error encountered while trying to do non-blocking retrieval of next completed operation handler",
                    e.getCause());
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
        } catch (Exception e) {
            throw new OperationHandlerExecutorException("Error encountered while trying to do blocking retrieval of next completed operation handler",
                    e.getCause());
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
