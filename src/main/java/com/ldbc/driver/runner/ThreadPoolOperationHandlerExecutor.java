package com.ldbc.driver.runner;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.util.temporal.Duration;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class ThreadPoolOperationHandlerExecutor implements OperationHandlerExecutor {
    private final Duration POLL_TIMEOUT = Duration.fromMilli(100);

    private final ExecutorService threadPool;
    private final CompletionService<OperationResult> operationHandlerCompletionPool;

    private AtomicLong retrievedResults = new AtomicLong(0);
    private AtomicLong submittedHandlers = new AtomicLong(0);

    private boolean shutdown = false;

    public ThreadPoolOperationHandlerExecutor(int threadCount) {
        this.threadPool = createThreadPool(threadCount);
        this.operationHandlerCompletionPool = new ExecutorCompletionService<OperationResult>(threadPool);
    }

    private ExecutorService createThreadPool(int threadCount) {
        return Executors.newFixedThreadPool(threadCount);
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
            throw new OperationHandlerExecutorException(e.getCause());
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
            throw new OperationHandlerExecutorException(e.getCause());
        }
    }

    @Override
    public final void shutdown() {
        if (true == shutdown) return;
        threadPool.shutdown();
        shutdown = true;
    }
}
