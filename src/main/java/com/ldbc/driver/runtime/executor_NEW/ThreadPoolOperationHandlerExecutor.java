package com.ldbc.driver.runtime.executor_NEW;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ThreadPoolOperationHandlerExecutor implements OperationHandlerExecutor {
    private final ExecutorService threadPool;
    private final CompletionService<OperationResult> operationHandlerCompletionPool;

    private final AtomicLong submittedHandlers = new AtomicLong(0);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    public ThreadPoolOperationHandlerExecutor(int threadCount) {
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        this.threadPool = Executors.newFixedThreadPool(threadCount, threadFactory);
        this.operationHandlerCompletionPool = new ExecutorCompletionService<OperationResult>(threadPool);
    }

    @Override
    public final Future<OperationResult> execute(OperationHandler<?> operationHandler) {
        Future<OperationResult> future = operationHandlerCompletionPool.submit(operationHandler);
        submittedHandlers.incrementAndGet();
        return future;
    }

    @Override
    public final void shutdown() throws OperationHandlerExecutorException {
        if (true == shutdown.get()) return;
        try {
            threadPool.shutdown();
            shutdown.set(true);
        } catch (Exception e) {
            throw new OperationHandlerExecutorException("Error encountered while trying to shutdown", e.getCause());
        }
    }
}
