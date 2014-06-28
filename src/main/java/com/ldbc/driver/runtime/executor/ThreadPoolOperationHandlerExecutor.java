package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.temporal.Duration;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class ThreadPoolOperationHandlerExecutor implements OperationHandlerExecutor {
    private final ExecutorService threadPool;

    private final AtomicLong submittedHandlers = new AtomicLong(0);
    private boolean shutdown = false;

    public ThreadPoolOperationHandlerExecutor(int threadCount) {
        ThreadFactory threadFactory = new ThreadFactory() {
            int count = 0;

            @Override
            public Thread newThread(Runnable runnable) {
                return new Thread(runnable, "ThreadPoolOperationHandlerExecutor.thread." + count);
            }
        };
        this.threadPool = Executors.newFixedThreadPool(threadCount, threadFactory);
    }

    @Override
    synchronized public final Future<OperationResultReport> execute(OperationHandler<?> operationHandler) {
        Future<OperationResultReport> future = threadPool.submit(operationHandler);
        submittedHandlers.incrementAndGet();
        return future;
    }

    @Override
    synchronized public final void shutdown(Duration wait) throws OperationHandlerExecutorException {
        if (true == shutdown)
            throw new OperationHandlerExecutorException("Executor has already been shutdown");
        try {
            threadPool.shutdown();
            boolean allHandlersCompleted = threadPool.awaitTermination(wait.asMilli(), TimeUnit.MILLISECONDS);
            if (false == allHandlersCompleted) {
                throw new OperationHandlerExecutorException("Executor shutdown before all handlers could complete execution");
            }
        } catch (Exception e) {
            throw new OperationHandlerExecutorException("Error encountered while trying to shutdown", e);
        }
        shutdown = true;
    }
}
