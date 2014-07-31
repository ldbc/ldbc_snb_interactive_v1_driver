package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.temporal.Duration;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ThreadPoolOperationHandlerExecutor implements OperationHandlerExecutor {
    private final ExecutorService threadPoolExecutorService;

    private final AtomicLong submittedHandlers = new AtomicLong(0);
    private AtomicBoolean shutdown = new AtomicBoolean(false);

    public ThreadPoolOperationHandlerExecutor(int threadCount) {
        ThreadFactory threadFactory = new ThreadFactory() {
            private final long factoryTimeStampId = System.currentTimeMillis();
            int count = 0;

            @Override
            public Thread newThread(Runnable runnable) {
                Thread newThread = new Thread(
                        runnable,
                        ThreadPoolOperationHandlerExecutor.class.getSimpleName() + "-id(" + factoryTimeStampId + ")" + "-thread(" + count++ + ")");
                return newThread;
            }
        };
        this.threadPoolExecutorService = Executors.newFixedThreadPool(threadCount, threadFactory);
    }

    @Override
    synchronized public final Future<OperationResultReport> execute(OperationHandler<?> operationHandler) {
        Future<OperationResultReport> future = threadPoolExecutorService.submit(operationHandler);
        submittedHandlers.incrementAndGet();
        return future;
    }

    @Override
    synchronized public final void shutdown(Duration wait) throws OperationHandlerExecutorException {
        if (shutdown.get())
            throw new OperationHandlerExecutorException("Executor has already been shutdown");
        try {
            threadPoolExecutorService.shutdown();
            boolean allHandlersCompleted = threadPoolExecutorService.awaitTermination(wait.asMilli(), TimeUnit.MILLISECONDS);
            if (false == allHandlersCompleted) {
                throw new OperationHandlerExecutorException("Executor shutdown before all handlers could complete execution");
            }
        } catch (Exception e) {
            throw new OperationHandlerExecutorException("Error encountered while trying to shutdown", e);
        }
        shutdown.set(true);
    }
}
