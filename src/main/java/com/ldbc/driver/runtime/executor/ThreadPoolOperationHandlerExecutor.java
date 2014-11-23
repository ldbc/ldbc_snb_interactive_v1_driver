package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.OperationHandlerRunnableContext;
import com.ldbc.driver.runtime.DefaultQueues;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ThreadPoolOperationHandlerExecutor implements OperationHandlerExecutor {
    private final ExecutorService threadPoolExecutorService;
    private final AtomicLong uncompletedHandlers = new AtomicLong(0);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    public ThreadPoolOperationHandlerExecutor(int threadCount, int boundedQueueSize) {
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
        this.threadPoolExecutorService = ThreadPoolExecutorWithAfterExecute.newFixedThreadPool(threadCount, threadFactory, uncompletedHandlers, boundedQueueSize);
    }

    @Override
    public final void execute(OperationHandlerRunnableContext operationHandlerRunner) {
        uncompletedHandlers.incrementAndGet();
        threadPoolExecutorService.execute(operationHandlerRunner);
    }

    @Override
    synchronized public final void shutdown(long waitAsMilli) throws OperationHandlerExecutorException {
        if (shutdown.get())
            throw new OperationHandlerExecutorException("Executor has already been shutdown");
        try {
            threadPoolExecutorService.shutdown();
            boolean allHandlersCompleted = threadPoolExecutorService.awaitTermination(waitAsMilli, TimeUnit.MILLISECONDS);
            if (false == allHandlersCompleted) {
                List<Runnable> stillRunningThreads = threadPoolExecutorService.shutdownNow();
                if (false == stillRunningThreads.isEmpty()) {
                    String errMsg = String.format(
                            "%s shutdown before all handlers could complete\n%s handlers were queued for execution but not yet started\n%s handlers were mid-execution",
                            getClass().getSimpleName(),
                            stillRunningThreads.size(),
                            uncompletedHandlers.get() - stillRunningThreads.size());
                    throw new OperationHandlerExecutorException(errMsg);
                }
            }
        } catch (Exception e) {
            throw new OperationHandlerExecutorException("Error encountered while trying to shutdown", e);
        }
        shutdown.set(true);
    }

    @Override
    public long uncompletedOperationHandlerCount() {
        return uncompletedHandlers.get();
    }

    private static class ThreadPoolExecutorWithAfterExecute extends ThreadPoolExecutor {
        public static ThreadPoolExecutorWithAfterExecute newFixedThreadPool(int threadCount, ThreadFactory threadFactory, AtomicLong uncompletedHandlers, int boundedQueueSize) {
            int corePoolSize = threadCount;
            int maximumPoolSize = threadCount;
            long keepAliveTime = 0;
            TimeUnit unit = TimeUnit.MILLISECONDS;
            BlockingQueue<Runnable> workQueue = DefaultQueues.newAlwaysBlockingBounded(boundedQueueSize);
            return new ThreadPoolExecutorWithAfterExecute(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, uncompletedHandlers);
        }

        private final AtomicLong uncompletedHandlers;

        private ThreadPoolExecutorWithAfterExecute(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, AtomicLong uncompletedHandlers) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
            this.uncompletedHandlers = uncompletedHandlers;
        }

        @Override
        protected void afterExecute(Runnable operationHandlerRunner, Throwable throwable) {
            super.afterExecute(operationHandlerRunner, throwable);
            ((OperationHandlerRunnableContext) operationHandlerRunner).cleanup();
            uncompletedHandlers.decrementAndGet();
        }
    }
}
