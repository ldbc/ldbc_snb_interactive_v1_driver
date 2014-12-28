package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.*;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.DefaultQueues;
import com.ldbc.driver.runtime.coordination.GlobalCompletionTimeReader;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TimeSource;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ThreadPoolOperationExecutor_NEW implements OperationExecutor_NEW {
    private final ExecutorService threadPoolExecutorService;
    private final AtomicLong uncompletedHandlers = new AtomicLong(0);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final OperationHandlerRunnableContextRetriever operationHandlerRunnableContextRetriever;

    public ThreadPoolOperationExecutor_NEW(int threadCount,
                                           int boundedQueueSize,
                                           Db db,
                                           WorkloadStreams.WorkloadStreamDefinition streamDefinition,
                                           LocalCompletionTimeWriter localCompletionTimeWriter,
                                           GlobalCompletionTimeReader globalCompletionTimeReader,
                                           Spinner spinner,
                                           TimeSource timeSource,
                                           ConcurrentErrorReporter errorReporter,
                                           ConcurrentMetricsService metricsService,
                                           ChildOperationGenerator childOperationGenerator) {
        this.operationHandlerRunnableContextRetriever = new OperationHandlerRunnableContextRetriever(
                streamDefinition,
                db,
                localCompletionTimeWriter,
                globalCompletionTimeReader,
                spinner,
                timeSource,
                errorReporter,
                metricsService
        );
        ThreadFactory threadFactory = new ThreadFactory() {
            private final long factoryTimeStampId = System.currentTimeMillis();
            int count = 0;

            @Override
            public Thread newThread(Runnable runnable) {
                Thread newThread = new Thread(
                        runnable,
                        ThreadPoolOperationExecutor_NEW.class.getSimpleName() + "-id(" + factoryTimeStampId + ")" + "-thread(" + count++ + ")"
                );
                return newThread;
            }
        };
        this.threadPoolExecutorService = ThreadPoolExecutorWithAfterExecute.newFixedThreadPool(
                threadCount,
                threadFactory,
                uncompletedHandlers,
                boundedQueueSize,
                childOperationGenerator,
                operationHandlerRunnableContextRetriever,
                errorReporter
        );
    }

    @Override
    public final void execute(Operation operation) throws OperationExecutorException {
        uncompletedHandlers.incrementAndGet();
        try {
            OperationHandlerRunnableContext operationHandlerRunnableContext =
                    operationHandlerRunnableContextRetriever.getInitializedHandlerFor(operation);
            threadPoolExecutorService.execute(operationHandlerRunnableContext);
        } catch (Throwable e) {
            throw new OperationExecutorException(
                    String.format("Error retrieving handler\nOperation: %s\n%s",
                            operation,
                            ConcurrentErrorReporter.stackTraceToString(e)),
                    e
            );
        }
    }

    @Override
    synchronized public final void shutdown(long waitAsMilli) throws OperationExecutorException {
        if (shutdown.get())
            throw new OperationExecutorException("Executor has already been shutdown");
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
                    throw new OperationExecutorException(errMsg);
                }
            }
        } catch (Exception e) {
            throw new OperationExecutorException("Error encountered while trying to shutdown", e);
        }
        shutdown.set(true);
    }

    @Override
    public long uncompletedOperationHandlerCount() {
        return uncompletedHandlers.get();
    }

    private static class ThreadPoolExecutorWithAfterExecute extends ThreadPoolExecutor {
        private final ChildOperationGenerator childOperationGenerator;
        private final OperationHandlerRunnableContextRetriever operationHandlerRunnableContextInitializer;
        private final ConcurrentErrorReporter errorReporter;

        public static ThreadPoolExecutorWithAfterExecute newFixedThreadPool(int threadCount,
                                                                            ThreadFactory threadFactory,
                                                                            AtomicLong uncompletedHandlers,
                                                                            int boundedQueueSize,
                                                                            ChildOperationGenerator childOperationGenerator,
                                                                            OperationHandlerRunnableContextRetriever operationHandlerRunnableContextInitializer,
                                                                            ConcurrentErrorReporter errorReporter) {
            int corePoolSize = threadCount;
            int maximumPoolSize = threadCount;
            long keepAliveTime = 0;
            TimeUnit unit = TimeUnit.MILLISECONDS;
            BlockingQueue<Runnable> workQueue = DefaultQueues.newAlwaysBlockingBounded(boundedQueueSize);
            return new ThreadPoolExecutorWithAfterExecute(
                    corePoolSize,
                    maximumPoolSize,
                    keepAliveTime,
                    unit,
                    workQueue,
                    threadFactory,
                    uncompletedHandlers,
                    childOperationGenerator,
                    operationHandlerRunnableContextInitializer,
                    errorReporter
            );
        }

        private final AtomicLong uncompletedHandlers;

        private ThreadPoolExecutorWithAfterExecute(int corePoolSize,
                                                   int maximumPoolSize,
                                                   long keepAliveTime,
                                                   TimeUnit unit,
                                                   BlockingQueue<Runnable> workQueue,
                                                   ThreadFactory threadFactory,
                                                   AtomicLong uncompletedHandlers,
                                                   ChildOperationGenerator childOperationGenerator,
                                                   OperationHandlerRunnableContextRetriever operationHandlerRunnableContextInitializer,
                                                   ConcurrentErrorReporter errorReporter) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
            this.uncompletedHandlers = uncompletedHandlers;
            this.childOperationGenerator = childOperationGenerator;
            this.operationHandlerRunnableContextInitializer = operationHandlerRunnableContextInitializer;
            this.errorReporter = errorReporter;
        }

        // Note, this occurs in same worker thread as beforeExecute() and run()
        @Override
        protected void afterExecute(Runnable operationHandlerRunner, Throwable throwable) {
            super.afterExecute(operationHandlerRunner, throwable);
            OperationHandlerRunnableContext operationHandlerRunnableContext = (OperationHandlerRunnableContext) operationHandlerRunner;

            if (null != childOperationGenerator) {
                try {
                    OperationResultReport resultReport = operationHandlerRunnableContext.operationResultReport();
                    double state = childOperationGenerator.initialState();
                    Operation childOperation;
                    while (null != (childOperation = childOperationGenerator.nextOperation(state, resultReport))) {
                        OperationHandlerRunnableContext childOperationHandlerRunnableContext =
                                operationHandlerRunnableContextInitializer.getInitializedHandlerFor(childOperation);
                        childOperationHandlerRunnableContext.run();
                        state = childOperationGenerator.updateState(state);
                        resultReport = childOperationHandlerRunnableContext.operationResultReport();
                        childOperationHandlerRunnableContext.cleanup();
                    }
                } catch (Throwable e) {
                    errorReporter.reportError(this, String.format("Error retrieving handler\n%s",
                            ConcurrentErrorReporter.stackTraceToString(e)));
                }
            }

            operationHandlerRunnableContext.cleanup();
            uncompletedHandlers.decrementAndGet();
        }
    }
}
