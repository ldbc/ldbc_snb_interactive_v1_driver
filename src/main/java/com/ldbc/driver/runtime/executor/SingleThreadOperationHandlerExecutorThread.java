package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.OperationHandlerRunnableContext;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.QueueEventFetcher;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SingleThreadOperationHandlerExecutorThread extends Thread {
    private final QueueEventFetcher<OperationHandlerRunnableContext> operationHandlerRunnerQueueEventFetcher;
    private final ConcurrentErrorReporter errorReporter;
    private final AtomicLong uncompletedHandlers;
    private final AtomicBoolean forcedShutdownRequested = new AtomicBoolean(false);

    SingleThreadOperationHandlerExecutorThread(Queue<OperationHandlerRunnableContext> operationHandlerRunnerQueue,
                                               ConcurrentErrorReporter errorReporter,
                                               AtomicLong uncompletedHandlers) {
        super(SingleThreadOperationHandlerExecutorThread.class.getSimpleName() + "-" + System.currentTimeMillis());
        this.operationHandlerRunnerQueueEventFetcher = QueueEventFetcher.queueEventFetcherFor(operationHandlerRunnerQueue);
        this.errorReporter = errorReporter;
        this.uncompletedHandlers = uncompletedHandlers;
    }

    @Override
    public void run() {
        try {
            OperationHandlerRunnableContext operationHandlerRunner = operationHandlerRunnerQueueEventFetcher.fetchNextEvent();
            while (operationHandlerRunner != SingleThreadOperationHandlerExecutor.TERMINATE_HANDLER_RUNNER && false == forcedShutdownRequested.get()) {
                operationHandlerRunner.run();
                operationHandlerRunner.cleanup();
                uncompletedHandlers.decrementAndGet();
                operationHandlerRunner = operationHandlerRunnerQueueEventFetcher.fetchNextEvent();
            }
        } catch (Exception e) {
            errorReporter.reportError(
                    this,
                    String.format("Encountered unexpected exception\n%s", ConcurrentErrorReporter.stackTraceToString(e)));
        }
    }

    void forceShutdown() {
        forcedShutdownRequested.set(true);
    }
}
