package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.QueueEventFetcher;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SingleThreadOperationHandlerExecutorThread extends Thread {
    private final QueueEventFetcher<OperationHandler<?>> operationHandlerQueueEventFetcher;
    private final ConcurrentErrorReporter errorReporter;
    private final AtomicLong uncompletedHandlers;
    private final AtomicBoolean forcedShutdownRequested = new AtomicBoolean(false);

    SingleThreadOperationHandlerExecutorThread(Queue<OperationHandler<?>> operationHandlerQueue,
                                               ConcurrentErrorReporter errorReporter,
                                               AtomicLong uncompletedHandlers) {
        super(SingleThreadOperationHandlerExecutorThread.class.getSimpleName() + "-" + System.currentTimeMillis());
        this.operationHandlerQueueEventFetcher = QueueEventFetcher.queueEventFetcherFor(operationHandlerQueue);
        this.errorReporter = errorReporter;
        this.uncompletedHandlers = uncompletedHandlers;
    }

    @Override
    public void run() {
        try {
            OperationHandler<?> operationHandler = operationHandlerQueueEventFetcher.fetchNextEvent();
            while (operationHandler != SingleThreadOperationHandlerExecutor.TERMINATE_HANDLER && false == forcedShutdownRequested.get()) {
                operationHandler.run();
                operationHandler.onComplete();
                operationHandler.cleanup();
                uncompletedHandlers.decrementAndGet();
                operationHandler = operationHandlerQueueEventFetcher.fetchNextEvent();
            }
        } catch (Exception e) {
            errorReporter.reportError(
                    this,
                    String.format("Encountered unexpected exception\n%s", ConcurrentErrorReporter.stackTraceToString(e)));
            return;
        }
    }

    void forceShutdown() {
        forcedShutdownRequested.set(true);
    }
}
