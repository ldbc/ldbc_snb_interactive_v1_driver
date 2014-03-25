package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.error.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.Duration;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

// TODO test
public class AsyncOperationStreamExecutorService {
    private static final Duration SHUTDOWN_WAIT_TIMEOUT = Duration.fromSeconds(5);

    private final AsyncOperationStreamExecutorThread asyncOperationStreamExecutorThread;
    private final AtomicBoolean hasFinished = new AtomicBoolean(false);
    private final ConcurrentErrorReporter errorReporter;
    private boolean executing = false;
    private boolean shuttingDown = false;

    public AsyncOperationStreamExecutorService(ConcurrentErrorReporter errorReporter,
                                               ConcurrentCompletionTimeService completionTimeService,
                                               Iterator<OperationHandler<?>> handlers,
                                               AlwaysValidCompletionTimeValidator.Spinner slightlyEarlySpinner,
                                               OperationHandlerExecutor operationHandlerExecutor) {
        this.errorReporter = errorReporter;
        this.asyncOperationStreamExecutorThread = new AsyncOperationStreamExecutorThread(
                operationHandlerExecutor,
                errorReporter,
                completionTimeService,
                handlers,
                hasFinished,
                slightlyEarlySpinner);
    }

    synchronized public AtomicBoolean execute() {
        if (executing)
            return hasFinished;
        executing = true;
        asyncOperationStreamExecutorThread.start();
        return hasFinished;
    }

    synchronized public void shutdown() {
        if (shuttingDown)
            return;
        shuttingDown = true;
        try {
            asyncOperationStreamExecutorThread.interrupt();
            asyncOperationStreamExecutorThread.join(SHUTDOWN_WAIT_TIMEOUT.asMilli());
        } catch (Exception e) {
            String errMsg = String.format("Unexpected error encountered while shutting down thread\n%s",
                    ConcurrentErrorReporter.stackTraceToString(e.getCause()));
            errorReporter.reportError(this, errMsg);
        }
    }
}
