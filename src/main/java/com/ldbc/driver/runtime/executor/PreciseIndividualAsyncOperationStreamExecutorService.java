package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.TimeSource;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

// TODO test
public class PreciseIndividualAsyncOperationStreamExecutorService {
    private static final Duration SHUTDOWN_WAIT_TIMEOUT = Duration.fromSeconds(5);

    private final TimeSource TIME_SOURCE;
    private final PreciseIndividualAsyncOperationStreamExecutorThread preciseIndividualAsyncOperationStreamExecutorThread;
    private final AtomicBoolean hasFinished = new AtomicBoolean(false);
    private final ConcurrentErrorReporter errorReporter;
    private boolean executing = false;
    private boolean shuttingDown = false;

    public PreciseIndividualAsyncOperationStreamExecutorService(TimeSource timeSource,
                                                                ConcurrentErrorReporter errorReporter,
                                                                Iterator<OperationHandler<?>> handlers,
                                                                Spinner slightlyEarlySpinner,
                                                                OperationHandlerExecutor operationHandlerExecutor) {
        this.TIME_SOURCE = timeSource;
        this.errorReporter = errorReporter;
        this.preciseIndividualAsyncOperationStreamExecutorThread = new PreciseIndividualAsyncOperationStreamExecutorThread(
                TIME_SOURCE,
                operationHandlerExecutor,
                errorReporter,
                handlers,
                hasFinished,
                slightlyEarlySpinner);
    }

    synchronized public AtomicBoolean execute() {
        if (executing)
            return hasFinished;
        executing = true;
        preciseIndividualAsyncOperationStreamExecutorThread.start();
        return hasFinished;
    }

    synchronized public void shutdown() {
        if (shuttingDown)
            return;
        shuttingDown = true;
        try {
            preciseIndividualAsyncOperationStreamExecutorThread.interrupt();
            preciseIndividualAsyncOperationStreamExecutorThread.join(SHUTDOWN_WAIT_TIMEOUT.asMilli());
        } catch (Exception e) {
            String errMsg = String.format("Unexpected error encountered while shutting down thread\n%s",
                    ConcurrentErrorReporter.stackTraceToString(e));
            errorReporter.reportError(this, errMsg);
        }
    }
}
