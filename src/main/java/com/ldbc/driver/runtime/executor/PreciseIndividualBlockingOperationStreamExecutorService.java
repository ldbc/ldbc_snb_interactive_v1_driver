package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.TimeSource;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

// TODO test
public class PreciseIndividualBlockingOperationStreamExecutorService {
    private static final Duration SHUTDOWN_WAIT_TIMEOUT = Duration.fromSeconds(5);
    private final PreciseIndividualBlockingOperationStreamExecutorThread preciseIndividualBlockingOperationStreamExecutorThread;
    private final AtomicBoolean hasFinished = new AtomicBoolean(false);
    private final ConcurrentErrorReporter errorReporter;
    private boolean executing = false;
    private boolean shuttingDown = false;

    public PreciseIndividualBlockingOperationStreamExecutorService(TimeSource timeSource,
                                                                   ConcurrentErrorReporter errorReporter,
                                                                   Iterator<OperationHandler<?>> handlers,
                                                                   Spinner slightlyEarlySpinner,
                                                                   OperationHandlerExecutor operationHandlerExecutor) {
        this.errorReporter = errorReporter;
        if (handlers.hasNext()) {
            this.preciseIndividualBlockingOperationStreamExecutorThread = new PreciseIndividualBlockingOperationStreamExecutorThread(
                    timeSource,
                    operationHandlerExecutor,
                    errorReporter,
                    handlers,
                    hasFinished,
                    slightlyEarlySpinner);
            this.preciseIndividualBlockingOperationStreamExecutorThread.setDaemon(true);
        } else {
            this.preciseIndividualBlockingOperationStreamExecutorThread = null;
            hasFinished.set(true);
            executing = true;
            shuttingDown = true;
        }
    }

    synchronized public AtomicBoolean execute() {
        if (executing)
            return hasFinished;
        executing = true;
        preciseIndividualBlockingOperationStreamExecutorThread.start();
        return hasFinished;
    }

    synchronized public void shutdown() {
        if (shuttingDown)
            return;
        shuttingDown = true;
        try {
            preciseIndividualBlockingOperationStreamExecutorThread.interrupt();
            preciseIndividualBlockingOperationStreamExecutorThread.join(SHUTDOWN_WAIT_TIMEOUT.asMilli());
        } catch (Exception e) {
            String errMsg = String.format("Unexpected error encountered while shutting down thread\n%s",
                    ConcurrentErrorReporter.stackTraceToString(e));
            errorReporter.reportError(this, errMsg);
        }
    }
}
