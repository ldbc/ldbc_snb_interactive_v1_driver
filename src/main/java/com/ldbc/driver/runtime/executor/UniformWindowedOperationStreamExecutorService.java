package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

// TODO test
public class UniformWindowedOperationStreamExecutorService {
    private static final Duration SHUTDOWN_WAIT_TIMEOUT = Duration.fromSeconds(5);

    private final UniformWindowedOperationStreamExecutorThread uniformWindowedOperationStreamExecutorThread;
    private final AtomicBoolean hasFinished = new AtomicBoolean(false);
    private final ConcurrentErrorReporter errorReporter;
    private boolean executing = false;
    private boolean shuttingDown = false;

    public UniformWindowedOperationStreamExecutorService(TimeSource timeSource,
                                                         ConcurrentErrorReporter errorReporter,
                                                         Iterator<OperationHandler<?>> handlers,
                                                         OperationHandlerExecutor operationHandlerExecutor,
                                                         Spinner slightlyEarlySpinner,
                                                         Time firstWindowStartTime,
                                                         Duration windowSize) {
        this.errorReporter = errorReporter;
        if (handlers.hasNext()) {
            this.uniformWindowedOperationStreamExecutorThread = new UniformWindowedOperationStreamExecutorThread(
                    timeSource,
                    firstWindowStartTime,
                    windowSize,
                    operationHandlerExecutor,
                    errorReporter,
                    handlers,
                    hasFinished,
                    slightlyEarlySpinner);
            this.uniformWindowedOperationStreamExecutorThread.setDaemon(true);
        } else {
            this.uniformWindowedOperationStreamExecutorThread = null;
            executing = true;
            shuttingDown = true;
            hasFinished.set(true);
        }
    }

    synchronized public AtomicBoolean execute() {
        if (executing)
            return hasFinished;
        executing = true;
        uniformWindowedOperationStreamExecutorThread.start();
        return hasFinished;
    }

    synchronized public void shutdown() {
        if (shuttingDown)
            return;
        shuttingDown = true;
        try {
            uniformWindowedOperationStreamExecutorThread.interrupt();
            uniformWindowedOperationStreamExecutorThread.join(SHUTDOWN_WAIT_TIMEOUT.asMilli());
        } catch (Exception e) {
            String errMsg = String.format("Unexpected error encountered while shutting down thread\n%s",
                    ConcurrentErrorReporter.stackTraceToString(e));
            errorReporter.reportError(this, errMsg);
        }
    }
}
