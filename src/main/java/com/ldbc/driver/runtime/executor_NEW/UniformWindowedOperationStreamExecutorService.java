package com.ldbc.driver.runtime.executor_NEW;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.error.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

// TODO test
public class UniformWindowedOperationStreamExecutorService {
    private static final Duration SHUTDOWN_WAIT_TIMEOUT = Duration.fromSeconds(5);

    private final UniformWindowedOperationStreamExecutorThread uniformWindowedOperationStreamExecutorThread;
    private final AtomicBoolean hasFinished = new AtomicBoolean(false);
    private final ConcurrentErrorReporter concurrentErrorReporter;
    private boolean executing = false;
    private boolean shuttingDown = false;

    public UniformWindowedOperationStreamExecutorService(final Time startTime,
                                                         final Duration windowSize,
                                                         int threadCount,
                                                         ConcurrentErrorReporter concurrentErrorReporter,
                                                         ConcurrentCompletionTimeService concurrentCompletionTimeService,
                                                         Iterator<OperationHandler<?>> handlers) {
        this.concurrentErrorReporter = concurrentErrorReporter;
        this.uniformWindowedOperationStreamExecutorThread = new UniformWindowedOperationStreamExecutorThread(
                startTime,
                windowSize,
                threadCount,
                concurrentErrorReporter,
                concurrentCompletionTimeService,
                handlers,
                hasFinished);
    }

    public AtomicBoolean execute() {
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
                    ConcurrentErrorReporter.stackTraceToString(e.getCause()));
            concurrentErrorReporter.reportError(this, errMsg);
        }
    }
}
