package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.error.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.error.ExecutionDelayPolicy;
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

    public UniformWindowedOperationStreamExecutorService(Time firstWindowStartTime,
                                                         Duration windowSize,
                                                         Duration gctDeltaTime,
                                                         ConcurrentErrorReporter concurrentErrorReporter,
                                                         ConcurrentCompletionTimeService concurrentCompletionTimeService,
                                                         Iterator<OperationHandler<?>> handlers,
                                                         ExecutionDelayPolicy delayPolicy,
                                                         OperationHandlerExecutor operationHandlerExecutor,
                                                         AlwaysValidCompletionTimeValidator.Spinner slightlyEarlySpinner) {
        this.concurrentErrorReporter = concurrentErrorReporter;
        CompletionTimeValidator deltaCompletionTimeValidator = new DeltaTimeCompletionTimeValidator(gctDeltaTime);
        this.uniformWindowedOperationStreamExecutorThread = new UniformWindowedOperationStreamExecutorThread(
                firstWindowStartTime,
                windowSize,
                deltaCompletionTimeValidator,
                operationHandlerExecutor,
                concurrentErrorReporter,
                concurrentCompletionTimeService,
                handlers,
                hasFinished,
                slightlyEarlySpinner);
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
                    ConcurrentErrorReporter.stackTraceToString(e.getCause()));
            concurrentErrorReporter.reportError(this, errMsg);
        }
    }
}
