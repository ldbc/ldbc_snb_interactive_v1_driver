package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.runtime.scheduling.SpinnerCheck;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

import java.util.Iterator;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

class PreciseIndividualBlockingOperationStreamExecutorThread extends Thread {
    // TODO this value should be configurable, or an entirely better policy should be used
    private static final Duration DURATION_TO_WAIT_FOR_LAST_HANDLER_TO_FINISH = Duration.fromMinutes(5);

    private final OperationHandlerExecutor operationHandlerExecutor;
    private final Spinner slightlyEarlySpinner;
    private final ConcurrentErrorReporter errorReporter;
    private final Iterator<OperationHandler<?>> handlers;
    private final AtomicBoolean hasFinished;

    public PreciseIndividualBlockingOperationStreamExecutorThread(OperationHandlerExecutor operationHandlerExecutor,
                                                                  ConcurrentErrorReporter errorReporter,
                                                                  Iterator<OperationHandler<?>> handlers,
                                                                  AtomicBoolean hasFinished,
                                                                  Spinner slightlyEarlySpinner) {
        super(PreciseIndividualBlockingOperationStreamExecutorThread.class.getSimpleName());
        this.operationHandlerExecutor = operationHandlerExecutor;
        this.slightlyEarlySpinner = slightlyEarlySpinner;
        this.errorReporter = errorReporter;
        this.handlers = handlers;
        this.hasFinished = hasFinished;
    }

    @Override
    public void run() {
        Future<OperationResult> executingHandler = null;
        while (handlers.hasNext()) {
            OperationHandler<?> handler = handlers.next();
            // Ensures previously executed handler has completed before handler starts executing
            handler.addCheck(new FutureCompletedCheck(executingHandler));

            // Schedule slightly early to account for context switch - internally, handler will schedule at exact start time
            slightlyEarlySpinner.waitForScheduledStartTime(handler.operation());

            try {
                handler.completionTimeService().submitInitiatedTime(handler.operation().scheduledStartTime());
            } catch (CompletionTimeException e) {
                errorReporter.reportError(this,
                        String.format("Error encountered while submitted Initiated Time for:\n\t%s\n%s",
                                handler.operation().toString(),
                                ConcurrentErrorReporter.stackTraceToString(e.getCause())));
            }
            try {
                executingHandler = operationHandlerExecutor.execute(handler);
            } catch (OperationHandlerExecutorException e) {
                errorReporter.reportError(this,
                        String.format("Error encountered while submitting operation for execution\n\t%s\n\t%s",
                                handler.operation().toString(),
                                ConcurrentErrorReporter.stackTraceToString(e.getCause())));
            }
        }
        boolean executingHandlerFinishedInTime = awaitExecutingHandler(DURATION_TO_WAIT_FOR_LAST_HANDLER_TO_FINISH, executingHandler);
        if (false == executingHandlerFinishedInTime) {
            errorReporter.reportError(this, "Last handler did not complete in time");
        }
        this.hasFinished.set(true);
    }

    private boolean awaitExecutingHandler(Duration timeoutDuration, Future<OperationResult> executingHandler) {
        long timeoutTimeMs = Time.now().plus(timeoutDuration).asMilli();
        while (Time.nowAsMilli() < timeoutTimeMs) {
            if (null == executingHandler || executingHandler.isDone()) return true;
        }
        return false;
    }

    private final class FutureCompletedCheck implements SpinnerCheck {
        private final Future<?> future;

        private FutureCompletedCheck(Future<?> future) {
            this.future = future;
        }

        @Override
        public Boolean doCheck() {
            return future.isDone();
        }

        @Override
        public void handleFailedCheck(Operation<?> operation) {
            errorReporter.reportError(this, "Previous operation did not complete in time for next, synchronous operation to start");
        }
    }
}
