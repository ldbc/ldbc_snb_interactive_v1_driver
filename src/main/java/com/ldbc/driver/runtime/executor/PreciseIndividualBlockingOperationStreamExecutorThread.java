package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.runtime.scheduling.SpinnerCheck;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.TimeSource;

import java.util.Iterator;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

class PreciseIndividualBlockingOperationStreamExecutorThread extends Thread {
    // TODO this value should be configurable, or an entirely better policy should be used
    private static final Duration DURATION_TO_WAIT_FOR_LAST_HANDLER_TO_FINISH = Duration.fromMinutes(30);
    private static final Duration POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH = Duration.fromMilli(100);

    private final TimeSource TIME_SOURCE;
    private final OperationHandlerExecutor operationHandlerExecutor;
    private final Spinner slightlyEarlySpinner;
    private final ConcurrentErrorReporter errorReporter;
    private final Iterator<OperationHandler<?>> handlers;
    private final AtomicBoolean hasFinished;
    private final AtomicBoolean forcedTerminate;

    public PreciseIndividualBlockingOperationStreamExecutorThread(TimeSource timeSource,
                                                                  OperationHandlerExecutor operationHandlerExecutor,
                                                                  ConcurrentErrorReporter errorReporter,
                                                                  Iterator<OperationHandler<?>> handlers,
                                                                  AtomicBoolean hasFinished,
                                                                  Spinner slightlyEarlySpinner,
                                                                  AtomicBoolean forcedTerminate) {
        super(PreciseIndividualBlockingOperationStreamExecutorThread.class.getSimpleName() + "-" + System.currentTimeMillis());
        this.TIME_SOURCE = timeSource;
        this.operationHandlerExecutor = operationHandlerExecutor;
        this.slightlyEarlySpinner = slightlyEarlySpinner;
        this.errorReporter = errorReporter;
        this.handlers = handlers;
        this.hasFinished = hasFinished;
        this.forcedTerminate = forcedTerminate;
    }

    @Override
    public void run() {
        Future<OperationResultReport> executingHandlerFuture = null;
        Operation<?> previousOperation = null;
        while (handlers.hasNext() && false == forcedTerminate.get()) {
            OperationHandler<?> handler = handlers.next();

            // Ensures previously executed handler has completed before handler starts executing
            if (null != executingHandlerFuture) {
                handler.addCheck(new FutureCompletedCheck(executingHandlerFuture, previousOperation));
            }

            // submit initiated time as soon as possible so GCT/dependencies can advance as soon as possible
            submitInitiatedTime(handler);

            // Schedule slightly early to account for context switch - internally, handler will schedule at exact start time
            // TODO forcedTerminate does not cover all cases at present this spin loop is still blocking -> inject a check that throws exception?
            // TODO or SpinnerChecks have three possible results? (TRUE, NOT_TRUE_YET, FALSE)
            // TODO and/or Spinner has an emergency terminate button?
            slightlyEarlySpinner.waitForScheduledStartTime(handler.operation());

            executingHandlerFuture = executeHandler(handler);

            previousOperation = handler.operation();
        }
        // Wait for final operation handler
        boolean executingHandlerFinishedInTime = awaitExecutingHandler(DURATION_TO_WAIT_FOR_LAST_HANDLER_TO_FINISH, executingHandlerFuture);
        if (false == executingHandlerFinishedInTime) {
            errorReporter.reportError(this, "Last handler did not complete in time");
        }
        this.hasFinished.set(true);
    }

    private void submitInitiatedTime(OperationHandler<?> handler) {
        try {
            handler.localCompletionTimeWriter().submitLocalInitiatedTime(handler.operation().scheduledStartTime());
        } catch (CompletionTimeException e) {
            errorReporter.reportError(this,
                    String.format("Error encountered while submitted Initiated Time for:\n\t%s\n%s",
                            handler.operation().toString(),
                            ConcurrentErrorReporter.stackTraceToString(e)));
        }
    }

    private Future<OperationResultReport> executeHandler(OperationHandler<?> handler) {
        try {
            return operationHandlerExecutor.execute(handler);
        } catch (OperationHandlerExecutorException e) {
            errorReporter.reportError(this,
                    String.format("Error encountered while submitting operation for execution\n\t%s\n\t%s",
                            handler.operation().toString(),
                            ConcurrentErrorReporter.stackTraceToString(e)));
        }
        return null;
    }

    private boolean awaitExecutingHandler(Duration timeoutDuration, Future<OperationResultReport> executingHandler) {
        long pollInterval = POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH.asMilli();
        long timeoutTimeMs = TIME_SOURCE.now().plus(timeoutDuration).asMilli();
        while (TIME_SOURCE.nowAsMilli() < timeoutTimeMs) {
            if (null == executingHandler || executingHandler.isDone()) return true;
            if (forcedTerminate.get()) return true;
            Spinner.powerNap(pollInterval);
        }
        return false;
    }

    private final class FutureCompletedCheck implements SpinnerCheck {
        private final Future<?> future;
        private final Operation<?> previousOperation;

        private FutureCompletedCheck(Future<?> future, Operation<?> previousOperation) {
            this.future = future;
            this.previousOperation = previousOperation;
        }

        @Override
        public boolean doCheck() {
            return future.isDone();
        }

        @Override
        public boolean handleFailedCheck(Operation<?> operation) {
            System.out.println(previousOperation);
            String errMsg = String.format(
                    "Previous operation did not complete in time for next synchronous operation to start\n"
                            + " Previous Operation (%s): %s\n"
                            + " Next Operation (%s): %s",
                    previousOperation.scheduledStartTime(),
                    previousOperation,
                    operation.scheduledStartTime(),
                    operation
            );
            errorReporter.reportError(this, errMsg);
            return false;
        }
    }
}
