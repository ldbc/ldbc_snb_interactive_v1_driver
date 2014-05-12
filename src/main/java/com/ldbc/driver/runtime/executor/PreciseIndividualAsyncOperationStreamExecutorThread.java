package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

class PreciseIndividualAsyncOperationStreamExecutorThread extends Thread {
    // TODO this value should be configurable, or an entirely better policy should be used
    private static final Duration DURATION_TO_WAIT_FOR_ALL_HANDLERS_TO_FINISH = Duration.fromMinutes(5);

    private final OperationHandlerExecutor operationHandlerExecutor;
    private final Spinner slightlyEarlySpinner;
    private final ConcurrentErrorReporter errorReporter;
    private final Iterator<OperationHandler<?>> handlers;
    private final AtomicBoolean hasFinished;
    private final ArrayList<Future<OperationResult>> runningHandlers = new ArrayList<Future<OperationResult>>();

    public PreciseIndividualAsyncOperationStreamExecutorThread(OperationHandlerExecutor operationHandlerExecutor,
                                                               ConcurrentErrorReporter errorReporter,
                                                               Iterator<OperationHandler<?>> handlers,
                                                               AtomicBoolean hasFinished,
                                                               Spinner slightlyEarlySpinner) {
        super(PreciseIndividualAsyncOperationStreamExecutorThread.class.getSimpleName());
        this.operationHandlerExecutor = operationHandlerExecutor;
        this.slightlyEarlySpinner = slightlyEarlySpinner;
        this.errorReporter = errorReporter;
        this.handlers = handlers;
        this.hasFinished = hasFinished;
    }

    @Override
    public void run() {
        while (handlers.hasNext()) {
            OperationHandler<?> handler = null;
            try {
                handler = handlers.next();
            } catch (Exception e) {
                errorReporter.reportError(this, String.format("Error getting next handler\n%s", ConcurrentErrorReporter.stackTraceToString(e)));
                break;
            }

            // Schedule slightly early to account for context switch - internally, handler will schedule at exact start time
            slightlyEarlySpinner.waitForScheduledStartTime(handler.operation());
            try {
                handler.completionTimeService().submitInitiatedTime(handler.operation().scheduledStartTime());
            } catch (CompletionTimeException e) {
                String errMsg = String.format("Error encountered while submitted Initiated Time for:\n\t%s\n%s",
                        handler.operation().toString(),
                        ConcurrentErrorReporter.stackTraceToString(e));
                errorReporter.reportError(this, errMsg);
            }
            try {
                Future<OperationResult> runningHandler = operationHandlerExecutor.execute(handler);
                runningHandlers.add(runningHandler);
            } catch (OperationHandlerExecutorException e) {
                String errMsg = String.format("Error encountered while submitting operation for execution\n\t%s\n\t%s",
                        handler.operation().toString(),
                        ConcurrentErrorReporter.stackTraceToString(e));
                errorReporter.reportError(this, errMsg);
            }
        }
        boolean handlersFinishedInTime = awaitAllRunningHandlers(DURATION_TO_WAIT_FOR_ALL_HANDLERS_TO_FINISH);
        if (false == handlersFinishedInTime) {
            errorReporter.reportError(this, String.format("At least one operation handler did not complete in time"));
        }
        this.hasFinished.set(true);
    }

    // TODO possibly use similar logic to make it possible to cap maximum query run time
    private boolean awaitAllRunningHandlers(Duration timeoutDuration) {
        long timeoutTimeMs = Time.now().plus(timeoutDuration).asMilli();
        while (Time.nowAsMilli() < timeoutTimeMs) {
            if (allHandlersCompleted()) return true;
        }
        return false;
    }

    private boolean allHandlersCompleted() {
        for (Future<OperationResult> runningHandler : runningHandlers)
            if (false == runningHandler.isDone()) return false;
        return true;
    }
}
