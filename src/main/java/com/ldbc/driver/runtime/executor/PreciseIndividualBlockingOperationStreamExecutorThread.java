package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.runtime.scheduling.SpinnerCheck;

import java.util.Iterator;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

class PreciseIndividualBlockingOperationStreamExecutorThread extends Thread {
    private final OperationHandlerExecutor operationHandlerExecutor;
    private final Spinner slightlyEarlySpinner;
    private final ConcurrentErrorReporter errorReporter;
    private final ConcurrentCompletionTimeService completionTimeService;
    private final Iterator<OperationHandler<?>> handlers;
    private final AtomicBoolean hasFinished;

    public PreciseIndividualBlockingOperationStreamExecutorThread(OperationHandlerExecutor operationHandlerExecutor,
                                                                  ConcurrentErrorReporter errorReporter,
                                                                  ConcurrentCompletionTimeService completionTimeService,
                                                                  Iterator<OperationHandler<?>> handlers,
                                                                  AtomicBoolean hasFinished,
                                                                  Spinner slightlyEarlySpinner) {
        this.operationHandlerExecutor = operationHandlerExecutor;
        this.slightlyEarlySpinner = slightlyEarlySpinner;
        this.errorReporter = errorReporter;
        this.completionTimeService = completionTimeService;
        this.handlers = handlers;
        this.hasFinished = hasFinished;
    }

    @Override
    public void run() {
        Future<OperationResult> executingHandler = null;
        while (handlers.hasNext()) {
            OperationHandler<?> handler = handlers.next();
            handler.addCheck(new FutureCompletedCheck(executingHandler));

            // Schedule slightly early to account for context switch - internally, handler will schedule at exact start time
            // Ensure previously executed handler has completed
            slightlyEarlySpinner.waitForScheduledStartTime(handler.operation());

            try {
                completionTimeService.submitInitiatedTime(handler.operation().scheduledStartTime());
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

        this.hasFinished.set(true);
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
