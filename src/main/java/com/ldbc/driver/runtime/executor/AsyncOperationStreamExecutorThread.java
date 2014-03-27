package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.runtime.Spinner;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.error.ConcurrentErrorReporter;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

class AsyncOperationStreamExecutorThread extends Thread {
    private final OperationHandlerExecutor operationHandlerExecutor;
    private final Spinner slightlyEarlySpinner;
    private final ConcurrentErrorReporter errorReporter;
    private final ConcurrentCompletionTimeService completionTimeService;
    private final Iterator<OperationHandler<?>> handlers;
    private final AtomicBoolean hasFinished;

    public AsyncOperationStreamExecutorThread(OperationHandlerExecutor operationHandlerExecutor,
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
        // retrieve and execute windows of handlers, one window at a time
        while (handlers.hasNext()) {
            OperationHandler<?> handler = handlers.next();
            // Schedule slightly early to account for context switch latency
            // Internally, OperationHandler will schedule at exact scheduled start time
            slightlyEarlySpinner.waitForScheduledStartTime(handler.operation());
            try {
                completionTimeService.submitInitiatedTime(handler.operation().scheduledStartTime());
            } catch (CompletionTimeException e) {
                String errMsg = String.format("Error encountered while submitted Initiated Time for:\n\t%s\n%s",
                        handler.operation().toString(),
                        ConcurrentErrorReporter.stackTraceToString(e.getCause()));
                errorReporter.reportError(this, errMsg);
            }
            try {
                operationHandlerExecutor.execute(handler);
            } catch (OperationHandlerExecutorException e) {
                String errMsg = String.format("Error encountered while submitting operation for execution\n\t%s\n\t%s",
                        handler.operation().toString(),
                        ConcurrentErrorReporter.stackTraceToString(e.getCause()));
                errorReporter.reportError(this, errMsg);
            }
        }

        this.hasFinished.set(true);
    }
}
