package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.temporal.Duration;

public class GctCheck implements SpinnerCheck {
    private final ConcurrentCompletionTimeService completionTimeService;
    private final Duration gctDeltaDuration;
    private final Operation<?> operation;
    private final ConcurrentErrorReporter errorReporter;

    public GctCheck(ConcurrentCompletionTimeService completionTimeService,
                    Duration gctDeltaDuration,
                    Operation<?> operation,
                    ConcurrentErrorReporter errorReporter) {
        this.completionTimeService = completionTimeService;
        this.gctDeltaDuration = gctDeltaDuration;
        this.operation = operation;
        this.errorReporter = errorReporter;
    }

    @Override
    public boolean doCheck() {
        try {
            return completionTimeService.globalCompletionTime().plus(gctDeltaDuration).gt(operation.scheduledStartTime());
        } catch (CompletionTimeException e) {
            errorReporter.reportError(this,
                    String.format(
                            "Error encountered while reading/writing GCT for query %s\n%s",
                            operation.getClass().getSimpleName(),
                            ConcurrentErrorReporter.stackTraceToString(e)));
            return false;
        }
    }

    @Override
    public boolean handleFailedCheck(Operation<?> operation) {
        try {
            errorReporter.reportError(this,
                    String.format("GCT(%s) has not advanced sufficiently to execute operation(%s)\n%s",
                            completionTimeService.globalCompletionTime().toString(),
                            operation.scheduledStartTime(),
                            operation.toString()));
            return false;
        } catch (CompletionTimeException e) {
            errorReporter.reportError(this,
                    String.format(
                            "Error encountered in handleFailedCheck while reading GCT for query %s\n%s",
                            operation.getClass().getSimpleName(),
                            ConcurrentErrorReporter.stackTraceToString(e)));
            return false;
        }
    }
}
