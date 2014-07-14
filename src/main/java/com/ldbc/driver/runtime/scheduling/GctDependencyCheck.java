package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;

public class GctDependencyCheck implements SpinnerCheck {
    private final ConcurrentCompletionTimeService completionTimeService;
    private final Operation<?> operation;
    private final ConcurrentErrorReporter errorReporter;

    public GctDependencyCheck(ConcurrentCompletionTimeService completionTimeService,
                              Operation<?> operation,
                              ConcurrentErrorReporter errorReporter) {
        this.completionTimeService = completionTimeService;
        this.operation = operation;
        this.errorReporter = errorReporter;
    }

    @Override
    public boolean doCheck() {
        try {
            return completionTimeService.globalCompletionTime().gte(operation.dependencyTime());
        } catch (CompletionTimeException e) {
            errorReporter.reportError(this,
                    String.format(
                            "Error encountered while reading GCT for query %s\n%s",
                            operation.getClass().getSimpleName(),
                            ConcurrentErrorReporter.stackTraceToString(e)));
            return false;
        }
    }

    @Override
    public boolean handleFailedCheck(Operation<?> operation) {
        try {
            // TODO, not GCT printed here may be a little later than GCT that was measured during check
            errorReporter.reportError(this,
                    String.format("GCT(%s) has not advanced sufficiently to execute operation\n"
                                    + "Operation: %s\n"
                                    + "Scheduled Start Time: %s\n"
                                    + "Dependency Time: %s",
                            completionTimeService.globalCompletionTime().toString(),
                            operation.toString(),
                            operation.scheduledStartTime(),
                            operation.dependencyTime()));
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
