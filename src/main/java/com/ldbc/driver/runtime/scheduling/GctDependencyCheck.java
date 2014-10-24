package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.GlobalCompletionTimeReader;

public class GctDependencyCheck implements SpinnerCheck {
    private final GlobalCompletionTimeReader globalCompletionTimeReader;
    private final Operation<?> operation;
    private final ConcurrentErrorReporter errorReporter;

    public GctDependencyCheck(GlobalCompletionTimeReader globalCompletionTimeReader,
                              Operation<?> operation,
                              ConcurrentErrorReporter errorReporter) {
        this.globalCompletionTimeReader = globalCompletionTimeReader;
        this.operation = operation;
        this.errorReporter = errorReporter;
    }

    @Override
    public SpinnerCheckResult doCheck() {
        try {
            return (globalCompletionTimeReader.globalCompletionTime().gte(operation.dependencyTimeAsMilli())) ? SpinnerCheckResult.PASSED : SpinnerCheckResult.STILL_CHECKING;
        } catch (CompletionTimeException e) {
            errorReporter.reportError(this,
                    String.format(
                            "Error encountered while reading GCT for query %s\n%s",
                            operation.getClass().getSimpleName(),
                            ConcurrentErrorReporter.stackTraceToString(e)));
            return SpinnerCheckResult.FAILED;
        }
    }

    @Override
    public boolean handleFailedCheck(Operation<?> operation) {
        try {
            // Note, GCT printed here may be a little later than GCT that was measured during check
            errorReporter.reportError(this,
                    String.format("GCT(%s) has not advanced sufficiently to execute operation\n"
                                    + "Operation: %s\n"
                                    + "Scheduled Start Time: %s\n"
                                    + "Dependency Time: %s",
                            globalCompletionTimeReader.globalCompletionTime().toString(),
                            operation.toString(),
                            operation.scheduledStartTimeAsMilli(),
                            operation.dependencyTimeAsMilli()));
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
