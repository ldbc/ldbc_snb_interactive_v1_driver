package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.Duration;

public class ErrorReportingExecutionDelayPolicy implements ExecutionDelayPolicy {
    private final Duration toleratedDelay;
    private final ConcurrentErrorReporter concurrentErrorReporter;

    public ErrorReportingExecutionDelayPolicy(Duration toleratedDelay,
                                              ConcurrentErrorReporter concurrentErrorReporter) {
        this.toleratedDelay = toleratedDelay;
        this.concurrentErrorReporter = concurrentErrorReporter;
    }

    @Override
    public void handleUnassignedScheduledStartTime(Operation<?> operation) {
        String errMsg = String.format("Operation has no Scheduled Start Time\n%s",
                operation.toString());
        concurrentErrorReporter.reportError(this, errMsg);
    }

    @Override
    public Duration toleratedDelay() {
        return toleratedDelay;
    }

    @Override
    public void handleExcessiveDelay(Operation<?> operation) {
        String errMsg = String.format("Tolerated scheduled start time delay [%s] exceeded on operation:\n\t%s",
                toleratedDelay, operation);
        concurrentErrorReporter.reportError(this, errMsg);
    }
}
