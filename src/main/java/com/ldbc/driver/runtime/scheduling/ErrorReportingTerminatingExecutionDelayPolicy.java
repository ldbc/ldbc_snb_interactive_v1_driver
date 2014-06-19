package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;

public class ErrorReportingTerminatingExecutionDelayPolicy implements ExecutionDelayPolicy {
    private final TimeSource TIME_SOURCE;
    private final Duration toleratedDelay;
    private final ConcurrentErrorReporter concurrentErrorReporter;

    public ErrorReportingTerminatingExecutionDelayPolicy(TimeSource timeSource,
                                                         Duration toleratedDelay,
                                                         ConcurrentErrorReporter concurrentErrorReporter) {
        this.TIME_SOURCE = timeSource;
        this.toleratedDelay = toleratedDelay;
        this.concurrentErrorReporter = concurrentErrorReporter;
    }

    @Override
    public boolean handleUnassignedScheduledStartTime(Operation<?> operation) {
        String errMsg = String.format("Operation has no Scheduled Start Time\n%s",
                operation.toString());
        concurrentErrorReporter.reportError(this, errMsg);
        return false;
    }

    @Override
    public Duration toleratedDelay() {
        return toleratedDelay;
    }

    @Override
    public boolean handleExcessiveDelay(Operation<?> operation) {
        Time now = TIME_SOURCE.now();
        String errMsg = String.format("Tolerated start time delay [%s] exceeded by approximately [%s]\n\t%s",
                toleratedDelay, now.greaterBy(operation.scheduledStartTime()), operation);
        concurrentErrorReporter.reportError(this, errMsg);
        return false;
    }
}
