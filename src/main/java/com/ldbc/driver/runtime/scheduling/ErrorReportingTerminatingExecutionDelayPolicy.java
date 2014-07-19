package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;

public class ErrorReportingTerminatingExecutionDelayPolicy implements ExecutionDelayPolicy {
    private final TimeSource TIME_SOURCE;
    private final Duration toleratedDelay;
    private final ConcurrentErrorReporter errorReporter;

    public ErrorReportingTerminatingExecutionDelayPolicy(TimeSource timeSource,
                                                         Duration toleratedDelay,
                                                         ConcurrentErrorReporter errorReporter) {
        this.TIME_SOURCE = timeSource;
        this.toleratedDelay = toleratedDelay;
        this.errorReporter = errorReporter;
    }

    @Override
    public boolean handleUnassignedScheduledStartTime(Operation<?> operation) {
        String errMsg = String.format("Operation has no Scheduled Start Time\n%s",
                operation.toString());
        errorReporter.reportError(this, errMsg);
        return false;
    }

    @Override
    public Duration toleratedDelay() {
        return toleratedDelay;
    }

    @Override
    public boolean handleExcessiveDelay(Operation<?> operation) {
        // Note, that this time is ever so slightly later than when the error actually occurred
        Time now = TIME_SOURCE.now();
        String errMsg = String.format("Operation start time delayed excessively\n"
                        + "  Operation: %s\n"
                        + "  Tolerated Delay: %s\n"
                        + "  Delay Now: %s\n"
                        + "  Scheduled Start Time: %s\n"
                        + "  Time Now: %s\n"
                ,
                toleratedDelay,
                now.durationGreaterThan(operation.scheduledStartTime()),
                operation,
                operation.scheduledStartTime(),
                now
        );
        errorReporter.reportError(this, errMsg);
        return false;
    }
}
