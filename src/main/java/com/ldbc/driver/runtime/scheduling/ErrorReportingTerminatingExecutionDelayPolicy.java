package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;

public class ErrorReportingTerminatingExecutionDelayPolicy implements ExecutionDelayPolicy {
    private final TimeSource timeSource;
    private final Duration toleratedDelay;
    private final ConcurrentErrorReporter errorReporter;

    public ErrorReportingTerminatingExecutionDelayPolicy(TimeSource timeSource,
                                                         Duration toleratedDelay,
                                                         ConcurrentErrorReporter errorReporter) {
        this.timeSource = timeSource;
        this.toleratedDelay = toleratedDelay;
        this.errorReporter = errorReporter;
    }

    @Override
    public Duration toleratedDelayAsMilli() {
        return toleratedDelay;
    }

    @Override
    public boolean handleExcessiveDelay(Operation<?> operation) {
        // Note, that this time is ever so slightly later than when the error actually occurred
        Time now = timeSource.now();
        String errMsg = String.format("Operation start time delayed excessively\n"
                        + "  Operation: %s\n"
                        + "  Tolerated Delay: %s\n"
                        + "  Delayed So Far: %s\n"
                        + "  Scheduled Start Time: %s\n"
                        + "  Time Now: %s\n"
                ,
                operation,
                toleratedDelay,
                now.durationGreaterThan(operation.scheduledStartTimeAsMilli()),
                operation.scheduledStartTimeAsMilli(),
                now
        );
        errorReporter.reportError(this, errMsg);
        return false;
    }
}
