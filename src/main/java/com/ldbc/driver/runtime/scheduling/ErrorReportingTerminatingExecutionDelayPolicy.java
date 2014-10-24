package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.temporal.TimeSource;

import java.util.concurrent.TimeUnit;

public class ErrorReportingTerminatingExecutionDelayPolicy implements ExecutionDelayPolicy {
    private final TemporalUtil temporalUtil = new TemporalUtil();
    private final TimeSource timeSource;
    private final long toleratedDelayAsMilli;
    private final ConcurrentErrorReporter errorReporter;

    public ErrorReportingTerminatingExecutionDelayPolicy(TimeSource timeSource,
                                                         long toleratedDelayAsMilli,
                                                         ConcurrentErrorReporter errorReporter) {
        this.timeSource = timeSource;
        this.toleratedDelayAsMilli = toleratedDelayAsMilli;
        this.errorReporter = errorReporter;
    }

    @Override
    public long toleratedDelayAsMilli() {
        return toleratedDelayAsMilli;
    }

    @Override
    public boolean handleExcessiveDelay(Operation<?> operation) {
        // Note, that this time is ever so slightly later than when the error actually occurred
        long nowAsMilli = timeSource.nowAsMilli();
        String errMsg = String.format("Operation start time delayed excessively\n"
                        + "  Operation: %s\n"
                        + "  Tolerated Delay: %s\n"
                        + "  Delayed So Far: %s\n"
                        + "  Scheduled Start Time: %s\n"
                        + "  Time Now: %s\n"
                ,
                operation,
                temporalUtil.nanoDurationToString(temporalUtil.convert(toleratedDelayAsMilli, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS)),
                temporalUtil.nanoDurationToString(temporalUtil.convert(nowAsMilli - operation.scheduledStartTimeAsMilli(), TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS)),
                temporalUtil.millisecondsToDateTimeString(operation.scheduledStartTimeAsMilli()),
                temporalUtil.millisecondsToDateTimeString(nowAsMilli)
        );
        errorReporter.reportError(this, errMsg);
        return false;
    }
}
