package com.ldbc.driver.runner;

import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.Duration;
import org.apache.log4j.Logger;

public class FeedbackOperationSchedulingPolicy implements OperationSchedulingPolicy {
    private static Logger logger = Logger.getLogger(FeedbackOperationSchedulingPolicy.class);

    private final Duration toleratedDelay;
    private final boolean ignoreScheduledStartTime;
    private final ConcurrentErrorReporter concurrentErrorReporter;

    // TODO test
    public FeedbackOperationSchedulingPolicy(Duration toleratedDelay, boolean ignoreScheduleStartTime,
                                             ConcurrentErrorReporter concurrentErrorReporter) {
        this.toleratedDelay = toleratedDelay;
        this.ignoreScheduledStartTime = ignoreScheduleStartTime;
        this.concurrentErrorReporter = concurrentErrorReporter;
    }

    @Override
    public boolean ignoreScheduledStartTime() {
        return ignoreScheduledStartTime;
    }

    @Override
    public void handleUnassignedScheduledStartTime() {
        concurrentErrorReporter.reportError(this, "Operation must have an assigned Scheduled Start Time");
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
