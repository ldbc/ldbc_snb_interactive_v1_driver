package com.ldbc.driver.runner;

import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.Duration;
import org.apache.log4j.Logger;

public class ErrorLoggingOperationSchedulingPolicy implements OperationSchedulingPolicy {
    private static Logger logger = Logger.getLogger(ErrorLoggingOperationSchedulingPolicy.class);

    private final Duration toleratedDelay;
    private final boolean ignoreScheduledStartTime;

    public ErrorLoggingOperationSchedulingPolicy(Duration toleratedDelay, boolean ignoreScheduleStartTime) {
        this.toleratedDelay = toleratedDelay;
        this.ignoreScheduledStartTime = ignoreScheduleStartTime;
    }

    @Override
    public boolean ignoreScheduledStartTime() {
        return ignoreScheduledStartTime;
    }

    @Override
    public void handleUnassignedScheduledStartTime() {
        String errMsg = String.format("%s\nOperation must have an assigned Scheduled Start Time", ConcurrentErrorReporter.whoAmI(this));
        logger.error(errMsg);
    }

    @Override
    public Duration toleratedDelay() {
        return toleratedDelay;
    }

    @Override
    public void handleExcessiveDelay(Operation<?> operation) {
        String errMsg = String.format("%s\nTolerated scheduled start time delay [%s] exceeded on operation:\n\t%s",
                ConcurrentErrorReporter.whoAmI(this), toleratedDelay, operation);
        logger.error(errMsg);
    }
}
