package com.ldbc.driver.runner;

import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationException;
import com.ldbc.driver.util.temporal.Duration;
import org.apache.log4j.Logger;

public class BasicOperationSchedulingPolicy implements OperationSchedulingPolicy {
    private static Logger logger = Logger.getLogger(BasicOperationSchedulingPolicy.class);

    private final Duration toleratedDelay;
    private final boolean ignoreScheduledStartTime;

    public BasicOperationSchedulingPolicy(Duration toleratedDelay, boolean ignoreScheduleStartTime) {
        this.toleratedDelay = toleratedDelay;
        this.ignoreScheduledStartTime = ignoreScheduleStartTime;
    }

    @Override
    public boolean ignoreScheduledStartTime() {
        return ignoreScheduledStartTime;
    }

    @Override
    public void handleUnassignedScheduledStartTime() throws OperationException {
        String errMsg = String.format("Operation must have an assigned Scheduled Start Time");
        logger.error(errMsg);
        throw new OperationException(errMsg);
    }

    @Override
    public Duration toleratedDelay() {
        return toleratedDelay;
    }

    @Override
    public void handleExcessiveDelay(Operation<?> operation) throws OperationException {
        String errMsg = String.format("Tolerated scheduled start time delay [%s] exceeded on operation:\n\t%s",
                toleratedDelay, operation);
        logger.error(errMsg);
        throw new OperationException(errMsg);
    }
}
