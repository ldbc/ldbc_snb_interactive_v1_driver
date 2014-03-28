package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.Duration;

public interface ExecutionDelayPolicy {

    /**
     * Only called if scheduledStartTime is null.
     */
    public void handleUnassignedScheduledStartTime(Operation<?> operation);

    /**
     * Amount of time after its scheduled start time than an operation may be
     * scheduled for execution. Useful for ensuring load generating machine is
     * capable of generating the target load.
     */
    public Duration toleratedDelay();

    /**
     * Only called if toleratedDelay is exceeded.
     */
    public void handleExcessiveDelay(Operation<?> operation);

}
