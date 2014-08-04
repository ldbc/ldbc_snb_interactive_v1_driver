package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.Duration;

public interface ExecutionDelayPolicy {

    /**
     * Only called if scheduledStartTime is null or dependencyTime is null.
     * Return value dictates if operation may still be executed, or if execution should be aborted.
     *
     * @param operation
     * @return operation may still be executed
     */
    public boolean handleUnassignedTime(Operation<?> operation);

    /**
     * Amount of time after its scheduled start time than an operation may be
     * scheduled for execution. Useful for ensuring load generating machine is
     * capable of generating the target load.
     */
    public Duration toleratedDelay();

    /**
     * Only called if tolerated delay is exceeded.
     * Return value dictates if operation may still be executed, or if execution should be aborted.
     *
     * @param operation
     * @return operation may still be executed
     */
    public boolean handleExcessiveDelay(Operation<?> operation);

}
