package com.ldbc.driver.runner;

import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationException;
import com.ldbc.driver.temporal.Duration;

public interface OperationSchedulingPolicy
{
    /**
     * If true, operations will be scheduled for execution as quickly as
     * possible. This does not mean operations will be executed immediately;
     * operations will be executed as soon as the load generating machine has
     * the resources to do so.
     * 
     * @return true, if scheduled operation start time should be ignored.
     *         Otherwise, false.
     */
    public boolean ignoreScheduledStartTime();

    /**
     * Only called if ignoreScheduledStartTime is false.
     */
    public void handleUnassignedScheduledStartTime() throws OperationException;

    /**
     * Amount of time after its scheduled start time than an operation may be
     * scheduled for execution. Useful for ensuring load generating machine is
     * capable of generating the target load.
     */
    public Duration toleratedDelay();

    /**
     * Only called if ignoreScheduledStartTime is false and toleratedDelay is
     * exceeded.
     */
    public void handleExcessiveDelay( Operation<?> operation ) throws OperationException;

}
