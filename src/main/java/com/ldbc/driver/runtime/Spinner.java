package com.ldbc.driver.runtime;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.error.ExecutionDelayPolicy;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

public class Spinner {
    // Duration that operation will be executed before scheduled start time
    // if offset==0 operation will be scheduled at exactly operation.scheduledStartTime()
    private final Duration offset;
    private final ExecutionDelayPolicy executionDelayPolicy;

    public Spinner(ExecutionDelayPolicy lateOperationPolicy) {
        this(lateOperationPolicy, Duration.fromMilli(0));
    }

    public Spinner(ExecutionDelayPolicy lateOperationPolicy, Duration offset) {
        this.executionDelayPolicy = lateOperationPolicy;
        this.offset = offset;
    }

    public void waitForScheduledStartTime(Operation<?> operation) {
        if (null == operation.scheduledStartTime()) {
            executionDelayPolicy.handleUnassignedScheduledStartTime(operation);
            return;
        }

        long timeNowMilli = Time.nowAsMilli();
        long scheduledStartTimeWithOffsetMs = operation.scheduledStartTime().minus(offset).asMilli();
        long toleratedDelayMilli = executionDelayPolicy.toleratedDelay().asMilli();
        // changed to the below to avoid unnecessary Time & Duration object instantiations
        // if (Time.now().greaterBy(operation.scheduledStartTime()).greatThan(executionDelayPolicy.toleratedDelay())) {
        if (timeNowMilli - scheduledStartTimeWithOffsetMs > toleratedDelayMilli) {
            executionDelayPolicy.handleExcessiveDelay(operation);
        }

        // Time.nowAsMilli() to avoid object creation where possible
        while (Time.nowAsMilli() < scheduledStartTimeWithOffsetMs) {
            // loop/wait until operation scheduled start time
        }
    }
}
