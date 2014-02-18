package com.ldbc.driver.runner;

import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

// TODO support offset
public class Spinner {
    // Duration that operation will be executed before scheduled start time
    // if offset==0 operation will be scheduled at exactly operation.scheduledStartTime()
    private final Duration offset;
    private final OperationSchedulingPolicy operationSchedulingPolicy;

    public Spinner(OperationSchedulingPolicy lateOperationPolicy) {
        this(lateOperationPolicy, Duration.fromMilli(0));
    }

    public Spinner(OperationSchedulingPolicy lateOperationPolicy, Duration offset) {
        this.operationSchedulingPolicy = lateOperationPolicy;
        this.offset = offset;
    }

    public void waitForScheduledStartTime(Operation<?> operation) {
        if (operationSchedulingPolicy.ignoreScheduledStartTime()) {
            return;
        }

        if (operation.scheduledStartTime().equals(Operation.UNASSIGNED_SCHEDULED_START_TIME)) {
            operationSchedulingPolicy.handleUnassignedScheduledStartTime();
        }

        long timeNowMilli = Time.nowAsMilli();
        long scheduledStartTimeWithOffsetMs = operation.scheduledStartTime().minus(offset).asMilli();
        long toleratedDelayMilli = operationSchedulingPolicy.toleratedDelay().asMilli();
        // changed to the below to avoid unnecessary Time & Duration object instantiations
        // if (Time.now().greaterBy(operation.scheduledStartTime()).greatThan(operationSchedulingPolicy.toleratedDelay())) {
        if (timeNowMilli - scheduledStartTimeWithOffsetMs > toleratedDelayMilli) {
            operationSchedulingPolicy.handleExcessiveDelay(operation);
        }

        // Time.nowAsMilli() to avoid object creation where possible
        while (Time.nowAsMilli() < scheduledStartTimeWithOffsetMs) {
            // loop/wait until operation scheduled start time
        }
    }
}
