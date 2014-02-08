package com.ldbc.driver.runner;

import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationException;
import com.ldbc.driver.util.temporal.Time;

public class Spinner {
    private final OperationSchedulingPolicy operationSchedulingPolicy;

    public Spinner(OperationSchedulingPolicy lateOperationPolicy) {
        this.operationSchedulingPolicy = lateOperationPolicy;
    }

    public void waitForScheduledStartTime(Operation<?> operation) throws OperationException {
        if (operationSchedulingPolicy.ignoreScheduledStartTime()) {
            return;
        }

        if (operation.scheduledStartTime().equals(Operation.UNASSIGNED_SCHEDULED_START_TIME)) {
            operationSchedulingPolicy.handleUnassignedScheduledStartTime();
        }

        // TODO Temporal util methods take milli to avoid Time object creation
        // TODO not urgent, only happens once per operation
        // TODO move to other place? real delay occurs at OperationHandler
        if (Time.now().greaterBy(operation.scheduledStartTime()
        ).greatThan(
                operationSchedulingPolicy.toleratedDelay())) {
            operationSchedulingPolicy.handleExcessiveDelay(operation);
        }

        // Time.nowAsMilli() to avoid object creation where possible
        while (Time.nowAsMilli() < operation.scheduledStartTime().asMilli()) {
            // loop/wait until operation scheduled start time
        }
    }
}
