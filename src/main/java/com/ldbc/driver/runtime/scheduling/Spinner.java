package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

import java.util.List;

public class Spinner {
    private static final SpinnerCheck TRUE_CHECK = new TrueCheck();
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
        waitForScheduledStartTime(operation, TRUE_CHECK);
    }

    public void waitForScheduledStartTime(Operation<?> operation, SpinnerCheck check) {
        if (null == operation.scheduledStartTime()) {
            executionDelayPolicy.handleUnassignedScheduledStartTime(operation);
            return;
        }

        long scheduledStartTimeWithOffsetMs = operation.scheduledStartTime().minus(offset).asMilli();
        long toleratedDelayMs = executionDelayPolicy.toleratedDelay().asMilli();
        if (Time.nowAsMilli() - scheduledStartTimeWithOffsetMs > toleratedDelayMs) {
            executionDelayPolicy.handleExcessiveDelay(operation);
        }

        boolean checkHasNotPassed = true;
        while (Time.nowAsMilli() < scheduledStartTimeWithOffsetMs) {
            // loop/wait until operation scheduled start time
            if (checkHasNotPassed && check.doCheck())
                checkHasNotPassed = false;
        }

        if (checkHasNotPassed) {
            check.handleFailedCheck(operation);
        }
    }

    private static class TrueCheck implements SpinnerCheck {
        @Override
        public Boolean doCheck() {
            return true;
        }

        @Override
        public void handleFailedCheck(Operation<?> operation) {
        }
    }
}
