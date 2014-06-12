package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

public class Spinner {
    private static final SpinnerCheck TRUE_CHECK = new TrueCheck();
    // Duration that operation will be executed before scheduled start time
    // if offset==0 operation will be scheduled at exactly operation.scheduledStartTime()
    private final Duration offset;
    private final ExecutionDelayPolicy executionDelayPolicy;
    private final long toleratedDelayAccountingForOffsetAsMilli;


    public Spinner(ExecutionDelayPolicy lateOperationPolicy) {
        this(lateOperationPolicy, Duration.fromMilli(0));
    }

    public Spinner(ExecutionDelayPolicy lateOperationPolicy, Duration offset) {
        this.executionDelayPolicy = lateOperationPolicy;
        this.offset = offset;
        // tolerated delay only applies to actual scheduled start time.
        // offset will move the scheduled start time earlier, but execution "deadline" will still be: (original, before offset applied) scheduled start time + tolerated delay
        this.toleratedDelayAccountingForOffsetAsMilli = executionDelayPolicy.toleratedDelay().plus(offset).asMilli();
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
        if (Time.nowAsMilli() - scheduledStartTimeWithOffsetMs > toleratedDelayAccountingForOffsetAsMilli) {
            executionDelayPolicy.handleExcessiveDelay(operation);
        }

        // perform check at least once, in case time is ready (and within acceptable delay) on first loop
        boolean checkHasNotPassed = false == check.doCheck();
        while (Time.nowAsMilli() < scheduledStartTimeWithOffsetMs) {
            // loop/wait until operation scheduled start time
            if (checkHasNotPassed && check.doCheck())
                checkHasNotPassed = false;
        }

        if (checkHasNotPassed) {
            // TODO keep spinning for tolerated delay before reporting error? (e.g. to wait for GCT)
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
