package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.TimeSource;

public class Spinner {
    public static final Duration DEFAULT_SLEEP_DURATION_10_MILLI = Duration.fromMilli(10);
    public static final Duration DEFAULT_OFFSET_DURATION_0_MILLI = Duration.fromMilli(0);
    private static final SpinnerCheck TRUE_CHECK = new TrueCheck();

    private final TimeSource TIME_SOURCE;
    // Duration that operation will be executed before scheduled start time
    // if offset==0 operation will be scheduled at exactly operation.scheduledStartTime()
    private final Duration offset;
    private final ExecutionDelayPolicy executionDelayPolicy;
    private final long toleratedDelayAccountingForOffsetAsMilli;
    private final long sleepDurationAsMilli;

    public Spinner(TimeSource timeSource, Duration sleepDuration, ExecutionDelayPolicy lateOperationPolicy) {
        this(timeSource, sleepDuration, lateOperationPolicy, DEFAULT_OFFSET_DURATION_0_MILLI);
    }

    public Spinner(TimeSource timeSource, Duration sleepDuration, ExecutionDelayPolicy lateOperationPolicy, Duration offset) {
        this.TIME_SOURCE = timeSource;
        this.sleepDurationAsMilli = sleepDuration.asMilli();
        this.executionDelayPolicy = lateOperationPolicy;
        this.offset = offset;
        // tolerated delay only applies to actual scheduled start time
        // offset will move the scheduled start time earlier, but execution "deadline" will still be:
        //      "deadline" (i.e., latest allowed start time) = (original, i.e.,å before offset applied) scheduled start time + tolerated delay
        this.toleratedDelayAccountingForOffsetAsMilli = executionDelayPolicy.toleratedDelay().plus(offset).asMilli();
    }

    public boolean waitForScheduledStartTime(Operation<?> operation) {
        return waitForScheduledStartTime(operation, TRUE_CHECK);
    }

    /**
     * waits for the scheduled start time of an operation, and returns boolean value indicating if operation should be
     * executed or not.
     * true  = operation may still be executed
     * false = operation should not be scheduled
     * return value calculated follows:
     * true && handleUnassignedScheduledStartTime && handleExcessiveDelay && handleFailedCheck
     * i.e., if error occurs it (1) has ability to cancel operation execution (2) can do anything else in its handler
     *
     * @param operation
     * @param check
     * @return operation may be executed
     */
    public boolean waitForScheduledStartTime(Operation<?> operation, SpinnerCheck check) {
        boolean operationMayBeExecuted = true;
        // check that a scheduled start time has been assigned to the operation
        if (null == operation.scheduledStartTime()) {
            return executionDelayPolicy.handleUnassignedScheduledStartTime(operation);
        }

        // earliest time at which operation may start
        long scheduledStartTimeWithOffsetAsMilli = operation.scheduledStartTime().minus(offset).asMilli();
        // latest tolerated time at which operation may start, after this time operation is considered late
        long latestAllowableStartTimeAsMilli = scheduledStartTimeWithOffsetAsMilli + toleratedDelayAccountingForOffsetAsMilli;
        // TOO EARLY = <---(now)--(scheduled)[<---delay--->]------> <=(Time Line)
        // GOOD      = <-----(scheduled)[<-(now)--delay--->]------> <=(Time Line)
        // TOO LATE  = <-----(scheduled)[<---delay--->]--(now)----> <=(Time Line)

        // wait for checks to have all passed before allowing operation to start
        while (operationMayBeExecuted && false == check.doCheck()) {
            // give up if checks did not pass before latest tolerated operation start time was exceeded
            if (TIME_SOURCE.nowAsMilli() > latestAllowableStartTimeAsMilli) {
                boolean failedCheckResult = check.handleFailedCheck(operation);
                boolean executionDelayResult = executionDelayPolicy.handleExcessiveDelay(operation);
                operationMayBeExecuted = operationMayBeExecuted && failedCheckResult && executionDelayResult;
                break;
            }
            sleep();
        }

        // wait for scheduled operation start time
        while (TIME_SOURCE.nowAsMilli() < scheduledStartTimeWithOffsetAsMilli) {
            sleep();
        }

        // check that excessive delay has not already occurred
        if (TIME_SOURCE.nowAsMilli() > latestAllowableStartTimeAsMilli) {
            boolean executionDelayResult = executionDelayPolicy.handleExcessiveDelay(operation);
            operationMayBeExecuted = operationMayBeExecuted && executionDelayResult;
        }

        return operationMayBeExecuted;
    }

    // sleep to reduce CPU load while spinning
    // NOTE: longer sleep == lower scheduling accuracy
    private void sleep() {
        if (0 == sleepDurationAsMilli) return;
        try {
            Thread.sleep(sleepDurationAsMilli);
        } catch (InterruptedException e) {
        }
    }

    private static class TrueCheck implements SpinnerCheck {
        @Override
        public boolean doCheck() {
            return true;
        }

        @Override
        public boolean handleFailedCheck(Operation<?> operation) {
            return true;
        }
    }
}
