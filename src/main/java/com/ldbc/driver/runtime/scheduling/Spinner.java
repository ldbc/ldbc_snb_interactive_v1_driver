package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.Function2;

// TODO if an error policy DOES NOT terminate the benchmark and DOES NOT allow the operation to complete
// TODO something needs to be done about DEPENDENT/GCT, because the initiated time for the operation has already been reported
// TODO perhaps the completed time for that operation needs to be reported too (to GCT service, not to MetricsService, right?),
// TODO to make sure DEPENDENT/GCT does not freeze at the start time of that "Failed" operation

// TODO take boolean result from spinner into consideration, i.e., DO NOT execute handler for "Failed" operations

public class Spinner {
    public static final Duration DEFAULT_SLEEP_DURATION_10_MILLI = Duration.fromMilli(10);
    private static final SpinnerCheck TRUE_CHECK = new TrueCheck();

    private final Function2<Operation<?>, SpinnerCheck, Boolean> spinFun;

    public Spinner(TimeSource timeSource,
                   Duration sleepDuration,
                   ExecutionDelayPolicy executionDelayPolicy,
                   Duration offset,
                   boolean ignoreScheduleStartTimes) {
        // tolerated delay only applies to actual scheduled start time
        // offset will move the scheduled start time earlier, but execution "deadline" will still be:
        //      "deadline" (i.e., latest allowed start time) = (original, i.e.,before offset applied) scheduled start time + tolerated delay
        long toleratedDelayAccountingForOffsetAsMilli = executionDelayPolicy.toleratedDelay().plus(offset).asMilli();
        this.spinFun = (ignoreScheduleStartTimes)
                ?
                new SpinFunWithoutWaitForScheduledStartTime(
                        executionDelayPolicy,
                        sleepDuration.asMilli())
                :
                new SpinFunWithWaitForScheduledStartTime(
                        timeSource,
                        offset,
                        executionDelayPolicy,
                        toleratedDelayAccountingForOffsetAsMilli,
                        sleepDuration.asMilli());
    }

    public boolean waitForScheduledStartTime(Operation<?> operation) {
        return waitForScheduledStartTime(operation, TRUE_CHECK);
    }

    /**
     * waits for the scheduled start time of an operation, and returns boolean value indicating if operation should be
     * executed or not.
     * true  = operation may still be executed
     * false = operation should not be scheduled
     * return value calculated as follows:
     * true && handleUnassignedScheduledStartTime && handleExcessiveDelay && handleFailedCheck
     * i.e., if error occurs it (1) has ability to cancel operation execution (2) can do anything else in its handler
     *
     * @param operation
     * @param check
     * @return operation may be executed
     */
    public boolean waitForScheduledStartTime(Operation<?> operation, SpinnerCheck check) {
        return spinFun.apply(operation, check);
    }

    // sleep to reduce CPU load while spinning
    // NOTE: longer sleep == lower scheduling accuracy AND lower achievable throughput
    public static void powerNap(long sleepMs) {
        if (0 == sleepMs) return;
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
        }
    }

    private static class SpinFunWithWaitForScheduledStartTime implements Function2<Operation<?>, SpinnerCheck, Boolean> {
        private final TimeSource timeSource;
        // Duration that operation will be executed before scheduled start time
        // if offset==0 operation will be scheduled at exactly operation.scheduledStartTime()
        private final Duration offset;
        private final ExecutionDelayPolicy executionDelayPolicy;
        private final long toleratedDelayAccountingForOffsetAsMilli;
        private final long sleepDurationAsMilli;

        private SpinFunWithWaitForScheduledStartTime(TimeSource timeSource,
                                                     Duration offset,
                                                     ExecutionDelayPolicy executionDelayPolicy,
                                                     long toleratedDelayAccountingForOffsetAsMilli,
                                                     long sleepDurationAsMilli) {
            this.timeSource = timeSource;
            this.offset = offset;
            this.executionDelayPolicy = executionDelayPolicy;
            this.toleratedDelayAccountingForOffsetAsMilli = toleratedDelayAccountingForOffsetAsMilli;
            this.sleepDurationAsMilli = sleepDurationAsMilli;
        }

        @Override
        public Boolean apply(Operation<?> operation, SpinnerCheck check) {
            boolean operationMayBeExecuted = true;

            // check that a scheduled start time has been assigned to the operation
            if (null == operation.scheduledStartTime() || null == operation.dependencyTime()) {
                return executionDelayPolicy.handleUnassignedTime(operation);
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
                if (timeSource.nowAsMilli() > latestAllowableStartTimeAsMilli) {
                    boolean failedCheckResult = check.handleFailedCheck(operation);
                    boolean executionDelayResult = executionDelayPolicy.handleExcessiveDelay(operation);
                    operationMayBeExecuted = operationMayBeExecuted && failedCheckResult && executionDelayResult;
                    break;
                }
                powerNap(sleepDurationAsMilli);
            }

            // wait for scheduled operation start time
            while (timeSource.nowAsMilli() < scheduledStartTimeWithOffsetAsMilli) {
                powerNap(sleepDurationAsMilli);
            }

            // check that excessive delay has not already occurred
            if (operationMayBeExecuted && timeSource.nowAsMilli() > latestAllowableStartTimeAsMilli) {
                boolean executionDelayResult = executionDelayPolicy.handleExcessiveDelay(operation);
                operationMayBeExecuted = operationMayBeExecuted && executionDelayResult;
            }

            return operationMayBeExecuted;
        }
    }

    private static class SpinFunWithoutWaitForScheduledStartTime implements Function2<Operation<?>, SpinnerCheck, Boolean> {
        // Duration that operation will be executed before scheduled start time
        // if offset==0 operation will be scheduled at exactly operation.scheduledStartTime()
        private final ExecutionDelayPolicy executionDelayPolicy;
        private final long sleepDurationAsMilli;

        private SpinFunWithoutWaitForScheduledStartTime(ExecutionDelayPolicy executionDelayPolicy,
                                                        long sleepDurationAsMilli) {
            this.executionDelayPolicy = executionDelayPolicy;
            this.sleepDurationAsMilli = sleepDurationAsMilli;
        }

        @Override
        public Boolean apply(Operation<?> operation, SpinnerCheck check) {
            boolean operationMayBeExecuted = true;

            // check that a scheduled start time has been assigned to the operation
            if (null == operation.scheduledStartTime() || null == operation.dependencyTime()) {
                return executionDelayPolicy.handleUnassignedTime(operation);
            }

            // wait for checks to have all passed before allowing operation to start
            while (operationMayBeExecuted && false == check.doCheck()) {
                powerNap(sleepDurationAsMilli);
            }

            return operationMayBeExecuted;
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
