package com.ldbc.driver.runtime.scheduling;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.*;
import com.ldbc.driver.workloads.dummy.NothingOperation;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SpinnerTests {
    long ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING = 500;
    ManualTimeSource timeSource = new ManualTimeSource(0);

    @Test
    public void shouldPassWhenNoCheckAndStartTimeArrives() throws InterruptedException {
        // Given
        timeSource.setNowFromMilli(0);
        boolean ignoreScheduledStartTime = false;
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        Spinner spinner = new Spinner(timeSource, spinnerSleepDuration, ignoreScheduledStartTime);

        Time scheduledStartTime = Time.fromMilli(10);
        Operation<?> operation = new TimedNamedOperation1(scheduledStartTime, Time.fromMilli(0), "name");

        SpinningThread spinningThread = new SpinningThread(spinner, operation);

        // When
        spinningThread.start();

        // Then
        // should not return before start time
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(spinningThread.spinnerHasCompleted(), is(false));
        assertThat(spinningThread.isFineToExecuteOperation(), is(false));

        timeSource.setNowFromMilli(scheduledStartTime.asMilli());

        // should return when start time reached
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(spinningThread.spinnerHasCompleted(), is(true));
        assertThat(spinningThread.isFineToExecuteOperation(), is(true));

        spinningThread.join(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
    }

    @Test
    public void shouldPassOnlyWhenCheckPassesAndStartTimeArrives() throws InterruptedException {
        // Given
        timeSource.setNowFromMilli(0);
        boolean ignoreScheduledStartTime = false;
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        SettableSpinnerCheck check = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        Spinner spinner = new Spinner(timeSource, spinnerSleepDuration, ignoreScheduledStartTime);

        Time scheduledStartTime = Time.fromMilli(10);
        Operation<?> operation = new TimedNamedOperation1(scheduledStartTime, Time.fromMilli(0), "name");

        SpinningThread spinningThread = new SpinningThread(spinner, operation, check);

        // When
        spinningThread.start();

        // Then
        // time = no, check = not yet
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(spinningThread.spinnerHasCompleted(), is(false));
        assertThat(spinningThread.isFineToExecuteOperation(), is(false));

        timeSource.setNowFromMilli(scheduledStartTime.asMilli() - 1);

        // time = no, check = not yet
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(spinningThread.spinnerHasCompleted(), is(false));
        assertThat(spinningThread.isFineToExecuteOperation(), is(false));

        timeSource.setNowFromMilli(scheduledStartTime.asMilli());

        // time = yes, check = not yet
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(spinningThread.spinnerHasCompleted(), is(false));
        assertThat(spinningThread.isFineToExecuteOperation(), is(false));

        check.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);

        // time = yes, check = yes
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(spinningThread.spinnerHasCompleted(), is(true));
        assertThat(spinningThread.isFineToExecuteOperation(), is(true));

        spinningThread.join(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
    }

    @Test
    public void shouldFailWhenCheckFails() throws InterruptedException {
        // Given
        timeSource.setNowFromMilli(0);
        boolean ignoreScheduledStartTime = false;
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        SettableSpinnerCheck check = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        Spinner spinner = new Spinner(timeSource, spinnerSleepDuration, ignoreScheduledStartTime);

        Time scheduledStartTime = Time.fromMilli(10);
        Operation<?> operation = new TimedNamedOperation1(scheduledStartTime, Time.fromMilli(0), "name");

        SpinningThread spinningThread = new SpinningThread(spinner, operation, check);

        // When
        spinningThread.start();

        // Then
        // time = no, check = not yet
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(spinningThread.spinnerHasCompleted(), is(false));
        assertThat(spinningThread.isFineToExecuteOperation(), is(false));

        timeSource.setNowFromMilli(scheduledStartTime.asMilli() - 1);

        // time = no, check = not yet
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(spinningThread.spinnerHasCompleted(), is(false));
        assertThat(spinningThread.isFineToExecuteOperation(), is(false));

        timeSource.setNowFromMilli(scheduledStartTime.asMilli());

        // time = yes, check = not yet
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(spinningThread.spinnerHasCompleted(), is(false));
        assertThat(spinningThread.isFineToExecuteOperation(), is(false));

        check.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);

        // time = yes, check = no
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(spinningThread.spinnerHasCompleted(), is(true));
        assertThat(spinningThread.isFineToExecuteOperation(), is(false));

        spinningThread.join(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
    }

    private static class SpinningThread extends Thread {
        private final Spinner spinner;
        private final Operation<?> operation;
        private final AtomicBoolean isFineToExecuteOperation;
        private final AtomicBoolean spinnerHasCompleted;
        private final SpinnerCheck check;

        SpinningThread(Spinner spinner, Operation<?> operation) {
            this(spinner, operation, null);
        }

        SpinningThread(Spinner spinner, Operation<?> operation, SpinnerCheck check) {
            this.spinner = spinner;
            this.operation = operation;
            this.check = check;
            this.spinnerHasCompleted = new AtomicBoolean(false);
            this.isFineToExecuteOperation = new AtomicBoolean(false);
        }

        @Override
        public void run() {
            try {
                if (null == check)
                    isFineToExecuteOperation.set(spinner.waitForScheduledStartTime(operation));
                else
                    isFineToExecuteOperation.set(spinner.waitForScheduledStartTime(operation, check));
                spinnerHasCompleted.set(true);
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }

        boolean spinnerHasCompleted() {
            return spinnerHasCompleted.get();
        }

        boolean isFineToExecuteOperation() {
            return isFineToExecuteOperation.get();
        }
    }

    // This testing methodology seems bad, not enough iterations or something, the numbers are dependent on order things are done
    @Ignore
    @Test
    public void measureCostOfSpinnerWithNoSleepAndPassingCheckAndAtScheduledStartTime() {
        TimeSource systemTimeSource = new SystemTimeSource();
        timeSource.setNowFromMilli(0);
        Time scheduledStartTime = this.timeSource.now();
        long operationCount = 100000000;
        int experimentCount = 10;
        boolean ignoreScheduledStartTime;

        ignoreScheduledStartTime = false;
        Spinner spinnerWithStartTimeCheck = new Spinner(timeSource, Duration.fromMilli(0), ignoreScheduledStartTime);
        ignoreScheduledStartTime = true;
        Spinner spinnerWithoutStartTimeCheck = new Spinner(timeSource, Duration.fromMilli(0), ignoreScheduledStartTime);
        SpinnerCheck singleTrueCheck = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.PASSED);
        SpinnerCheck manyTrueChecks = new MultiCheck(
                Lists.<SpinnerCheck>newArrayList(
                        new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.PASSED),
                        new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.PASSED),
                        new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.PASSED),
                        new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.PASSED),
                        new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.PASSED),
                        new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.PASSED),
                        new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.PASSED),
                        new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.PASSED),
                        new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.PASSED),
                        new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.PASSED)
                )
        );

        Duration singleCheckWithStartTimeCheckTestDuration = Duration.fromNano(0);
        Duration manyChecksWithStartTimeCheckTestDuration = Duration.fromNano(0);
        Duration singleCheckWithoutStartTimeCheckTestDuration = Duration.fromNano(0);
        Duration manyChecksWithoutStartTimeCheckTestDuration = Duration.fromNano(0);

        for (int i = 0; i < experimentCount; i++) {
            FastSameOperationIterator operationsSingleCheckWithStartTimeCheck = new FastSameOperationIterator(scheduledStartTime, operationCount);
            Time singleCheckWithStartTimeCheckTestStartTime = systemTimeSource.now();
            while (operationsSingleCheckWithStartTimeCheck.hasNext())
                spinnerWithStartTimeCheck.waitForScheduledStartTime(operationsSingleCheckWithStartTimeCheck.next(), singleTrueCheck);
            singleCheckWithStartTimeCheckTestDuration =
                    singleCheckWithStartTimeCheckTestDuration.plus(systemTimeSource.now().durationGreaterThan(singleCheckWithStartTimeCheckTestStartTime));

            FastSameOperationIterator operationsManyChecksWithStartTimeCheck = new FastSameOperationIterator(scheduledStartTime, operationCount);
            Time manyChecksWithStartTimeCheckTestStartTime = systemTimeSource.now();
            while (operationsManyChecksWithStartTimeCheck.hasNext())
                spinnerWithStartTimeCheck.waitForScheduledStartTime(operationsManyChecksWithStartTimeCheck.next(), manyTrueChecks);
            manyChecksWithStartTimeCheckTestDuration =
                    manyChecksWithStartTimeCheckTestDuration.plus(systemTimeSource.now().durationGreaterThan(manyChecksWithStartTimeCheckTestStartTime));

            FastSameOperationIterator operationsSingleCheckWithoutStartTimeCheck = new FastSameOperationIterator(scheduledStartTime, operationCount);
            Time singleCheckWithoutStartTimeCheckTestStartTime = systemTimeSource.now();
            while (operationsSingleCheckWithoutStartTimeCheck.hasNext())
                spinnerWithStartTimeCheck.waitForScheduledStartTime(operationsSingleCheckWithoutStartTimeCheck.next(), singleTrueCheck);
            singleCheckWithoutStartTimeCheckTestDuration =
                    singleCheckWithoutStartTimeCheckTestDuration.plus(systemTimeSource.now().durationGreaterThan(singleCheckWithoutStartTimeCheckTestStartTime));

            FastSameOperationIterator operationsManyChecksWithoutStartTimeCheck = new FastSameOperationIterator(scheduledStartTime, operationCount);
            Time manyChecksWithoutStartTimeCheckTestStartTime = systemTimeSource.now();
            while (operationsManyChecksWithoutStartTimeCheck.hasNext())
                spinnerWithoutStartTimeCheck.waitForScheduledStartTime(operationsManyChecksWithoutStartTimeCheck.next(), manyTrueChecks);
            manyChecksWithoutStartTimeCheckTestDuration =
                    manyChecksWithoutStartTimeCheckTestDuration.plus(systemTimeSource.now().durationGreaterThan(manyChecksWithoutStartTimeCheckTestStartTime));
        }

        singleCheckWithStartTimeCheckTestDuration = Duration.fromNano(singleCheckWithStartTimeCheckTestDuration.asNano() / experimentCount);
        manyChecksWithStartTimeCheckTestDuration = Duration.fromNano(manyChecksWithStartTimeCheckTestDuration.asNano() / experimentCount);
        singleCheckWithoutStartTimeCheckTestDuration = Duration.fromNano(singleCheckWithoutStartTimeCheckTestDuration.asNano() / experimentCount);
        manyChecksWithoutStartTimeCheckTestDuration = Duration.fromNano(manyChecksWithoutStartTimeCheckTestDuration.asNano() / experimentCount);

        System.out.println(String.format("Spinner(start time check = true) (1 true check) processed %s operations in %s time: %s ops/ms, %s ns/op",
                operationCount, singleCheckWithStartTimeCheckTestDuration, operationCount / singleCheckWithStartTimeCheckTestDuration.asMilli(), singleCheckWithStartTimeCheckTestDuration.asNano() / operationCount));

        System.out.println(String.format("Spinner(start time check = true) (10 true checks) processed %s operations in %s time: %s ops/ms, %s ns/op",
                operationCount, manyChecksWithStartTimeCheckTestDuration, operationCount / manyChecksWithStartTimeCheckTestDuration.asMilli(), manyChecksWithStartTimeCheckTestDuration.asNano() / operationCount));

        System.out.println(String.format("Spinner(start time check = false) (1 true check) processed %s operations in %s time: %s ops/ms, %s ns/op",
                operationCount, singleCheckWithoutStartTimeCheckTestDuration, operationCount / singleCheckWithoutStartTimeCheckTestDuration.asMilli(), singleCheckWithoutStartTimeCheckTestDuration.asNano() / operationCount));

        System.out.println(String.format("Spinner(start time check = false) (10 true checks) processed %s operations in %s time: %s ops/ms, %s ns/op",
                operationCount, manyChecksWithoutStartTimeCheckTestDuration, operationCount / manyChecksWithoutStartTimeCheckTestDuration.asMilli(), manyChecksWithoutStartTimeCheckTestDuration.asNano() / operationCount));
    }

    private static class FastSameOperationIterator implements Iterator<Operation<?>> {
        private final Operation<?> operation;
        private long currentOperationCount = 0;
        private final long operationCount;

        FastSameOperationIterator(Time scheduledStartTime, long operationCount) {
            operation = new NothingOperation();
            operation.setScheduledStartTimeAsMilli(scheduledStartTime);
            this.operationCount = operationCount;
        }

        @Override
        public boolean hasNext() {
            return currentOperationCount < operationCount;
        }

        @Override
        public Operation<?> next() {
            currentOperationCount++;
            return operation;
        }

        @Override
        public void remove() {
        }
    }
}
