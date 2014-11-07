package com.ldbc.driver.runtime.scheduling;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.ManualTimeSource;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.workloads.dummy.NothingOperation;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SpinnerTests {
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
    long ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING = 500;
    ManualTimeSource timeSource = new ManualTimeSource(0);

    @Test
    public void shouldPassWhenNoCheckAndStartTimeArrives() throws InterruptedException {
        // Given
        timeSource.setNowFromMilli(0);
        boolean ignoreScheduledStartTime = false;
        long spinnerSleepDuration = 0l;
        Spinner spinner = new Spinner(timeSource, spinnerSleepDuration, ignoreScheduledStartTime);

        long scheduledStartTime = 10l;
        Operation<?> operation = new TimedNamedOperation1(scheduledStartTime, scheduledStartTime, 0l, "name");

        SpinningThread spinningThread = new SpinningThread(spinner, operation);

        // When
        spinningThread.start();

        // Then
        // should not return before start time
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(spinningThread.spinnerHasCompleted(), is(false));
        assertThat(spinningThread.isFineToExecuteOperation(), is(false));

        timeSource.setNowFromMilli(scheduledStartTime);

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
        long spinnerSleepDuration = 0l;
        SettableSpinnerCheck check = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        Spinner spinner = new Spinner(timeSource, spinnerSleepDuration, ignoreScheduledStartTime);

        long scheduledStartTime = 10l;
        Operation<?> operation = new TimedNamedOperation1(scheduledStartTime, scheduledStartTime, 0l, "name");

        SpinningThread spinningThread = new SpinningThread(spinner, operation, check);

        // When
        spinningThread.start();

        // Then
        // time = no, check = not yet
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(spinningThread.spinnerHasCompleted(), is(false));
        assertThat(spinningThread.isFineToExecuteOperation(), is(false));

        timeSource.setNowFromMilli(scheduledStartTime - 1);

        // time = no, check = not yet
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(spinningThread.spinnerHasCompleted(), is(false));
        assertThat(spinningThread.isFineToExecuteOperation(), is(false));

        timeSource.setNowFromMilli(scheduledStartTime);

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
        long spinnerSleepDuration = 0l;
        SettableSpinnerCheck check = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        Spinner spinner = new Spinner(timeSource, spinnerSleepDuration, ignoreScheduledStartTime);

        long scheduledStartTime = 10l;
        Operation<?> operation = new TimedNamedOperation1(scheduledStartTime, scheduledStartTime, 0l, "name");

        SpinningThread spinningThread = new SpinningThread(spinner, operation, check);

        // When
        spinningThread.start();

        // Then
        // time = no, check = not yet
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(spinningThread.spinnerHasCompleted(), is(false));
        assertThat(spinningThread.isFineToExecuteOperation(), is(false));

        timeSource.setNowFromMilli(scheduledStartTime - 1);

        // time = no, check = not yet
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(spinningThread.spinnerHasCompleted(), is(false));
        assertThat(spinningThread.isFineToExecuteOperation(), is(false));

        timeSource.setNowFromMilli(scheduledStartTime);

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
        long scheduledStartTime = this.timeSource.nowAsMilli();
        long operationCount = 100000000;
        int experimentCount = 10;
        boolean ignoreScheduledStartTime;

        ignoreScheduledStartTime = false;
        Spinner spinnerWithStartTimeCheck = new Spinner(timeSource, 0l, ignoreScheduledStartTime);
        ignoreScheduledStartTime = true;
        Spinner spinnerWithoutStartTimeCheck = new Spinner(timeSource, 0l, ignoreScheduledStartTime);
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

        long singleCheckWithStartTimeCheckTestDuration = 0l;
        long manyChecksWithStartTimeCheckTestDuration = 0l;
        long singleCheckWithoutStartTimeCheckTestDuration = 0l;
        long manyChecksWithoutStartTimeCheckTestDuration = 0l;

        for (int i = 0; i < experimentCount; i++) {
            FastSameOperationIterator operationsSingleCheckWithStartTimeCheck = new FastSameOperationIterator(scheduledStartTime, operationCount);
            long singleCheckWithStartTimeCheckTestStartTime = systemTimeSource.nowAsMilli();
            while (operationsSingleCheckWithStartTimeCheck.hasNext())
                spinnerWithStartTimeCheck.waitForScheduledStartTime(operationsSingleCheckWithStartTimeCheck.next(), singleTrueCheck);
            singleCheckWithStartTimeCheckTestDuration =
                    singleCheckWithStartTimeCheckTestDuration + (systemTimeSource.nowAsMilli() - singleCheckWithStartTimeCheckTestStartTime);

            FastSameOperationIterator operationsManyChecksWithStartTimeCheck = new FastSameOperationIterator(scheduledStartTime, operationCount);
            long manyChecksWithStartTimeCheckTestStartTime = systemTimeSource.nowAsMilli();
            while (operationsManyChecksWithStartTimeCheck.hasNext())
                spinnerWithStartTimeCheck.waitForScheduledStartTime(operationsManyChecksWithStartTimeCheck.next(), manyTrueChecks);
            manyChecksWithStartTimeCheckTestDuration =
                    manyChecksWithStartTimeCheckTestDuration + (systemTimeSource.nowAsMilli() - manyChecksWithStartTimeCheckTestStartTime);

            FastSameOperationIterator operationsSingleCheckWithoutStartTimeCheck = new FastSameOperationIterator(scheduledStartTime, operationCount);
            long singleCheckWithoutStartTimeCheckTestStartTime = systemTimeSource.nowAsMilli();
            while (operationsSingleCheckWithoutStartTimeCheck.hasNext())
                spinnerWithStartTimeCheck.waitForScheduledStartTime(operationsSingleCheckWithoutStartTimeCheck.next(), singleTrueCheck);
            singleCheckWithoutStartTimeCheckTestDuration =
                    singleCheckWithoutStartTimeCheckTestDuration + (systemTimeSource.nowAsMilli() - singleCheckWithoutStartTimeCheckTestStartTime);

            FastSameOperationIterator operationsManyChecksWithoutStartTimeCheck = new FastSameOperationIterator(scheduledStartTime, operationCount);
            long manyChecksWithoutStartTimeCheckTestStartTime = systemTimeSource.nowAsMilli();
            while (operationsManyChecksWithoutStartTimeCheck.hasNext())
                spinnerWithoutStartTimeCheck.waitForScheduledStartTime(operationsManyChecksWithoutStartTimeCheck.next(), manyTrueChecks);
            manyChecksWithoutStartTimeCheckTestDuration =
                    manyChecksWithoutStartTimeCheckTestDuration + (systemTimeSource.nowAsMilli() - manyChecksWithoutStartTimeCheckTestStartTime);
        }

        singleCheckWithStartTimeCheckTestDuration = singleCheckWithStartTimeCheckTestDuration / experimentCount;
        manyChecksWithStartTimeCheckTestDuration = manyChecksWithStartTimeCheckTestDuration / experimentCount;
        singleCheckWithoutStartTimeCheckTestDuration = singleCheckWithoutStartTimeCheckTestDuration / experimentCount;
        manyChecksWithoutStartTimeCheckTestDuration = manyChecksWithoutStartTimeCheckTestDuration / experimentCount;

        System.out.println(String.format("Spinner(start time check = true) (1 true check) processed %s operations in %s time: %s ops/ms, %s ns/op",
                operationCount, singleCheckWithStartTimeCheckTestDuration, operationCount / singleCheckWithStartTimeCheckTestDuration, TEMPORAL_UTIL.convert(singleCheckWithStartTimeCheckTestDuration, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS) / operationCount));

        System.out.println(String.format("Spinner(start time check = true) (10 true checks) processed %s operations in %s time: %s ops/ms, %s ns/op",
                operationCount, manyChecksWithStartTimeCheckTestDuration, operationCount / manyChecksWithStartTimeCheckTestDuration, TEMPORAL_UTIL.convert(manyChecksWithStartTimeCheckTestDuration, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS) / operationCount));

        System.out.println(String.format("Spinner(start time check = false) (1 true check) processed %s operations in %s time: %s ops/ms, %s ns/op",
                operationCount, singleCheckWithoutStartTimeCheckTestDuration, operationCount / singleCheckWithoutStartTimeCheckTestDuration, TEMPORAL_UTIL.convert(singleCheckWithoutStartTimeCheckTestDuration, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS) / operationCount));

        System.out.println(String.format("Spinner(start time check = false) (10 true checks) processed %s operations in %s time: %s ops/ms, %s ns/op",
                operationCount, manyChecksWithoutStartTimeCheckTestDuration, operationCount / manyChecksWithoutStartTimeCheckTestDuration, TEMPORAL_UTIL.convert(manyChecksWithoutStartTimeCheckTestDuration, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS) / operationCount));
    }

    private static class FastSameOperationIterator implements Iterator<Operation<?>> {
        private final Operation<?> operation;
        private long currentOperationCount = 0;
        private final long operationCount;

        FastSameOperationIterator(long scheduledStartTime, long operationCount) {
            operation = new NothingOperation();
            operation.setScheduledStartTimeAsMilli(scheduledStartTime);
            operation.setTimeStamp(scheduledStartTime);
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
