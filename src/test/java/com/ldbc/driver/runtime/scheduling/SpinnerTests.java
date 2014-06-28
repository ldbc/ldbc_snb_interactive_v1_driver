package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.*;
import com.ldbc.driver.testutils.NothingOperation;
import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SpinnerTests {
    long ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING = 500;
    ManualTimeSource TIME_SOURCE = new ManualTimeSource(0);

    @Test
    public void shouldFailWhenNoStartTimeGiven() throws InterruptedException {
        // Given
        Duration toleratedDelay = Duration.fromMilli(10);
        CheckableDelayPolicy delayPolicy = new CheckableDelayPolicy(toleratedDelay);
        Spinner spinner = new Spinner(TIME_SOURCE, Spinner.DEFAULT_SLEEP_DURATION_10_MILLI, delayPolicy);

        Operation<?> operation = new NothingOperation();

        SpinningThread spinningThread = new SpinningThread(spinner, operation);

        assertThat(delayPolicy.excessiveDelay(), is(false));
        assertThat(delayPolicy.unassignedScheduledStartTime(), is(false));
        assertThat(delayPolicy.checkResult(), is(true));

        // When
        spinningThread.start();

        // Then
        // give spinning thread time to do its thing
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(delayPolicy.excessiveDelay(), is(false));
        assertThat(delayPolicy.unassignedScheduledStartTime(), is(true));
        assertThat(delayPolicy.checkResult(), is(true));
        assertThat(spinningThread.spinnerHasCompleted(), is(true));
        assertThat(spinningThread.shouldExecuteOperation(), is(false));

        spinningThread.join(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
    }

    @Test
    public void shouldPassWhenCheckPassesAndStartTimeArrivesAndToleratedDelayIsNotExceeded() throws InterruptedException {
        // Given
        TIME_SOURCE.setNowFromMilli(0);
        Duration toleratedDelay = Duration.fromMilli(10);
        CheckableDelayPolicy delayPolicy = new CheckableDelayPolicy(toleratedDelay);
        Spinner spinner = new Spinner(TIME_SOURCE, Spinner.DEFAULT_SLEEP_DURATION_10_MILLI, delayPolicy);

        Time scheduledStartTime = Time.fromMilli(10);
        Operation<?> operation = new NothingOperation();
        operation.setScheduledStartTime(scheduledStartTime);

        SpinningThread spinningThread = new SpinningThread(spinner, operation);

        // When
        spinningThread.start();

        // Then
        // should not return before start time
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(delayPolicy.excessiveDelay(), is(false));
        assertThat(delayPolicy.unassignedScheduledStartTime(), is(false));
        assertThat(delayPolicy.checkResult(), is(true));
        assertThat(spinningThread.spinnerHasCompleted(), is(false));
        assertThat(spinningThread.shouldExecuteOperation(), is(false));

        TIME_SOURCE.setNowFromMilli(20);

        // should return when start time reached
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(delayPolicy.excessiveDelay(), is(false));
        assertThat(delayPolicy.unassignedScheduledStartTime(), is(false));
        assertThat(delayPolicy.checkResult(), is(true));
        assertThat(spinningThread.spinnerHasCompleted(), is(true));
        assertThat(spinningThread.shouldExecuteOperation(), is(true));

        spinningThread.join(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
    }

    @Test
    public void shouldFailWhenCheckPassesAndToleratedDelayIsExceeded() throws InterruptedException {
        // Given
        TIME_SOURCE.setNowFromMilli(0);
        Duration toleratedDelay = Duration.fromMilli(10);
        CheckableDelayPolicy delayPolicy = new CheckableDelayPolicy(toleratedDelay);
        Spinner spinner = new Spinner(TIME_SOURCE, Spinner.DEFAULT_SLEEP_DURATION_10_MILLI, delayPolicy);

        Time scheduledStartTime = Time.fromMilli(10);
        Operation<?> operation = new NothingOperation();
        operation.setScheduledStartTime(scheduledStartTime);

        SpinningThread spinningThread = new SpinningThread(spinner, operation);

        // When
        spinningThread.start();

        // Then
        // should not return before start time
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(delayPolicy.excessiveDelay(), is(false));
        assertThat(delayPolicy.unassignedScheduledStartTime(), is(false));
        assertThat(delayPolicy.checkResult(), is(true));
        assertThat(spinningThread.spinnerHasCompleted(), is(false));
        assertThat(spinningThread.shouldExecuteOperation(), is(false));

        TIME_SOURCE.setNowFromMilli(21);

        // should return when start time reached
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(delayPolicy.excessiveDelay(), is(true));
        assertThat(delayPolicy.unassignedScheduledStartTime(), is(false));
        assertThat(delayPolicy.checkResult(), is(true));
        assertThat(spinningThread.spinnerHasCompleted(), is(true));
        assertThat(spinningThread.shouldExecuteOperation(), is(false));

        spinningThread.join(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
    }

    @Test
    public void shouldFailWhenCheckFailsButNotUntilToleratedDelayIsExceeded() throws InterruptedException {
        // Given
        TIME_SOURCE.setNowFromMilli(0);
        Duration toleratedDelay = Duration.fromMilli(0);
        CheckableDelayPolicy delayPolicy = new CheckableDelayPolicy(toleratedDelay);
        SpinnerCheck failCheck = new TrueFalseSpinnerCheck(delayPolicy, false);
        Spinner spinner = new Spinner(TIME_SOURCE, Spinner.DEFAULT_SLEEP_DURATION_10_MILLI, delayPolicy);

        Time scheduledStartTime = Time.fromMilli(10);
        Operation<?> operation = new NothingOperation();
        operation.setScheduledStartTime(scheduledStartTime);

        SpinningThread spinningThread = new SpinningThread(spinner, operation, failCheck);

        // When
        spinningThread.start();

        // Then
        // time has not advanced yet, spinner will still be waiting for check to pass
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(delayPolicy.excessiveDelay(), is(false));
        assertThat(delayPolicy.unassignedScheduledStartTime(), is(false));
        // this will only change to false when time has past maximum tolerated delay
        assertThat(delayPolicy.checkResult(), is(true));
        assertThat(spinningThread.spinnerHasCompleted(), is(false));

        // spinner has had enough time to perform the check, advance time past tolerated delay threshold
        TIME_SOURCE.setNowFromMilli(scheduledStartTime.plus(toleratedDelay).asMilli() + 1);

        // tolerated delay has been exceeded and check has not yet passed, spinner should return
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(delayPolicy.excessiveDelay(), is(true));
        assertThat(delayPolicy.unassignedScheduledStartTime(), is(false));
        assertThat(delayPolicy.checkResult(), is(false));
        assertThat(spinningThread.spinnerHasCompleted(), is(true));
        assertThat(spinningThread.shouldExecuteOperation(), is(false));

        spinningThread.join(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
    }

    @Test
    public void shouldReturnAndPassAtCorrectOffsetWhenCheckPasses() throws InterruptedException {
        // Given
        TIME_SOURCE.setNowFromMilli(0);
        Duration offset = Duration.fromMilli(5);
        Duration toleratedDelay = Duration.fromMilli(10);
        CheckableDelayPolicy delayPolicy = new CheckableDelayPolicy(toleratedDelay);
        Spinner spinner = new Spinner(TIME_SOURCE, Spinner.DEFAULT_SLEEP_DURATION_10_MILLI, delayPolicy, offset);

        Time scheduledStartTime = Time.fromMilli(10);
        Operation<?> operation = new NothingOperation();
        operation.setScheduledStartTime(scheduledStartTime);

        SpinningThread spinningThread = new SpinningThread(spinner, operation);

        // When
        spinningThread.start();

        // Then
        // before scheduled start time minus offset
        TIME_SOURCE.setNowFromMilli(4);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(delayPolicy.excessiveDelay(), is(false));
        assertThat(delayPolicy.unassignedScheduledStartTime(), is(false));
        assertThat(delayPolicy.checkResult(), is(true));
        assertThat(spinningThread.spinnerHasCompleted(), is(false));
        assertThat(spinningThread.shouldExecuteOperation(), is(false));

        // at scheduled start time minus offset, but still before start time
        TIME_SOURCE.setNowFromMilli(5);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
        assertThat(delayPolicy.excessiveDelay(), is(false));
        assertThat(delayPolicy.unassignedScheduledStartTime(), is(false));
        assertThat(delayPolicy.checkResult(), is(true));
        assertThat(spinningThread.spinnerHasCompleted(), is(true));
        assertThat(spinningThread.shouldExecuteOperation(), is(true));

        spinningThread.join(ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING);
    }

    @Test
    public void measureCostOfSpinnerWithNoSleepAndPassingCheckAndAtScheduledStartTime() {
        TimeSource performanceMeasuringTimeSource = new SystemTimeSource();
        TIME_SOURCE.setNowFromMilli(0);
        Time scheduledStartTime = TIME_SOURCE.now();
        long operationCount = 1000000;
        Duration toleratedDelay = Duration.fromMilli(10);
        CheckableDelayPolicy delayPolicy = new CheckableDelayPolicy(toleratedDelay);
        FastSameOperationIterator operations = new FastSameOperationIterator(scheduledStartTime, operationCount);

        Spinner spinner = new Spinner(TIME_SOURCE, Duration.fromMilli(0), delayPolicy);
        Time testStartTime = performanceMeasuringTimeSource.now();
        while (operations.hasNext())
            spinner.waitForScheduledStartTime(operations.next());
        Duration testDuration = performanceMeasuringTimeSource.now().greaterBy(testStartTime);
        System.out.println(String.format("Spinner processed %s operations in %s time: %s ops/ms, %s ns/op",
                operationCount, testDuration, operationCount / testDuration.asMilli(), testDuration.asNano() / operationCount));
    }

    private static class FastSameOperationIterator implements Iterator<Operation<?>> {
        private final Operation<?> operation;
        private long currentOperationCount = 0;
        private final long operationCount;

        FastSameOperationIterator(Time scheduledStartTime, long operationCount) {
            operation = new NothingOperation();
            operation.setScheduledStartTime(scheduledStartTime);
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

    private static class SpinningThread extends Thread {
        private final Spinner spinner;
        private final Operation<?> operation;
        private final AtomicBoolean shouldExecuteOperation;
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
            this.shouldExecuteOperation = new AtomicBoolean(false);
        }

        @Override
        public void run() {
            try {
                if (null == check)
                    shouldExecuteOperation.set(spinner.waitForScheduledStartTime(operation));
                else
                    shouldExecuteOperation.set(spinner.waitForScheduledStartTime(operation, check));
                spinnerHasCompleted.set(true);
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }

        boolean spinnerHasCompleted() {
            return spinnerHasCompleted.get();
        }

        boolean shouldExecuteOperation() {
            return shouldExecuteOperation.get();
        }
    }

    private class TrueFalseSpinnerCheck implements SpinnerCheck {
        private final CheckableDelayPolicy delayPolicy;
        private final boolean result;

        private TrueFalseSpinnerCheck(CheckableDelayPolicy delayPolicy, boolean result) {
            this.delayPolicy = delayPolicy;
            this.result = result;
        }

        @Override
        public boolean doCheck() {
            return result;
        }

        @Override
        public boolean handleFailedCheck(Operation<?> operation) {
            delayPolicy.setCheckResultToFalse();
            return false;
        }
    }

    private class CheckableDelayPolicy implements ExecutionDelayPolicy {
        private boolean unassignedScheduledStartTime = false;
        private boolean excessiveDelay = false;
        private boolean checkResult = true;
        private final Duration toleratedDelay;

        public boolean unassignedScheduledStartTime() {
            return unassignedScheduledStartTime;
        }

        public boolean excessiveDelay() {
            return excessiveDelay;
        }

        public void setCheckResultToFalse() {
            this.checkResult = false;
        }

        public boolean checkResult() {
            return checkResult;
        }

        private CheckableDelayPolicy(Duration toleratedDelay) {
            this.toleratedDelay = toleratedDelay;
        }

        @Override
        public boolean handleUnassignedScheduledStartTime(Operation<?> operation) {
            unassignedScheduledStartTime = true;
            return false;
        }

        @Override
        public Duration toleratedDelay() {
            return toleratedDelay;
        }

        @Override
        public boolean handleExcessiveDelay(Operation<?> operation) {
            excessiveDelay = true;
            return false;
        }
    }
}
