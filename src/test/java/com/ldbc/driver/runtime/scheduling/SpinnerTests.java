package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SpinnerTests {
    // Assumed Spinner latency (this is a horrible solution, but "works" for now)
    // TODO a test Time implementation with manual time control would be better
    private static Duration ASSUMED_SPINNER_LATENCY = Duration.fromMilli(1);

    @Test
    public void shouldFailWhenNoStartTimeGiven() {
        // Given
        Duration toleratedDelay = Duration.fromMilli(10);
        TestExecutionDelayPolicy delayPolicy = new TestExecutionDelayPolicy(toleratedDelay);
        Spinner spinner = new Spinner(delayPolicy);

        Time scheduledStartTime = Time.now().plus(Duration.fromMilli(500));
        Operation<?> operation = new Operation<Object>() {
        };

        // When
        spinner.waitForScheduledStartTime(operation);
        Time finishedTime = Time.now();

        // Then
        assertThat(delayPolicy.excessiveDelay(), is(false));
        assertThat(delayPolicy.unassignedScheduledStartTime(), is(true));
        assertThat(delayPolicy.checkResult(), is(true));
    }

    @Test
    public void shouldFailWhenToleratedDelayIsExceeded() {
        // Given
        Duration toleratedDelay = Duration.fromMilli(0);
        TestExecutionDelayPolicy delayPolicy = new TestExecutionDelayPolicy(toleratedDelay);
        Spinner spinner = new Spinner(delayPolicy);

        Time scheduledStartTime = Time.now().minus(Duration.fromNano(1));
        Operation<?> operation = new Operation<Object>() {
        };
        operation.setScheduledStartTime(scheduledStartTime);

        // When
        spinner.waitForScheduledStartTime(operation);

        // Then
        assertThat(delayPolicy.excessiveDelay(), is(true));
        assertThat(delayPolicy.unassignedScheduledStartTime(), is(false));
        assertThat(delayPolicy.checkResult(), is(true));
    }

    @Test
    public void shouldFailWhenCheckFails() {
        // Given
        Duration toleratedDelay = Duration.fromMilli(0);
        TestExecutionDelayPolicy delayPolicy = new TestExecutionDelayPolicy(toleratedDelay);
        SpinnerCheck failCheck = new ResultSpinnerCheck(delayPolicy, false);
        Spinner spinner = new Spinner(delayPolicy);

        Time scheduledStartTime = Time.now().plus(Duration.fromMilli(500));
        Operation<?> operation = new Operation<Object>() {
        };
        operation.setScheduledStartTime(scheduledStartTime);

        // When
        spinner.waitForScheduledStartTime(operation, failCheck);

        // Then
        assertThat(delayPolicy.excessiveDelay(), is(false));
        assertThat(delayPolicy.unassignedScheduledStartTime(), is(false));
        assertThat(delayPolicy.checkResult(), is(false));
    }

    @Test
    public void shouldPassWhenCheckPasses() {
        // Given
        Duration toleratedDelay = Duration.fromMilli(0);
        TestExecutionDelayPolicy delayPolicy = new TestExecutionDelayPolicy(toleratedDelay);
        SpinnerCheck passCheck = new ResultSpinnerCheck(delayPolicy, true);
        Spinner spinner = new Spinner(delayPolicy);

        Time scheduledStartTime = Time.now().plus(Duration.fromMilli(500));
        Operation<?> operation = new Operation<Object>() {
        };
        operation.setScheduledStartTime(scheduledStartTime);

        // When
        spinner.waitForScheduledStartTime(operation, passCheck);

        // Then
        assertThat(delayPolicy.excessiveDelay(), is(false));
        assertThat(delayPolicy.unassignedScheduledStartTime(), is(false));
        assertThat(delayPolicy.checkResult(), is(true));
    }

    @Test
    public void shouldNotReturnBeforeStartTime() {
        // Given
        Duration toleratedDelay = Duration.fromMilli(10);
        TestExecutionDelayPolicy delayPolicy = new TestExecutionDelayPolicy(toleratedDelay);
        Spinner spinner = new Spinner(delayPolicy);

        Time scheduledStartTime = Time.now().plus(Duration.fromMilli(500));
        Operation<?> operation = new Operation<Object>() {
        };
        operation.setScheduledStartTime(scheduledStartTime);

        // When
        spinner.waitForScheduledStartTime(operation);
        Time finishedTime = Time.now();

        // Then
        assertThat(delayPolicy.excessiveDelay(), is(false));
        assertThat(delayPolicy.unassignedScheduledStartTime(), is(false));
        assertThat(finishedTime.gte(scheduledStartTime), is(true));
        assertThat(finishedTime.lte(scheduledStartTime.plus(ASSUMED_SPINNER_LATENCY)), is(true));
    }

    @Test
    public void shouldNotReturnBeforeStartTimeMinusOffset() {
        // Given
        Duration offset = Duration.fromMilli(400);
        Duration toleratedDelay = Duration.fromMilli(10);
        TestExecutionDelayPolicy delayPolicy = new TestExecutionDelayPolicy(toleratedDelay);
        Spinner spinner = new Spinner(delayPolicy, offset);

        Time scheduledStartTime = Time.now().plus(Duration.fromMilli(500));
        Operation<?> operation = new Operation<Object>() {
        };
        operation.setScheduledStartTime(scheduledStartTime);

        // When
        spinner.waitForScheduledStartTime(operation);
        Time finishedTime = Time.now();

        // Then
        assertThat(delayPolicy.excessiveDelay(), is(false));
        assertThat(delayPolicy.unassignedScheduledStartTime(), is(false));
        assertThat(finishedTime.gte(scheduledStartTime.minus(offset)), is(true));
        assertThat(finishedTime.lte(scheduledStartTime.minus(offset).plus(ASSUMED_SPINNER_LATENCY)), is(true));
    }

    private class ResultSpinnerCheck implements SpinnerCheck {
        private final TestExecutionDelayPolicy delayPolicy;
        private final boolean result;

        private ResultSpinnerCheck(TestExecutionDelayPolicy delayPolicy, boolean result) {
            this.delayPolicy = delayPolicy;
            this.result = result;
        }

        @Override
        public Boolean doCheck() {
            return result;
        }

        @Override
        public void handleFailedCheck(Operation<?> operation) {
            delayPolicy.setFailed();
        }
    }

    private class TestExecutionDelayPolicy implements ExecutionDelayPolicy {
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

        public void setFailed() {
            this.checkResult = false;
        }

        public boolean checkResult() {
            return checkResult;
        }

        private TestExecutionDelayPolicy(Duration toleratedDelay) {
            this.toleratedDelay = toleratedDelay;
        }

        @Override
        public void handleUnassignedScheduledStartTime(Operation<?> operation) {
            unassignedScheduledStartTime = true;
        }

        @Override
        public Duration toleratedDelay() {
            return toleratedDelay;
        }

        @Override
        public void handleExcessiveDelay(Operation<?> operation) {
            excessiveDelay = true;
        }
    }
}
