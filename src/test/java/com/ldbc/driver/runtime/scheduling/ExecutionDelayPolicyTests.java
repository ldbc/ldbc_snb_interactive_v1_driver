package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ExecutionDelayPolicyTests {

    @Test
    public void errorReportingPolicyShouldReportErrorWhenHandleExcessiveDelayIsCalled() {
        // Given
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        Duration toleratedDelay = Duration.fromMilli(10);
        ExecutionDelayPolicy delayPolicy = new ErrorReportingExecutionDelayPolicy(toleratedDelay, errorReporter);
        Operation<?> operation = new Operation<Object>() {
        };
        operation.setScheduledStartTime(Time.now().minus(Duration.fromMilli(2000)));

        assertThat(errorReporter.errorEncountered(), is(false));

        // When
        delayPolicy.handleExcessiveDelay(operation);

        // Then
        assertThat(errorReporter.errorEncountered(), is(true));
    }

    @Test
    public void errorReportingPolicyShouldReportErrorWhenHandleUnassignedStartTimeIsCalled() {
        // Given
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        Duration toleratedDelay = Duration.fromMilli(10);
        ExecutionDelayPolicy delayPolicy = new ErrorReportingExecutionDelayPolicy(toleratedDelay, errorReporter);
        Operation<?> operation = new Operation<Object>() {
        };

        assertThat(errorReporter.errorEncountered(), is(false));

        // When
        delayPolicy.handleUnassignedScheduledStartTime(operation);

        // Then
        assertThat(errorReporter.errorEncountered(), is(true));
    }

    @Test
    public void errorReportingPolicyShouldReturnExpectedToleratedDelay() {
        // Given
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        Duration toleratedDelay = Duration.fromMilli(10);

        // When
        ExecutionDelayPolicy delayPolicy = new ErrorReportingExecutionDelayPolicy(toleratedDelay, errorReporter);

        // Then
        assertThat(delayPolicy.toleratedDelay(), is(Duration.fromMilli(10)));
    }
}
