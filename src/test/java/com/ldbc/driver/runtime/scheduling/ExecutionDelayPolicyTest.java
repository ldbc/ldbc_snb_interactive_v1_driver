package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.workloads.dummy.NothingOperation;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ExecutionDelayPolicyTest {
    private TimeSource timeSource = new SystemTimeSource();

    @Test
    public void errorReportingPolicyShouldReportErrorWhenHandleExcessiveDelayIsCalled() {
        // Given
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        Duration toleratedDelay = Duration.fromMilli(10);
        ExecutionDelayPolicy delayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(timeSource, toleratedDelay, errorReporter);
        Operation<?> operation = new NothingOperation();
        operation.setScheduledStartTimeAsMilli(timeSource.now().minus(Duration.fromMilli(2000)));

        assertThat(errorReporter.errorEncountered(), is(false));

        // When
        delayPolicy.handleExcessiveDelay(operation);

        // Then
        assertThat(errorReporter.errorEncountered(), is(true));
    }

    @Test
    public void errorReportingPolicyShouldReturnExpectedToleratedDelay() {
        // Given
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        Duration toleratedDelay = Duration.fromMilli(10);

        // When
        ExecutionDelayPolicy delayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(timeSource, toleratedDelay, errorReporter);

        // Then
        assertThat(delayPolicy.toleratedDelayAsMilli(), is(Duration.fromMilli(10)));
    }
}
