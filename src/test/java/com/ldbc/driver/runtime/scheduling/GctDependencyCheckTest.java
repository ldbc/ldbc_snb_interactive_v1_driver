package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.DummyGlobalCompletionTimeReader;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class GctDependencyCheckTest {
    @Test
    public void shouldOnlyReturnTrueWhenGctIsGreaterThanOrEqualToDependencyTime() {
        // Given
        Time dependencyTime = Time.fromMilli(5);
        Time scheduledStartTime = null;
        Operation<?> operation = new TimedNamedOperation1(scheduledStartTime, dependencyTime, null);
        DummyGlobalCompletionTimeReader dummyGlobalCompletionTimeReader = new DummyGlobalCompletionTimeReader();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        // When
        GctDependencyCheck gctDependencyCheck = new GctDependencyCheck(dummyGlobalCompletionTimeReader, operation, errorReporter);

        // Then
        // GCT is clearly before Dependency Time
        dummyGlobalCompletionTimeReader.setGlobalCompletionTime(Time.fromMilli(0));
        assertThat(gctDependencyCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING));

        // GCT is just before Dependency Time
        dummyGlobalCompletionTimeReader.setGlobalCompletionTime(Time.fromMilli(4));
        assertThat(gctDependencyCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING));

        // GCT is equal to Dependency Time
        dummyGlobalCompletionTimeReader.setGlobalCompletionTime(Time.fromMilli(5));
        assertThat(gctDependencyCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.PASSED));

        // GCT is just after Dependency Time
        dummyGlobalCompletionTimeReader.setGlobalCompletionTime(Time.fromMilli(6));
        assertThat(gctDependencyCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.PASSED));

        // GCT is clearly after Dependency Time
        dummyGlobalCompletionTimeReader.setGlobalCompletionTime(Time.fromMilli(10));
        assertThat(gctDependencyCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.PASSED));
    }
}
