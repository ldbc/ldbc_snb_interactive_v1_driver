package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.DummyConcurrentCompletionTimeService;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.testutils.TimedOperation;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class GctDependencyCheckTest {
    @Test
    public void shouldOnlyReturnTrueWhenGctIsGreaterThanOrEqualToDependencyTime() {
        // Given
        Time dependencyTime = Time.fromMilli(5);
        Time scheduledStartTime = null;
        Operation<?> operation = new TimedOperation(scheduledStartTime, dependencyTime);
        DummyConcurrentCompletionTimeService completionTimeService = new DummyConcurrentCompletionTimeService();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        // When
        GctDependencyCheck gctDependencyCheck = new GctDependencyCheck(completionTimeService, operation, errorReporter);

        // Then
        // GCT is clearly before Dependency Time
        completionTimeService.setGlobalCompletionTime(Time.fromMilli(0));
        assertThat(gctDependencyCheck.doCheck(), is(false));

        // GCT is just before Dependency Time
        completionTimeService.setGlobalCompletionTime(Time.fromMilli(4));
        assertThat(gctDependencyCheck.doCheck(), is(false));

        // GCT is equal to Dependency Time
        completionTimeService.setGlobalCompletionTime(Time.fromMilli(5));
        assertThat(gctDependencyCheck.doCheck(), is(true));

        // GCT is just after Dependency Time
        completionTimeService.setGlobalCompletionTime(Time.fromMilli(6));
        assertThat(gctDependencyCheck.doCheck(), is(true));

        // GCT is clearly after Dependency Time
        completionTimeService.setGlobalCompletionTime(Time.fromMilli(10));
        assertThat(gctDependencyCheck.doCheck(), is(true));
    }
}
