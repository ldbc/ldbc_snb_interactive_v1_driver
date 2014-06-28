package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.DummyConcurrentCompletionTimeService;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.testutils.TimedNothingOperation;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class GctCheckTest {
    @Test
    public void shouldReturnFalseWhenTimeIsGreaterThanOrEqualToGctPlusDelta() {
        // Given
        DummyConcurrentCompletionTimeService completionTimeService = new DummyConcurrentCompletionTimeService();
        Duration delta = Duration.fromMilli(100);
        Operation<?> operation = new TimedNothingOperation(Time.fromMilli(200));
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        GctCheck gctCheck = new GctCheck(completionTimeService, delta, operation, errorReporter);

        // When

        // Then
        completionTimeService.setGlobalCompletionTime(Time.fromMilli(99));
        assertThat(gctCheck.doCheck(), is(false));
        completionTimeService.setGlobalCompletionTime(Time.fromMilli(100));
        assertThat(gctCheck.doCheck(), is(false));
        completionTimeService.setGlobalCompletionTime(Time.fromMilli(101));
        assertThat(gctCheck.doCheck(), is(true));
    }
}
