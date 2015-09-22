package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.DummyGlobalCompletionTimeReader;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class GctDependencyCheckTest
{
    @Test
    public void shouldOnlyReturnTrueWhenGctIsGreaterThanOrEqualToDependencyTime()
    {
        // Given
        long dependencyTimeAsMilli = 5;
        long scheduledStartTimeAsMilli = -1;
        Operation operation =
                new TimedNamedOperation1( scheduledStartTimeAsMilli, scheduledStartTimeAsMilli, dependencyTimeAsMilli,
                        null );
        DummyGlobalCompletionTimeReader dummyGlobalCompletionTimeReader = new DummyGlobalCompletionTimeReader();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        // When
        GctDependencyCheck gctDependencyCheck =
                new GctDependencyCheck( dummyGlobalCompletionTimeReader, errorReporter );

        // Then
        // GCT is clearly before Dependency Time
        dummyGlobalCompletionTimeReader.setGlobalCompletionTimeAsMilli( 0 );
        assertThat( gctDependencyCheck.doCheck( operation ), is( SpinnerCheck.SpinnerCheckResult.STILL_CHECKING ) );

        // GCT is just before Dependency Time
        dummyGlobalCompletionTimeReader.setGlobalCompletionTimeAsMilli( 4 );
        assertThat( gctDependencyCheck.doCheck( operation ), is( SpinnerCheck.SpinnerCheckResult.STILL_CHECKING ) );

        // GCT is equal to Dependency Time
        dummyGlobalCompletionTimeReader.setGlobalCompletionTimeAsMilli( 5 );
        assertThat( gctDependencyCheck.doCheck( operation ), is( SpinnerCheck.SpinnerCheckResult.PASSED ) );

        // GCT is just after Dependency Time
        dummyGlobalCompletionTimeReader.setGlobalCompletionTimeAsMilli( 6 );
        assertThat( gctDependencyCheck.doCheck( operation ), is( SpinnerCheck.SpinnerCheckResult.PASSED ) );

        // GCT is clearly after Dependency Time
        dummyGlobalCompletionTimeReader.setGlobalCompletionTimeAsMilli( 10 );
        assertThat( gctDependencyCheck.doCheck( operation ), is( SpinnerCheck.SpinnerCheckResult.PASSED ) );
    }
}
