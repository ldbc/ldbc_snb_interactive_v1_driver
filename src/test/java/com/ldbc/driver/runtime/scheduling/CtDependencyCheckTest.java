package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.DummyCompletionTimeReader;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class CtDependencyCheckTest
{
    @Test
    public void shouldOnlyReturnTrueWhenCtIsGreaterThanOrEqualToDependencyTime()
    {
        // Given
        long dependencyTimeAsMilli = 5;
        long scheduledStartTimeAsMilli = -1;
        Operation operation =
                new TimedNamedOperation1( scheduledStartTimeAsMilli, scheduledStartTimeAsMilli, dependencyTimeAsMilli,
                        null );
        DummyCompletionTimeReader dummyCompletionTimeReader = new DummyCompletionTimeReader();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        // When
        CtDependencyCheck ctDependencyCheck =
                new CtDependencyCheck( dummyCompletionTimeReader, errorReporter );

        // Then
        // CT is clearly before Dependency Time
        dummyCompletionTimeReader.setCompletionTimeAsMilli( 0 );
        assertThat( ctDependencyCheck.doCheck( operation ), is( SpinnerCheck.SpinnerCheckResult.STILL_CHECKING ) );

        // CT is just before Dependency Time
        dummyCompletionTimeReader.setCompletionTimeAsMilli( 4 );
        assertThat( ctDependencyCheck.doCheck( operation ), is( SpinnerCheck.SpinnerCheckResult.STILL_CHECKING ) );

        // CT is equal to Dependency Time
        dummyCompletionTimeReader.setCompletionTimeAsMilli( 5 );
        assertThat( ctDependencyCheck.doCheck( operation ), is( SpinnerCheck.SpinnerCheckResult.PASSED ) );

        // CT is just after Dependency Time
        dummyCompletionTimeReader.setCompletionTimeAsMilli( 6 );
        assertThat( ctDependencyCheck.doCheck( operation ), is( SpinnerCheck.SpinnerCheckResult.PASSED ) );

        // CT is clearly after Dependency Time
        dummyCompletionTimeReader.setCompletionTimeAsMilli( 10 );
        assertThat( ctDependencyCheck.doCheck( operation ), is( SpinnerCheck.SpinnerCheckResult.PASSED ) );
    }
}
