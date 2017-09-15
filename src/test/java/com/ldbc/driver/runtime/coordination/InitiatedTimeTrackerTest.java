package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.coordination.CompletionTimeStateManager.InitiatedTimeTrackerImpl;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;


public class InitiatedTimeTrackerTest
{
    @Test
    public void shouldReturnNullsWhenNoTimesHaveBeenSubmitted_TreeMultiSetImplementation()
    {
        shouldReturnNullsWhenNoTimesHaveBeenSubmitted( InitiatedTimeTrackerImpl.createUsingTreeMultiSet() );
    }

    @Test
    public void shouldReturnNullsWhenNoTimesHaveBeenSubmitted_ListImplementation()
    {
        shouldReturnNullsWhenNoTimesHaveBeenSubmitted( InitiatedTimeTrackerImpl.createUsingArrayList() );
    }

    private void shouldReturnNullsWhenNoTimesHaveBeenSubmitted( CompletionTimeStateManager.InitiatedTimeTracker
            tracker )
    {
        // Given
        // tracker

        // When
        // nothing

        // Then
        assertThat( tracker.highestInitiatedTimeAsMilli(), is( -1L ) );
        boolean exceptionThrown = false;
        try
        {
            tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli( 1L );
        }
        catch ( CompletionTimeException e )
        {
            exceptionThrown = true;
        }
        assertThat( exceptionThrown, is( true ) );
        assertThat( tracker.uncompletedInitiatedTimes(), is( 0 ) );
    }

    @Test
    public void shouldBehaveAsExpectedUnderScenario1_TreeMultiSetImplementation() throws CompletionTimeException
    {
        shouldBehaveAsExpectedUnderScenario1( CompletionTimeStateManager.InitiatedTimeTrackerImpl.createUsingTreeMultiSet() );
    }

    @Test
    public void shouldBehaveAsExpectedUnderScenario1_ListImplementation() throws CompletionTimeException
    {
        shouldBehaveAsExpectedUnderScenario1( InitiatedTimeTrackerImpl.createUsingArrayList() );
    }

    private void shouldBehaveAsExpectedUnderScenario1( CompletionTimeStateManager.InitiatedTimeTracker tracker )
            throws CompletionTimeException
    {
        // Given
        // tracker

        // When/Then
        assertThat( tracker.highestInitiatedTimeAsMilli(), is( -1L ) );
        boolean exceptionThrown = false;
        try
        {
            tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli( 1L );
        }
        catch ( CompletionTimeException e )
        {
            exceptionThrown = true;
        }
        assertThat( exceptionThrown, is( true ) );
        assertThat( tracker.uncompletedInitiatedTimes(), is( 0 ) );

        // [0]
        assertThat( tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli( 0L ), equalTo( 0L ) );

        assertThat( tracker.highestInitiatedTimeAsMilli(), is( 0L ) );
        assertThat( tracker.uncompletedInitiatedTimes(), is( 1 ) );

        // [0]
        exceptionThrown = false;
        try
        {
            tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli( 1L );
        }
        catch ( CompletionTimeException e )
        {
            exceptionThrown = true;
        }
        assertThat( exceptionThrown, is( true ) );

        // [0,0]
        assertThat( tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli( 0L ), equalTo( 0L ) );

        assertThat( tracker.highestInitiatedTimeAsMilli(), is( 0L ) );
        assertThat( tracker.uncompletedInitiatedTimes(), is( 2 ) );

        // [0,0]
        exceptionThrown = false;
        try
        {
            tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli( 1L );
        }
        catch ( CompletionTimeException e )
        {
            exceptionThrown = true;
        }
        assertThat( exceptionThrown, is( true ) );

        // [0,0,1,1,4,5]
        assertThat( tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli( 1L ), equalTo( 0L ) );
        assertThat( tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli( 1L ), equalTo( 0L ) );
        assertThat( tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli( 4L ), equalTo( 0L ) );
        assertThat( tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli( 5L ), equalTo( 0L ) );

        assertThat( tracker.highestInitiatedTimeAsMilli(), is( 5L ) );
        assertThat( tracker.uncompletedInitiatedTimes(), is( 6 ) );

        // [0,0,1,1,4,5,7,9,10,15]
        assertThat( tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli( 7L ), equalTo( 0L ) );
        assertThat( tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli( 9L ), equalTo( 0L ) );
        assertThat( tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli( 10L ), equalTo( 0L ) );
        assertThat( tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli( 15L ), equalTo( 0L ) );

        assertThat( tracker.highestInitiatedTimeAsMilli(), is( 15L ) );
        assertThat( tracker.uncompletedInitiatedTimes(), is( 10 ) );

        // [0,0,1,1,4,5,7,9,10,15]
        exceptionThrown = false;
        try
        {
            tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli( 2L );
        }
        catch ( CompletionTimeException e )
        {
            exceptionThrown = true;
        }
        assertThat( exceptionThrown, is( true ) );
        exceptionThrown = false;
        try
        {
            tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli( 3L );
        }
        catch ( CompletionTimeException e )
        {
            exceptionThrown = true;
        }
        assertThat( exceptionThrown, is( true ) );

        // [ ,0,1,1,4,5,7,9,10,15]
        assertThat( tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli( 0L ), is( 0L ) );
        // [ ,0,1,1,4, ,7,9,10,15]
        assertThat( tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli( 5L ), is( 0L ) );
        // [ ,0,1,1,4, ,7,9, ,15]
        assertThat( tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli( 10L ), is( 0L ) );

        // [ ,0,1,1,4, ,7,9, ,15]
        assertThat( tracker.highestInitiatedTimeAsMilli(), is( 15L ) );
        assertThat( tracker.uncompletedInitiatedTimes(), is( 7 ) );

        // [ , ,1,1,4, ,7,9, ,15]
        assertThat( tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli( 0L ), is( 1L ) );

        // [ , ,1,1, , ,7,9, ,15]
        assertThat( tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli( 4L ), is( 1L ) );

        // [ , ,1,1, , ,7, , ,15]
        assertThat( tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli( 9L ), is( 1L ) );

        // [ , ,1,1, , ,7, , ,15]
        assertThat( tracker.highestInitiatedTimeAsMilli(), is( 15L ) );
        assertThat( tracker.uncompletedInitiatedTimes(), is( 4 ) );

        // [ , ,1,1, , ,7, , ,15,15,15]
        exceptionThrown = false;
        try
        {
            tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli( 14L );
        }
        catch ( CompletionTimeException e )
        {
            exceptionThrown = true;
        }
        assertThat( exceptionThrown, is( true ) );
        assertThat( tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli( 15L ), equalTo( 1L ) );
        assertThat( tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli( 15L ), equalTo( 1L ) );

        assertThat( tracker.highestInitiatedTimeAsMilli(), is( 15L ) );
        assertThat( tracker.uncompletedInitiatedTimes(), is( 6 ) );

        // [ , ,1,1, , ,7, , , ,15,15]
        assertThat( tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli( 15L ), is( 1L ) );

        // [ , ,1,1, , ,7, , , , ,15]
        assertThat( tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli( 15L ), is( 1L ) );

        // [ , ,1,1, , ,7, , , , , ]
        assertThat( tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli( 15L ), is( 1L ) );

        // [ , ,1,1, , ,7, , , , , ]
        assertThat( tracker.highestInitiatedTimeAsMilli(), is( 15L ) );
        assertThat( tracker.uncompletedInitiatedTimes(), is( 3 ) );

        // [ , , ,1, , ,7, , , , , ]
        assertThat( tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli( 1L ), is( 1L ) );

        // [ , , , , , ,7, , , , , ]
        assertThat( tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli( 1L ), is( 7L ) );

        // [ , , , , , ,7, , , , , ]
        assertThat( tracker.highestInitiatedTimeAsMilli(), is( 15L ) );
        assertThat( tracker.uncompletedInitiatedTimes(), is( 1 ) );

        // [ , , , , , , , , , , , ]
        assertThat( tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli( 7L ), is( 15L ) );

        // [ , , , , , , , , , , , ]
        assertThat( tracker.highestInitiatedTimeAsMilli(), is( 15L ) );
        assertThat( tracker.uncompletedInitiatedTimes(), is( 0 ) );

        for ( int i = 16; i < 10000; i++ )
        {
            assertThat( tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli( i ), equalTo( 16L ) );
        }
        for ( int i = 16; i < 9999; i++ )
        {
            assertThat( tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli( i ), is( i + 1L ) );
        }
        assertThat( tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli( 9999L ), is( 9999L ) );
    }
}
