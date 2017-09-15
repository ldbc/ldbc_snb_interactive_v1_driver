package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.coordination.CompletionTimeStateManager.CompletedTimeTrackerImpl;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class CompletedTimeTrackerTest
{
    @Test
    public void shouldReturnNullsWhenNoTimesHaveBeenSubmitted() throws CompletionTimeException
    {
        // Given
        CompletedTimeTrackerImpl tracker = CompletionTimeStateManager.CompletedTimeTrackerImpl.createUsingTreeMultiSet();

        // When
        // nothing

        // Then
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( Long.MAX_VALUE ), is( -1L ) );
    }

    @Test
    public void shouldRemoveTimesCorrectlyWhenThereIsOnlyOneTime_UsingTreeMultiSet() throws CompletionTimeException
    {
        shouldRemoveTimesCorrectlyWhenThereIsOnlyOneTime( CompletionTimeStateManager.CompletedTimeTrackerImpl.createUsingTreeMultiSet() );
    }

    @Test
    public void shouldRemoveTimesCorrectlyWhenThereIsOnlyOneTime_UsingList() throws CompletionTimeException
    {
        shouldRemoveTimesCorrectlyWhenThereIsOnlyOneTime( CompletionTimeStateManager.CompletedTimeTrackerImpl.createUsingArrayList() );
    }

    private void shouldRemoveTimesCorrectlyWhenThereIsOnlyOneTime( CompletionTimeStateManager.CompletedTimeTrackerImpl tracker )
            throws CompletionTimeException
    {
        // Given
        // tracker

        // When
        tracker.addCompletedTimeAsMilli( 1L );

        // Then
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( 0L ), is( -1L ) );
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( 1L ), is( -1L ) );
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( Long.MAX_VALUE ), is( 1L ) );
    }

    @Test
    public void shouldRemoveTimesCorrectlyWhenThereIsAreMultipleTimesThatAreAddedInOrder_UsingTreeMultiSet()
            throws CompletionTimeException
    {
        shouldRemoveTimesCorrectlyWhenThereIsAreMultipleTimesThatAreAddedInOrder(
                CompletedTimeTrackerImpl.createUsingTreeMultiSet() );
    }

    @Test
    public void shouldRemoveTimesCorrectlyWhenThereIsAreMultipleTimesThatAreAddedInOrder_UsingList()
            throws CompletionTimeException
    {
        shouldRemoveTimesCorrectlyWhenThereIsAreMultipleTimesThatAreAddedInOrder(
                CompletionTimeStateManager.CompletedTimeTrackerImpl.createUsingArrayList() );
    }

    private void shouldRemoveTimesCorrectlyWhenThereIsAreMultipleTimesThatAreAddedInOrder(
            CompletionTimeStateManager.CompletedTimeTrackerImpl tracker ) throws CompletionTimeException
    {
        // Given
        // tracker

        // When/Then
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( 0L ), is( -1L ) );
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( Long.MAX_VALUE ), is( -1L ) );

        // [1]
        tracker.addCompletedTimeAsMilli( 1L );

        // [1]
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( 0L ), is( -1L ) );
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( 1L ), is( -1L ) );

        // [1,2]
        tracker.addCompletedTimeAsMilli( 2L );

        // [1,2]
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( 0L ), is( -1L ) );
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( 1L ), is( -1L ) );

        // [1,2,3]
        tracker.addCompletedTimeAsMilli( 3L );

        // [ ,2,3]
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( 2L ), is( 1L ) );

        // [ ,2,3,4]
        tracker.addCompletedTimeAsMilli( 4L );

        // [ ,2,3,4,5,6,7]
        tracker.addCompletedTimeAsMilli( 5L );
        tracker.addCompletedTimeAsMilli( 6L );
        tracker.addCompletedTimeAsMilli( 7L );

        // [ , , , ,5,6,7]
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( 5L ), is( 4L ) );

        // [ , , , ,5,6,7,8,9,10]
        tracker.addCompletedTimeAsMilli( 8L );
        tracker.addCompletedTimeAsMilli( 9L );
        tracker.addCompletedTimeAsMilli( 10L );

        // [ , , , ,5,6,7,8,9,10,11,14,15]
        tracker.addCompletedTimeAsMilli( 11L );
        tracker.addCompletedTimeAsMilli( 14L );
        tracker.addCompletedTimeAsMilli( 15L );

        // [ , , , , , , , , , , ,14,15]
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( 13L ), is( 11L ) );

        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( Long.MAX_VALUE ), is( 15L ) );
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( Long.MAX_VALUE ), is( -1L ) );
    }

    @Test
    public void shouldRemoveTimesCorrectlyWhenThereIsAreMultipleTimesThatAreAddedOutOfOrder_UsingTreeMultiSet()
            throws CompletionTimeException
    {
        shouldRemoveTimesCorrectlyWhenThereIsAreMultipleTimesThatAreAddedOutOfOrder(
                CompletionTimeStateManager.CompletedTimeTrackerImpl.createUsingTreeMultiSet() );
    }

    @Test
    public void shouldRemoveTimesCorrectlyWhenThereIsAreMultipleTimesThatAreAddedOutOfOrder_UsingList()
            throws CompletionTimeException
    {
        shouldRemoveTimesCorrectlyWhenThereIsAreMultipleTimesThatAreAddedOutOfOrder(
                CompletionTimeStateManager.CompletedTimeTrackerImpl.createUsingArrayList() );
    }

    private void shouldRemoveTimesCorrectlyWhenThereIsAreMultipleTimesThatAreAddedOutOfOrder(
            CompletionTimeStateManager.CompletedTimeTrackerImpl tracker ) throws CompletionTimeException
    {
        // Given
        // tracker

        // When/Then
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( 0L ), is( -1L ) );
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( Long.MAX_VALUE ), is( -1L ) );

        // [1]
        tracker.addCompletedTimeAsMilli( 1L );

        // [0,0,1,1]
        tracker.addCompletedTimeAsMilli( 0L );
        tracker.addCompletedTimeAsMilli( 0L );
        tracker.addCompletedTimeAsMilli( 1L );

        // [0,0,1,1,9,2,6]
        tracker.addCompletedTimeAsMilli( 9L );
        tracker.addCompletedTimeAsMilli( 2L );
        tracker.addCompletedTimeAsMilli( 6L );

        // [ , , , ,9, ,6]
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( 4L ), is( 2L ) );

        // [ , , , ,9, ,6,1,0,0,4]
        tracker.addCompletedTimeAsMilli( 1L );
        tracker.addCompletedTimeAsMilli( 0L );
        tracker.addCompletedTimeAsMilli( 0L );
        tracker.addCompletedTimeAsMilli( 4L );

        // [ , , , ,9, ,6, , , ,4]
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( 4L ), is( 1L ) );

        // [ , , , ,9, ,6, , , ,4,1,2,3,4,5,6,7,8,9]
        tracker.addCompletedTimeAsMilli( 1L );
        tracker.addCompletedTimeAsMilli( 2L );
        tracker.addCompletedTimeAsMilli( 3L );
        tracker.addCompletedTimeAsMilli( 4L );
        tracker.addCompletedTimeAsMilli( 5L );
        tracker.addCompletedTimeAsMilli( 6L );
        tracker.addCompletedTimeAsMilli( 7L );
        tracker.addCompletedTimeAsMilli( 8L );
        tracker.addCompletedTimeAsMilli( 9L );

        // [ , , , ,9, ,6, , , , , , , , , ,6,7,8,9]
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( 6L ), is( 5L ) );

        // [ , , , ,9, ,6, , , , , , , , , ,6,7,8,9,10]
        tracker.addCompletedTimeAsMilli( 10L );

        // [ , , , ,9, ,6, , , , , , , , , ,6,7,8,9,10]
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( 6L ), is( -1L ) );

        // [ , , , ,9, , , , , , , , , , , , , ,8,9,10]
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( 8L ), is( 7L ) );

        // [ , , , ,9, , , , , , , , , , , , , ,8,9,10,0]
        tracker.addCompletedTimeAsMilli( 0L );

        // [ , , , ,9, , , , , , , , , , , , , ,8,9,10,0,15]
        tracker.addCompletedTimeAsMilli( 15L );

        // [ , , , ,9, , , , , , , , , , , , , ,8,9,10, ,15]
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( 6L ), is( 0L ) );

        // [ , , , , , , , , , , , , , , , , , , , , , ,15]
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( 12L ), is( 10L ) );

        // [ , , , , , , , , , , , , , , , , , , , , , ,15]
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( 15L ), is( -1L ) );

        // [ , , , , , , , , , , , , , , , , , , , , , , ]
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( 16L ), is( 15L ) );

        // [ , , , , , , , , , , , , , , , , , , , , , , ]
        assertThat( tracker.removeTimesLowerThanAndReturnHighestRemoved( Long.MAX_VALUE ), is( -1L ) );
    }
}
