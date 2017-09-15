package com.ldbc.driver.runtime.coordination;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class CompletionTimeStateManagerTest
{
    @Test
    public void shouldReturnNullWhenNoEventsHaveBeenInitiatedOrCompleted() throws CompletionTimeException
    {
        // Given
        CompletionTimeStateManager completionTimeStateManager = new CompletionTimeStateManager();

        // When / Then
        // no events have been initiated or completed

        // IT []
        // CT []
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( -1L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );
    }

    @Test
    public void shouldReturnNullWhenNoOperationsHaveCompleted() throws CompletionTimeException
    {
        // Given
        CompletionTimeStateManager completionTimeStateManager = new CompletionTimeStateManager();

        // When / Then

        // IT [1,2,3]
        // CT [ , , ]
        completionTimeStateManager.submitInitiatedTime( 1000L );
        completionTimeStateManager.submitInitiatedTime( 2000L );
        completionTimeStateManager.submitInitiatedTime( 3000L );

        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 1000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );
    }

    @Test
    public void shouldReturnNullWhenEarliestInitiatedOperationHasNotCompleted() throws CompletionTimeException
    {
        // Given
        CompletionTimeStateManager completionTimeStateManager = new CompletionTimeStateManager();

        // When / Then

        // IT [1,2,3]
        // CT [ , , ]
        completionTimeStateManager.submitInitiatedTime( 1000L );
        completionTimeStateManager.submitInitiatedTime( 2000L );
        completionTimeStateManager.submitInitiatedTime( 3000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 1000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // IT [1, , ]
        // CT [ ,2,3]
        completionTimeStateManager.submitCompletedTime( 2000L );
        completionTimeStateManager.submitCompletedTime( 3000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 1000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );
    }

    @Test
    public void shouldAdvanceToNextUncompletedEventAsInitiatedEventsAreCompleted() throws CompletionTimeException
    {
        // Given
        CompletionTimeStateManager completionTimeStateManager = new CompletionTimeStateManager();

        // When / Then
        // IT [1,2,3]
        // CT [ , , ]
        completionTimeStateManager.submitInitiatedTime( 1000L );
        completionTimeStateManager.submitInitiatedTime( 2000L );
        completionTimeStateManager.submitInitiatedTime( 3000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 1000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // IT [ ,2, ]
        // CT [1, ,3]
        completionTimeStateManager.submitCompletedTime( 3000L );
        completionTimeStateManager.submitCompletedTime( 1000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 2000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 1000L ) );
    }

    @Test
    public void
    shouldAdvanceToNextUncompletedEventAsInitiatedEventsAreCompletedButOnlyIfNoUncompletedTimesExistAtSameTime()
            throws CompletionTimeException
    {
        // Given
        CompletionTimeStateManager completionTimeStateManager = new CompletionTimeStateManager();

        // When/Then

        // IT [1,1,2,3]
        // CT [ , , , ]
        completionTimeStateManager.submitInitiatedTime( 1000L );
        completionTimeStateManager.submitInitiatedTime( 1000L );
        completionTimeStateManager.submitInitiatedTime( 2000L );
        completionTimeStateManager.submitInitiatedTime( 3000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 1000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // IT [ ,1,2, ]
        // CT [1, , ,3]
        completionTimeStateManager.submitCompletedTime( 3000L );
        completionTimeStateManager.submitCompletedTime( 1000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 1000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // IT [ , ,2, ]
        // CT [1,1, ,3]
        completionTimeStateManager.submitCompletedTime( 1000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 2000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 1000L ) );

        // IT [ , , , ]
        // CT [1,1,2,3]
        completionTimeStateManager.submitCompletedTime( 2000L );
        // because we do not know if more Initiated Time == 3 will arrive
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 3000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // IT [ , , , ,3]
        // CT [1,1,2,3, ]
        completionTimeStateManager.submitInitiatedTime( 3000L );
        // another Initiated Time == 3 arrived
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 3000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // IT [ , , , , ]
        // CT [1,1,2,3,3]
        completionTimeStateManager.submitCompletedTime( 3000L );
        // because we do not know if more Initiated Time == 3 will arrive
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 3000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // IT [ , , , , ,4]
        // CT [1,1,2,3,3, ]
        completionTimeStateManager.submitInitiatedTime( 4000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 4000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 3000L ) );
    }

    @Test
    public void shouldAllowForSubmittedInitiatedTimeToEqualCurrentCompletionTimeButNotBeLower()
            throws CompletionTimeException
    {
        // Given
        CompletionTimeStateManager completionTimeStateManager = new CompletionTimeStateManager();

        // When/Then
        // IT [1,2,3]
        // CT [ , , ]
        completionTimeStateManager.submitInitiatedTime( 1000L );
        completionTimeStateManager.submitInitiatedTime( 2000L );
        completionTimeStateManager.submitInitiatedTime( 3000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 1000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // IT [ ,2,3]
        // CT [1, , ]
        completionTimeStateManager.submitCompletedTime( 1000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 2000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 1000L ) );

        //apply initiated time equal to highest submitted initiated time AND equal to CT <- should be ok
        // IT [ ,2,3,3]
        // CT [1, , , ]
        completionTimeStateManager.submitInitiatedTime( 3000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 2000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 1000L ) );

        // IT [ ,2, ,3]
        // CT [1, ,3, ]
        completionTimeStateManager.submitCompletedTime( 3000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 2000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 1000L ) );

        // IT [ ,2, ,3]
        // CT [1, ,3, ]
        boolean exceptionThrown = false;
        try
        {
            completionTimeStateManager.submitInitiatedTime( 1000L );
        }
        catch ( CompletionTimeException e )
        {
            exceptionThrown = true;
        }
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 2000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 1000L ) );
        assertThat( exceptionThrown, is( true ) );
    }

    @Test
    public void
    shouldAdvanceToNextUncompletedEventUntilAllInitiatedEventsAreCompletedWhenCompletedTimesComeInOutOfOrder1()
            throws CompletionTimeException
    {
        // Given
        CompletionTimeStateManager completionTimeStateManager = new CompletionTimeStateManager();

        // When/Then
        // IT [1,2,3,4]
        // CT [ , , , ]
        completionTimeStateManager.submitInitiatedTime( 1000L );
        completionTimeStateManager.submitInitiatedTime( 2000L );
        completionTimeStateManager.submitInitiatedTime( 3000L );
        completionTimeStateManager.submitInitiatedTime( 4000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 1000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // IT [1,2, , ]
        // CT [ , ,3,4]
        completionTimeStateManager.submitCompletedTime( 4000L );
        completionTimeStateManager.submitCompletedTime( 3000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 1000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // IT [ ,2, , ]
        // CT [1, ,3,4]
        completionTimeStateManager.submitCompletedTime( 1000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 2000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 1000L ) );

        // IT [ , , , ]
        // CT [1,2,3,4]
        completionTimeStateManager.submitCompletedTime( 2000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 4000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 3000L ) );
    }

    @Test
    public void
    shouldAdvanceToNextUncompletedEventUntilAllInitiatedEventsAreCompletedWhenCompletedTimesComeInOutOfOrder2()
            throws CompletionTimeException
    {
        // Given
        CompletionTimeStateManager completionTimeStateManager = new CompletionTimeStateManager();

        // When/Then
        // IT [1,2,3,4]
        // CT [ , , , ]
        completionTimeStateManager.submitInitiatedTime( 1000L );
        completionTimeStateManager.submitInitiatedTime( 2000L );
        completionTimeStateManager.submitInitiatedTime( 3000L );
        completionTimeStateManager.submitInitiatedTime( 4000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 1000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // IT [1,2, , ]
        // CT [ , ,3,4]
        completionTimeStateManager.submitCompletedTime( 3000L );
        completionTimeStateManager.submitCompletedTime( 4000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 1000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // IT [ ,2, , ]
        // CT [1, ,3,4]
        completionTimeStateManager.submitCompletedTime( 1000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 2000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 1000L ) );

        // IT [ , , , ]
        // CT [1,2,3,4]
        completionTimeStateManager.submitCompletedTime( 2000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 4000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 3000L ) );
    }

    @Test
    public void
    shouldAdvanceToNextUncompletedEventUntilAllInitiatedEventsAreCompletedWhenCompletedTimesComeInOutOfOrder3()
            throws CompletionTimeException
    {
        // Given
        CompletionTimeStateManager completionTimeStateManager = new CompletionTimeStateManager();

        // When/Then
        // IT [1,2,3,4]
        // CT [ , , , ]
        completionTimeStateManager.submitInitiatedTime( 1000L );
        completionTimeStateManager.submitInitiatedTime( 2000L );
        completionTimeStateManager.submitInitiatedTime( 3000L );
        completionTimeStateManager.submitInitiatedTime( 4000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 1000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // IT [1,2, , ]
        // CT [ , ,3,4]
        completionTimeStateManager.submitCompletedTime( 4000L );
        completionTimeStateManager.submitCompletedTime( 3000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 1000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // IT [1, , , ]
        // CT [ ,2,3,4]
        completionTimeStateManager.submitCompletedTime( 2000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 1000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // IT [ , , , ]
        // CT [1,2,3,4]
        completionTimeStateManager.submitCompletedTime( 1000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 4000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 3000L ) );
    }

    @Test
    public void shouldReturnLatestInitiatedEventTimeWhenAllEventsHaveCompleted() throws CompletionTimeException
    {
        // Given
        CompletionTimeStateManager completionTimeStateManager = new CompletionTimeStateManager();

        // When/Then

        // IT [1,2,3]
        // CT [ , , ]
        completionTimeStateManager.submitInitiatedTime( 1000L );
        completionTimeStateManager.submitInitiatedTime( 2000L );
        completionTimeStateManager.submitInitiatedTime( 3000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 1000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // IT [ , , ]
        // CT [1,2,3]
        completionTimeStateManager.submitCompletedTime( 1000L );
        completionTimeStateManager.submitCompletedTime( 2000L );
        completionTimeStateManager.submitCompletedTime( 3000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 3000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );
    }

    @Test
    public void shouldThrowExceptionWhenEventCompletesThatHasNoMatchingInitiatedEntry() throws CompletionTimeException
    {
        // Given
        CompletionTimeStateManager completionTimeStateManager = new CompletionTimeStateManager();

        // When/Then

        // IT [1,2,3]
        // CT [ , , ]
        completionTimeStateManager.submitInitiatedTime( 1000L );
        completionTimeStateManager.submitInitiatedTime( 2000L );
        completionTimeStateManager.submitInitiatedTime( 3000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 1000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // IT [ ,2,3]
        // CT [1, , ]
        completionTimeStateManager.submitCompletedTime( 1000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 2000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 1000L ) );

        boolean exceptionThrown = false;
        try
        {
            // only one entry with DueTime=1 exists, this should throw exception
            completionTimeStateManager.submitCompletedTime( 1000L );
        }
        catch ( CompletionTimeException e )
        {
            exceptionThrown = true;
        }
        assertThat( exceptionThrown, is( true ) );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 2000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 1000L ) );
    }

    @Test
    public void shouldReturnLatestTimeBehindWhichThereAreNoUncompletedITEvents() throws CompletionTimeException
    {
        // Given
        CompletionTimeStateManager completionTimeStateManager = new CompletionTimeStateManager();

        // When/Then
        // IT [1]
        // CT []
        completionTimeStateManager.submitInitiatedTime( 1000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 1000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );
        // IT [ ]
        // CT [1]
        completionTimeStateManager.submitCompletedTime( 1000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 1000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // IT [ ,2]
        // CT [1, ]
        completionTimeStateManager.submitInitiatedTime( 2000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 2000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 1000L ) );
        // IT [ , ]
        // CT [1,2]
        completionTimeStateManager.submitCompletedTime( 2000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 2000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 1000L ) );

        // IT [ , ,3]
        // CT [1,2, ]
        completionTimeStateManager.submitInitiatedTime( 3000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 3000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // IT [ , ,3,4]
        // CT [1,2, , ]
        completionTimeStateManager.submitInitiatedTime( 4000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 3000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // IT [ , ,3,4,5]
        // CT [1,2, , , ]
        completionTimeStateManager.submitInitiatedTime( 5000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 3000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // IT [ , ,3,4, ]
        // CT [1,2, , ,5]
        completionTimeStateManager.submitCompletedTime( 5000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 3000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // IT [ , ,3,4, ,5]
        // CT [1,2, , ,5, ]
        completionTimeStateManager.submitInitiatedTime( 5000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 3000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // IT [ , ,3,4, , ]
        // CT [1,2, , ,5,5]
        completionTimeStateManager.submitCompletedTime( 5000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 3000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // IT [ , ,3,4, , ,5]
        // CT [1,2, , ,5,5, ]
        completionTimeStateManager.submitInitiatedTime( 5000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 3000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // IT [ , ,3,4, , , ]
        // CT [1,2, , ,5,5,5]
        completionTimeStateManager.submitCompletedTime( 5000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 3000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // IT [ , , ,4, , , ]
        // CT [1,2,3, ,5,5,5]
        completionTimeStateManager.submitCompletedTime( 3000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 4000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 3000L ) );

        // IT [ , , , , , , ]
        // CT [1,2,3,4,5,5,5]
        completionTimeStateManager.submitCompletedTime( 4000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 5000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 4000L ) );

        // IT [ , , , , , , ,6]
        // CT [1,2,3,4,5,5,5, ]
        completionTimeStateManager.submitInitiatedTime( 6000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 6000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 5000L ) );
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNotMatchingCTEvenWhenMultipleEventsHaveSameInitiatedTime()
            throws CompletionTimeException
    {
        // Given
        CompletionTimeStateManager completionTimeStateManager = new CompletionTimeStateManager();

        // When/Then
        // IT [1]
        // CT [ ]
        completionTimeStateManager.submitInitiatedTime( 1000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 1000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // IT [ ]
        // CT [1]
        completionTimeStateManager.submitCompletedTime( 1000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 1000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // IT [ ,2]
        // CT [1, ]
        completionTimeStateManager.submitInitiatedTime( 2000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 2000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 1000L ) );

        // IT [ , ]
        // CT [1,2]
        completionTimeStateManager.submitCompletedTime( 2000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 2000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 1000L ) );

        // IT [ , ,3]
        // CT [1,2, ]
        completionTimeStateManager.submitInitiatedTime( 3000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 3000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // IT [ , ,3,3]
        // CT [1,2, , ]
        completionTimeStateManager.submitInitiatedTime( 3000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 3000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // IT [ , ,3,3,4]
        // CT [1,2, , , ]
        completionTimeStateManager.submitInitiatedTime( 4000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 3000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // IT [ , , ,3,4]
        // CT [1,2,3, , ]
        completionTimeStateManager.submitCompletedTime( 3000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 3000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // IT [ , , ,3, ]
        // CT [1,2,3, ,4]
        completionTimeStateManager.submitCompletedTime( 4000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 3000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // IT [ , , , , ]
        // CT [1,2,3,3,4]
        completionTimeStateManager.submitCompletedTime( 3000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 4000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 3000L ) );

        // IT [ , , , , ,5]
        // CT [1,2,3,3,4, ]
        completionTimeStateManager.submitInitiatedTime( 5000L );
        assertThat( completionTimeStateManager.lastKnownLowestInitiatedTimeAsMilli(), is( 5000L ) );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 4000L ) );
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTV1() throws CompletionTimeException
    {
        // Given
        CompletionTimeStateManager completionTimeStateManager = new CompletionTimeStateManager();

        // When/Then
        // initiated [1]
        // completed []
        completionTimeStateManager.submitInitiatedTime( 1000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // initiated [1]
        // completed [1]
        completionTimeStateManager.submitCompletedTime( 1000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // initiated [1,2]
        // completed [1]
        completionTimeStateManager.submitInitiatedTime( 2000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 1000L ) );

        // initiated [1,2]
        // completed [1,2]
        completionTimeStateManager.submitCompletedTime( 2000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 1000L ) );

        // initiated [1,2,3]
        // completed [1,2]
        completionTimeStateManager.submitInitiatedTime( 3000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4]
        // completed [1,2]
        completionTimeStateManager.submitInitiatedTime( 4000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2]
        completionTimeStateManager.submitInitiatedTime( 5000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2, , ,5]
        completionTimeStateManager.submitCompletedTime( 5000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, , , , ,  ]
        completionTimeStateManager.submitInitiatedTime( 6000L );
        completionTimeStateManager.submitInitiatedTime( 7000L );
        completionTimeStateManager.submitInitiatedTime( 8000L );
        completionTimeStateManager.submitInitiatedTime( 9000L );
        completionTimeStateManager.submitInitiatedTime( 10000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, , ,8, ,  ]
        completionTimeStateManager.submitCompletedTime( 8000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, ,7,8, ,  ]
        completionTimeStateManager.submitCompletedTime( 7000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, ,7,8,9,  ]
        completionTimeStateManager.submitCompletedTime( 9000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, ,4,5, ,7,8,9,  ]
        completionTimeStateManager.submitCompletedTime( 4000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5, ,7,8,9,  ]
        completionTimeStateManager.submitCompletedTime( 3000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 5000L ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5,6,7,8,9,  ]
        completionTimeStateManager.submitCompletedTime( 6000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 9000L ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5,6,7,8,9,10]
        completionTimeStateManager.submitCompletedTime( 10000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 9000L ) );

        // initiated [1,2,3,4,5,6,7,8,9,10,11]
        // completed [1,2,3,4,5,6,7,8,9,10,  ]
        completionTimeStateManager.submitInitiatedTime( 11000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 10000L ) );
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTV2() throws CompletionTimeException
    {
        // Given
        CompletionTimeStateManager completionTimeStateManager = new CompletionTimeStateManager();

        // When/Then
        // initiated [1]
        // completed [1]
        completionTimeStateManager.submitInitiatedTime( 1000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );
        completionTimeStateManager.submitCompletedTime( 1000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // initiated [1,2]
        // completed [1,2]
        completionTimeStateManager.submitInitiatedTime( 2000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 1000L ) );
        completionTimeStateManager.submitCompletedTime( 2000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 1000L ) );

        // initiated [1,2,3]
        // completed [1,2, ]
        completionTimeStateManager.submitInitiatedTime( 3000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,3]
        // completed [1,2, , ]
        completionTimeStateManager.submitInitiatedTime( 3000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,3,3]
        // completed [1,2, , , ]
        completionTimeStateManager.submitInitiatedTime( 3000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,3,3,4]
        // completed [1,2, , , , ]
        completionTimeStateManager.submitInitiatedTime( 4000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,3,3,4,5]
        // completed [1,2, , , , , ]
        completionTimeStateManager.submitInitiatedTime( 5000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, , , , , , ]
        completionTimeStateManager.submitInitiatedTime( 6000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, , , , ,5, ]
        completionTimeStateManager.submitCompletedTime( 5000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, , ,3, ,5, ]
        completionTimeStateManager.submitCompletedTime( 3000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, ,3,3, ,5, ]
        completionTimeStateManager.submitCompletedTime( 3000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2,3,3,3, ,5, ]
        completionTimeStateManager.submitCompletedTime( 3000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 3000L ) );

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2,3,3,3,4,5, ]
        completionTimeStateManager.submitCompletedTime( 4000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 5000L ) );

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2,3,3,3,4,5,6]
        completionTimeStateManager.submitCompletedTime( 6000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 5000L ) );
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTV3() throws CompletionTimeException
    {
        // Given
        CompletionTimeStateManager completionTimeStateManager = new CompletionTimeStateManager();

        // When/Then
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // initiated [1]
        // completed [1]
        completionTimeStateManager.submitInitiatedTime( 1000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        completionTimeStateManager.submitCompletedTime( 1000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // initiated [1]
        // completed [1]
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // initiated [1]
        // completed [1]
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( -1L ) );

        // initiated [1,2]
        // completed [1]
        completionTimeStateManager.submitInitiatedTime( 2000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 1000L ) );

        // initiated [1,2,3]
        // completed [1]
        completionTimeStateManager.submitInitiatedTime( 3000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 1000L ) );

        // initiated [1,2,3,4]
        // completed [1]
        completionTimeStateManager.submitInitiatedTime( 4000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 1000L ) );

        // initiated [1,2,3,4]
        // completed [1, , ,4]
        completionTimeStateManager.submitCompletedTime( 4000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 1000L ) );

        // initiated [1,2,3,4]
        // completed [1,2, ,4]
        completionTimeStateManager.submitCompletedTime( 2000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2, ,4, ]
        completionTimeStateManager.submitInitiatedTime( 5000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2, ,4, ]
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4, ]
        completionTimeStateManager.submitCompletedTime( 3000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 4000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4,5]
        completionTimeStateManager.submitCompletedTime( 5000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 4000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4,5]
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 4000L ) );

        // initiated [1,2,3,4,5,6]
        // completed [1,2,3,4,5]
        completionTimeStateManager.submitInitiatedTime( 6000L );
        assertThat( completionTimeStateManager.completionTimeAsMilli(), is( 5000L ) );
    }
}
