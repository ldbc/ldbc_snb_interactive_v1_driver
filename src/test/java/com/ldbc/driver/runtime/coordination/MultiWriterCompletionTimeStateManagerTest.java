package com.ldbc.driver.runtime.coordination;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple2;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MultiWriterCompletionTimeStateManagerTest
{
    /**
     * ****************************************************
     * ****************************************************
     * ****************************************************
     * Tests that are known to work with CompletionTimeStateManager
     * ****************************************************
     * ****************************************************
     * ****************************************************
     */

    @Test
    public void shouldReturnNullWhenNoEventsHaveBeenInitiatedOrCompletedWithOneWriter() throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer = completionTimeStateManager.newCompletionTimeWriter();

        // When
        // no events have been initiated or completed

        // Then
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( -1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );
    }

    @Test
    public void shouldReturnNullWhenNoOperationsHaveCompletedWithOneWriter() throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer = completionTimeStateManager.newCompletionTimeWriter();

        // When

        writer.submitInitiatedTime( 1 );
        writer.submitInitiatedTime( 2 );
        writer.submitInitiatedTime( 3 );

        // Then
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );
    }

    @Test
    public void shouldReturnNullWhenEarliestInitiatedOperationHasNotCompletedWithOneWriter()
            throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer = completionTimeStateManager.newCompletionTimeWriter();

        // When/Then

        writer.submitInitiatedTime( 1 );
        writer.submitInitiatedTime( 2 );
        writer.submitInitiatedTime( 3 );

        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        writer.submitCompletedTime( 2 );
        writer.submitCompletedTime( 3 );

        // Then
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );
    }

    @Test
    public void shouldAdvanceToNextUncompletedEventAsInitiatedEventsAreCompletedWithOneWriter()
            throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer = completionTimeStateManager.newCompletionTimeWriter();

        // When/Then
        writer.submitInitiatedTime( 1 );
        writer.submitInitiatedTime( 2 );
        writer.submitInitiatedTime( 3 );

        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        writer.submitCompletedTime( 3 );
        writer.submitCompletedTime( 1 );

        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 2L ) );
        assertThat( reader.completionTimeAsMilli(), is( 1L ) );
    }

    @Test
    public void
    shouldAdvanceToNextUncompletedEventAsInitiatedEventsAreCompletedButOnlyIfNoUncompletedTimesExistAtSameTimeWithOneWriter()
            throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer = completionTimeStateManager.newCompletionTimeWriter();

        // When/Then

        // IT [1,1,2,3]
        // CT []
        writer.submitInitiatedTime( 1 );
        writer.submitInitiatedTime( 1 );
        writer.submitInitiatedTime( 2 );
        writer.submitInitiatedTime( 3 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT [ ,1,2, ]
        // CT [1, , ,3]
        writer.submitCompletedTime( 3 );
        writer.submitCompletedTime( 1 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT [ , ,2, ]
        // CT [1,1, ,3]
        writer.submitCompletedTime( 1 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 2L ) );
        assertThat( reader.completionTimeAsMilli(), is( 1L ) );

        // IT [ , , , ]
        // CT [1,1,2,3]
        writer.submitCompletedTime( 2 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        // because we do not know if more Initiated Time == 3 will arrive
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT [ , , , ,3]
        // CT [1,1,2,3, ]
        writer.submitInitiatedTime( 3 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        // another Initiated Time == 3 arrived
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT [ , , , , ]
        // CT [1,1,2,3,3]
        writer.submitCompletedTime( 3 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        // because we do not know if more Initiated Time == 3 will arrive
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT [ , , , , ,4]
        // CT [1,1,2,3,3, ]
        writer.submitInitiatedTime( 4 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 4L ) );
        assertThat( reader.completionTimeAsMilli(), is( 3L ) );
    }

    @Test
    public void shouldAllowForSubmittedInitiatedTimeToEqualCurrentCompletionTimeButNotBeLowerWithOneWriter()
            throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer = completionTimeStateManager.newCompletionTimeWriter();

        // When/Then
        // IT [1,2,3]
        // CT [ , , ]
        writer.submitInitiatedTime( 1 );
        writer.submitInitiatedTime( 2 );
        writer.submitInitiatedTime( 3 );

        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT [ ,2,3]
        // CT [1, , ]
        writer.submitCompletedTime( 1 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 2L ) );
        assertThat( reader.completionTimeAsMilli(), is( 1L ) );

        //apply initiated time equal to highest submitted initiated time AND equal to CT <- should be ok
        // IT [ ,2,3,3]
        // CT [1, , , ]
        writer.submitInitiatedTime( 3 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 2L ) );
        assertThat( reader.completionTimeAsMilli(), is( 1L ) );

        // IT [ ,2, ,3]
        // CT [1, ,3, ]
        writer.submitCompletedTime( 3 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 2L ) );
        assertThat( reader.completionTimeAsMilli(), is( 1L ) );

        // IT [ ,2, ,3]
        // CT [1, ,3, ]
        boolean exceptionThrown = false;
        try
        {
            writer.submitInitiatedTime( 1 );
        }
        catch ( CompletionTimeException e )
        {
            exceptionThrown = true;
        }
        assertThat( exceptionThrown, is( true ) );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 2L ) );
        assertThat( reader.completionTimeAsMilli(), is( 1L ) );
    }

    @Test
    public void
    shouldAdvanceToNextUncompletedEventUntilAllInitiatedEventsAreCompletedWhenCompletedTimesComeInOutOfOrder1WithOneWriter()
            throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer = completionTimeStateManager.newCompletionTimeWriter();

        // When/Then
        // IT [1,2,3,4]
        // CT [ , , , ]
        writer.submitInitiatedTime( 1 );
        writer.submitInitiatedTime( 2 );
        writer.submitInitiatedTime( 3 );
        writer.submitInitiatedTime( 4 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT [1,2, , ]
        // CT [ , ,3,4]
        writer.submitCompletedTime( 4 );
        writer.submitCompletedTime( 3 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT [ ,2, , ]
        // CT [1, ,3,4]
        writer.submitCompletedTime( 1 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 2L ) );
        assertThat( reader.completionTimeAsMilli(), is( 1L ) );

        // IT [ , , , ]
        // CT [1,2,3,4]
        writer.submitCompletedTime( 2 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 4L ) );
        assertThat( reader.completionTimeAsMilli(), is( 3L ) );
    }

    @Test
    public void
    shouldAdvanceToNextUncompletedEventUntilAllInitiatedEventsAreCompletedWhenCompletedTimesComeInOutOfOrder2WithOneWriter()
            throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer = completionTimeStateManager.newCompletionTimeWriter();

        // When/Then

        // IT [1,2,3,4]
        // CT [ , , , ]
        writer.submitInitiatedTime( 1 );
        writer.submitInitiatedTime( 2 );
        writer.submitInitiatedTime( 3 );
        writer.submitInitiatedTime( 4 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT [1,2, , ]
        // CT [ , ,3,4]
        writer.submitCompletedTime( 3 );
        writer.submitCompletedTime( 4 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT [ ,2, , ]
        // CT [1, ,3,4]
        writer.submitCompletedTime( 1 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 2L ) );
        assertThat( reader.completionTimeAsMilli(), is( 1L ) );

        // IT [ , , , ]
        // CT [1,2,3,4]
        writer.submitCompletedTime( 2 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 4L ) );
        assertThat( reader.completionTimeAsMilli(), is( 3L ) );
    }

    @Test
    public void
    shouldAdvanceToNextUncompletedEventUntilAllInitiatedEventsAreCompletedWhenCompletedTimesComeInOutOfOrder3WithOneWriter()
            throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer = completionTimeStateManager.newCompletionTimeWriter();

        // When/Then
        // IT [1,2,3,4]
        // CT [ , , , ]
        writer.submitInitiatedTime( 1 );
        writer.submitInitiatedTime( 2 );
        writer.submitInitiatedTime( 3 );
        writer.submitInitiatedTime( 4 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT [1,2, , ]
        // CT [ , ,3,4]
        writer.submitCompletedTime( 4 );
        writer.submitCompletedTime( 3 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT [1, , , ]
        // CT [ ,2,3,4]
        writer.submitCompletedTime( 2 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT [ , , , ]
        // CT [1,2,3,4]
        writer.submitCompletedTime( 1 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 4L ) );
        assertThat( reader.completionTimeAsMilli(), is( 3L ) );
    }

    @Test
    public void shouldReturnLatestInitiatedEventTimeWhenAllEventsHaveCompletedWithOneWriter()
            throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer = completionTimeStateManager.newCompletionTimeWriter();

        // When/Then

        // IT [1,2,3]
        // CT [ , , ]
        writer.submitInitiatedTime( 1 );
        writer.submitInitiatedTime( 2 );
        writer.submitInitiatedTime( 3 );

        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT [ , , ]
        // CT [1,2,3]
        writer.submitCompletedTime( 1 );
        writer.submitCompletedTime( 2 );
        writer.submitCompletedTime( 3 );

        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );
    }

    @Test
    public void shouldThrowExceptionWhenEventCompletesThatHasNoMatchingInitiatedEntryWithOneWriter()
            throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer = completionTimeStateManager.newCompletionTimeWriter();

        // When/Then

        // IT [1,2,3]
        // CT [ , , ]
        writer.submitInitiatedTime( 1 );
        writer.submitInitiatedTime( 2 );
        writer.submitInitiatedTime( 3 );

        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT [ ,2,3]
        // CT [1, , ]
        writer.submitCompletedTime( 1 );

        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 2L ) );
        assertThat( reader.completionTimeAsMilli(), is( 1L ) );

        boolean exceptionThrown = false;
        try
        {
            // only one entry with DueTime=1 exists, this should throw exception
            writer.submitCompletedTime( 1 );
        }
        catch ( CompletionTimeException e )
        {
            exceptionThrown = true;
        }
        assertThat( exceptionThrown, is( true ) );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 2L ) );
        assertThat( reader.completionTimeAsMilli(), is( 1L ) );
    }

    @Test
    public void shouldReturnLatestTimeBehindWhichThereAreNoUncompletedITEventsWithOneWriter()
            throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer = completionTimeStateManager.newCompletionTimeWriter();

        // When/Then
        // IT [1]
        // CT [ ]
        writer.submitInitiatedTime( 1 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );
        // IT [ ]
        // CT [1]
        writer.submitCompletedTime( 1 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT [ ,2]
        // CT [1, ]
        writer.submitInitiatedTime( 2 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 2L ) );
        assertThat( reader.completionTimeAsMilli(), is( 1L ) );
        // IT [ , ]
        // CT [1,2]
        writer.submitCompletedTime( 2 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 2L ) );
        assertThat( reader.completionTimeAsMilli(), is( 1L ) );

        // IT [ , ,3]
        // CT [1,2, ]
        writer.submitInitiatedTime( 3 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT [ , ,3,4]
        // CT [1,2, , ]
        writer.submitInitiatedTime( 4 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT [ , ,3,4,5]
        // CT [1,2, , , ]
        writer.submitInitiatedTime( 5 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );
        // IT [ , ,3,4, ]
        // CT [1,2, , ,5]
        writer.submitCompletedTime( 5 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT [ , ,3,4, ,5]
        // CT [1,2, , ,5, ]
        writer.submitInitiatedTime( 5 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );
        // IT [ , ,3,4, , ]
        // CT [1,2, , ,5,5]
        writer.submitCompletedTime( 5 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT [ , ,3,4, , ,5]
        // CT [1,2, , ,5,5, ]
        writer.submitInitiatedTime( 5 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );
        // IT [ , ,3,4, , , ]
        // CT [1,2, , ,5,5,5]
        writer.submitCompletedTime( 5 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT [ , , ,4, , , ]
        // CT [1,2,3, ,5,5,5]
        writer.submitCompletedTime( 3 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 4L ) );
        assertThat( reader.completionTimeAsMilli(), is( 3L ) );

        // IT [ , , , , , , ]
        // CT [1,2,3,4,5,5,5]
        writer.submitCompletedTime( 4 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 5L ) );
        assertThat( reader.completionTimeAsMilli(), is( 4L ) );

        // IT [ , , , , , , ,6]
        // CT [1,2,3,4,5,5,5, ]
        writer.submitInitiatedTime( 6 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 6L ) );
        assertThat( reader.completionTimeAsMilli(), is( 5L ) );
    }

    @Test
    public void
    shouldReturnTimeOfEarliestITThatHasHadNotMatchingCTEvenWhenMultipleEventsHaveSameInitiatedTimeWithOneWriter()
            throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer = completionTimeStateManager.newCompletionTimeWriter();

        // When/Then
        // IT [1]
        // CT [ ]
        writer.submitInitiatedTime( 1 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );
        // IT [ ]
        // CT [1]
        writer.submitCompletedTime( 1 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT [ ,2]
        // CT [1, ]
        writer.submitInitiatedTime( 2 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 2L ) );
        assertThat( reader.completionTimeAsMilli(), is( 1L ) );
        // IT [ , ]
        // CT [1,2]
        writer.submitCompletedTime( 2 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 2L ) );
        assertThat( reader.completionTimeAsMilli(), is( 1L ) );

        // IT [ , ,3]
        // CT [1,2, ]
        writer.submitInitiatedTime( 3 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT [ , ,3,3]
        // CT [1,2, , ]
        writer.submitInitiatedTime( 3 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT [ , ,3,3,4]
        // CT [1,2, , , ]
        writer.submitInitiatedTime( 4 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT [ , , ,3,4]
        // CT [1,2,3, , ]
        writer.submitCompletedTime( 3 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT [ , , ,3, ]
        // CT [1,2,3, ,4]
        writer.submitCompletedTime( 4 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT [ , , , , ]
        // CT [1,2,3,3,4]
        writer.submitCompletedTime( 3 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 4L ) );
        assertThat( reader.completionTimeAsMilli(), is( 3L ) );

        // IT [ , , , , ,5]
        // CT [1,2,3,3,4, ]
        writer.submitInitiatedTime( 5 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 5L ) );
        assertThat( reader.completionTimeAsMilli(), is( 4L ) );
    }

    /**
     * **************************************************
     * ****************************************************
     * ****************************************************
     * Tests with multiple writers, but no concurrency
     * ****************************************************
     * ****************************************************
     * ****************************************************
     */

    @Test
    public void shouldReturnNullWhenNoWriters() throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();

        // When
        CompletionTimeReader reader = completionTimeStateManager;

        // Then
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( -1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );
    }

    @Test
    public void shouldReturnNullWhenOneWriterAndNoInitiatedTimesAndNoCompletedTimes() throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();

        // When
        CompletionTimeReader reader = completionTimeStateManager;

        // Then
        // IT(1) []
        // CT(1) []
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( -1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );
    }

    @Test
    public void shouldReturnNullWhenMultipleWritersAndNoInitiatedTimesAndNoCompletedTimes()
            throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();

        // When
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer1 = completionTimeStateManager.newCompletionTimeWriter();
        CompletionTimeWriter writer2 = completionTimeStateManager.newCompletionTimeWriter();
        CompletionTimeWriter writer3 = completionTimeStateManager.newCompletionTimeWriter();

        // Then
        // IT(1) []
        // CT(1) []
        // IT(2) []
        // CT(2) []
        // IT(3) []
        // CT(3) []
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( -1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );
    }

    @Test
    public void shouldReturnNullWhenOneWriterAndOneInitiatedTimeAndNoCompletedTimes() throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer = completionTimeStateManager.newCompletionTimeWriter();

        // When
        writer.submitInitiatedTime( 1 );

        // Then
        // IT(1) []
        // CT(1) []
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );
    }

    @Test
    public void shouldReturnNullWhenOneWriterAndMultipleInitiatedTimeAndNoCompletedTimes()
            throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer = completionTimeStateManager.newCompletionTimeWriter();

        // When/Then
        // IT(1) [1,2,3]
        // CT(1) [ , , ]
        writer.submitInitiatedTime( 1 );
        writer.submitInitiatedTime( 2 );
        writer.submitInitiatedTime( 3 );

        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );
    }

    @Test
    public void shouldReturnNullWhenMultipleWritersAndOneInitiatedTimeAndNoCompletedTimes()
            throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer1 = completionTimeStateManager.newCompletionTimeWriter();
        CompletionTimeWriter writer2 = completionTimeStateManager.newCompletionTimeWriter();

        // When/Then
        // IT(1) [1]
        // CT(1) [ ]
        // IT(2) []
        // CT(2) []
        writer1.submitInitiatedTime( 1 );

        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( -1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );
    }

    @Test
    public void shouldReturnNullWhenMultipleWritersAndMultipleInitiatedTimeAndNoCompletedTimes()
            throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer1 = completionTimeStateManager.newCompletionTimeWriter();
        CompletionTimeWriter writer2 = completionTimeStateManager.newCompletionTimeWriter();

        // When/Then
        // IT(1) [1]
        // CT(1) [ ]
        // IT(2) [1]
        // CT(2) [ ]
        writer1.submitInitiatedTime( 1 );
        writer2.submitInitiatedTime( 1 );

        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );
    }

    @Test
    public void shouldReturnNullWhenMultipleWritersAndMultipleInitiatedTimesPerWriterAndNoCompletedTimes()
            throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer1 = completionTimeStateManager.newCompletionTimeWriter();
        CompletionTimeWriter writer2 = completionTimeStateManager.newCompletionTimeWriter();

        // When/Then
        // IT(1) [1,2,3]
        // CT(1) [ , , ]
        // IT(2) []
        // CT(2) []
        writer1.submitInitiatedTime( 1 );
        writer1.submitInitiatedTime( 2 );
        writer1.submitInitiatedTime( 3 );

        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( -1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT(1) [1,2,3]
        // CT(1) [ , , ]
        // IT(2) [1,2,3]
        // CT(2) [ , , ]
        writer2.submitInitiatedTime( 1 );
        writer2.submitInitiatedTime( 2 );
        writer2.submitInitiatedTime( 3 );

        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );
    }

    @Test
    public void shouldReturnSubmittedTimeWhenOneWriterAndOneInitiatedTimeAndOneCompletedTime()
            throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer1 = completionTimeStateManager.newCompletionTimeWriter();

        // When
        // IT(1) [ ]
        // CT(1) [1]
        writer1.submitInitiatedTime( 1 );
        writer1.submitCompletedTime( 1 );

        // Then
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );
    }

    @Test
    public void shouldReturnNullWhenMultipleWritersAndOneWriterHasInitiatedTimeAndCompletedTimeButOtherHasNeither()
            throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer1 = completionTimeStateManager.newCompletionTimeWriter();
        CompletionTimeWriter writer2 = completionTimeStateManager.newCompletionTimeWriter();

        // When
        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) []
        // CT(2) []
        writer1.submitInitiatedTime( 1 );
        writer1.submitCompletedTime( 1 );

        // Then
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( -1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );
    }

    @Test
    public void shouldReturnSubmittedTimeWhenMultipleWritersAndOneInitiatedTimeEachAndOneCompletedTimeEach()
            throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer1 = completionTimeStateManager.newCompletionTimeWriter();
        CompletionTimeWriter writer2 = completionTimeStateManager.newCompletionTimeWriter();

        // When/Then
        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) []
        // CT(2) []
        writer1.submitInitiatedTime( 1 );
        writer1.submitCompletedTime( 1 );

        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( -1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) []
        // CT(2) [1]
        writer2.submitInitiatedTime( 1 );
        writer2.submitCompletedTime( 1 );

        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );
    }

    @Test
    public void shouldReturnMinTimeWhenMultipleWritersAndOneInitiatedTimeEachAndOneCompletedTimeEach()
            throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer1 = completionTimeStateManager.newCompletionTimeWriter();
        CompletionTimeWriter writer2 = completionTimeStateManager.newCompletionTimeWriter();

        // When/Then
        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) []
        // CT(2) []
        writer1.submitInitiatedTime( 1 );
        writer1.submitCompletedTime( 1 );

        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( -1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) []
        // CT(2) [2]
        writer2.submitInitiatedTime( 2 );
        writer2.submitCompletedTime( 2 );

        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );
    }

    @Test
    public void shouldPassComplexTwoWriterScenario1() throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer1 = completionTimeStateManager.newCompletionTimeWriter();
        CompletionTimeWriter writer2 = completionTimeStateManager.newCompletionTimeWriter();

        // When / Then

        // IT(1) []
        // CT(1) []
        // IT(2) []
        // CT(2) []
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( -1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) []
        // CT(2) []
        writer1.submitInitiatedTime( 1 );
        writer1.submitCompletedTime( 1 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( -1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT(1) [ ,2]
        // CT(1) [1, ]
        // IT(2) []
        // CT(2) []
        writer1.submitInitiatedTime( 2 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( -1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT(1) [ ,2]
        // CT(1) [1, ]
        // IT(2) [ , ]
        // CT(2) [ ,2]
        writer2.submitInitiatedTime( 2 );
        writer2.submitCompletedTime( 2 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 2L ) );
        assertThat( reader.completionTimeAsMilli(), is( 1L ) );

        // IT(1) [ ,2]
        // CT(1) [1, ]
        // IT(2) [ , , ]
        // CT(2) [ ,2,3]
        writer2.submitInitiatedTime( 3 );
        writer2.submitCompletedTime( 3 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 2L ) );
        assertThat( reader.completionTimeAsMilli(), is( 1L ) );

        // IT(1) [ ,2]
        // CT(1) [1, ]
        // IT(2) [ , , ,3,3,3]
        // CT(2) [ ,2,3, , , ]
        writer2.submitInitiatedTime( 3 );
        writer2.submitInitiatedTime( 3 );
        writer2.submitInitiatedTime( 3 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 2L ) );
        assertThat( reader.completionTimeAsMilli(), is( 1L ) );

        // IT(1) [ , ]
        // CT(1) [1,2]
        // IT(2) [ , , ,3,3,3]
        // CT(2) [ ,2,3, , , ]
        writer1.submitCompletedTime( 2 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 2L ) );
        assertThat( reader.completionTimeAsMilli(), is( 1L ) );

        // IT(1) [ , ,3]
        // CT(1) [1,2, ]
        // IT(2) [ , , ,3,3,3]
        // CT(2) [ ,2,3, , , ]
        writer1.submitInitiatedTime( 3 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT(1) [ , ,3,4]
        // CT(1) [1,2, , ]
        // IT(2) [ , , ,3,3,3]
        // CT(2) [ ,2,3, , , ]
        writer1.submitInitiatedTime( 4 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT(1) [ , ,3,4]
        // CT(1) [1,2, , ]
        // IT(2) [ , , , , , ]
        // CT(2) [ ,2,3,3,3,3]
        writer2.submitCompletedTime( 3 );
        writer2.submitCompletedTime( 3 );
        writer2.submitCompletedTime( 3 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT(1) [ , ,3,4]
        // CT(1) [1,2, , ]
        // IT(2) [ , , , , , , ]
        // CT(2) [ ,2,3,3,3,3,4]
        writer2.submitInitiatedTime( 4 );
        writer2.submitCompletedTime( 4 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT(1) [ , ,3, ]
        // CT(1) [1,2, ,4]
        // IT(2) [ , , , , , , ]
        // CT(2) [ ,2,3,3,3,3,4]
        writer1.submitCompletedTime( 4 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT(1) [ , ,3, ,5]
        // CT(1) [1,2, ,4, ]
        // IT(2) [ , , , , , , ]
        // CT(2) [ ,2,3,3,3,3,4]
        writer1.submitInitiatedTime( 5 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT(1) [ , ,3, , ]
        // CT(1) [1,2, ,4,5]
        // IT(2) [ , , , , , , ]
        // CT(2) [ ,2,3,3,3,3,4]
        writer1.submitCompletedTime( 5 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT(1) [ , ,3, , ]
        // CT(1) [1,2, ,4,5]
        // IT(2) [ , , , , , , , ]
        // CT(2) [ ,2,3,3,3,3,4,5]
        writer2.submitInitiatedTime( 5 );
        writer2.submitCompletedTime( 5 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT(1) [ , , , , ]
        // CT(1) [1,2,3,4,5]
        // IT(2) [ , , , , , , , ]
        // CT(2) [ ,2,3,3,3,3,4,5]
        writer1.submitCompletedTime( 3 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 5L ) );
        assertThat( reader.completionTimeAsMilli(), is( 4L ) );

        // IT(1) [ , , , , ]
        // CT(1) [1,2,3,4,5]
        // IT(2) [ , , , , , , , ]
        // CT(2) [ ,2,3,3,3,3,4,5]
        boolean exceptionThrown = false;
        try
        {
            writer2.submitInitiatedTime( 4 );
        }
        catch ( CompletionTimeException e )
        {
            exceptionThrown = true;
        }
        assertThat( exceptionThrown, is( true ) );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 5L ) );
        assertThat( reader.completionTimeAsMilli(), is( 4L ) );
    }

    @Test
    public void shouldPassComplexTwoWriterScenario2() throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer1 = completionTimeStateManager.newCompletionTimeWriter();
        CompletionTimeWriter writer2 = completionTimeStateManager.newCompletionTimeWriter();

        // When / Then

        // IT(1) []
        // CT(1) []
        // IT(2) []
        // CT(2) []
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( -1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) []
        // CT(2) []
        writer1.submitInitiatedTime( 1 );
        writer1.submitCompletedTime( 1 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( -1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) []
        // CT(2) [1]
        writer2.submitInitiatedTime( 1 );
        writer2.submitCompletedTime( 1 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) [ , ]
        // CT(2) [1,2]
        writer2.submitInitiatedTime( 2 );
        writer2.submitCompletedTime( 2 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT(1) [ , ]
        // CT(1) [1,2]
        // IT(2) [ , ]
        // CT(2) [1,2]
        writer1.submitInitiatedTime( 2 );
        writer1.submitCompletedTime( 2 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 2L ) );
        assertThat( reader.completionTimeAsMilli(), is( 1L ) );

        // IT(1) [ , ,10]
        // CT(1) [1,2,  ]
        // IT(2) [ , ]
        // CT(2) [1,2]
        writer1.submitInitiatedTime( 10 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 2L ) );
        assertThat( reader.completionTimeAsMilli(), is( 1L ) );

        // IT(1) [ , ,10]
        // CT(1) [1,2,  ]
        // IT(2) [ , , ]
        // CT(2) [1,2,3]
        writer2.submitInitiatedTime( 3 );
        writer2.submitCompletedTime( 3 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT(1) [ , ,10]
        // CT(1) [1,2,  ]
        // IT(2) [ , , ,  ]
        // CT(2) [1,2,3,10]
        writer2.submitInitiatedTime( 10 );
        writer2.submitCompletedTime( 10 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 10L ) );
        assertThat( reader.completionTimeAsMilli(), is( 3L ) );
    }

    @Test
    public void shouldPassComplexTwoWriterScenario3() throws CompletionTimeException
    {
        // Given
        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer1 = completionTimeStateManager.newCompletionTimeWriter();
        CompletionTimeWriter writer2 = completionTimeStateManager.newCompletionTimeWriter();

        // When / Then

        // IT(1) []
        // CT(1) []
        // IT(2) []
        // CT(2) []
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( -1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) []
        // CT(2) []
        writer1.submitInitiatedTime( 10 );
        writer1.submitCompletedTime( 10 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( -1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [1]
        // CT(2) [ ]
        writer2.submitInitiatedTime( 1 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [1,2]
        // CT(2) [ , ]
        writer2.submitInitiatedTime( 2 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [1,2,3]
        // CT(2) [ , ]
        writer2.submitInitiatedTime( 3 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ ,2,3]
        // CT(2) [1, , ]
        writer2.submitCompletedTime( 1 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 2L ) );
        assertThat( reader.completionTimeAsMilli(), is( 1L ) );

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , ,3]
        // CT(2) [1,2, ]
        writer2.submitCompletedTime( 2 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , ,3,5]
        // CT(2) [1,2, , ]
        writer2.submitInitiatedTime( 5 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , ,3,5,9]
        // CT(2) [1,2, , , ]
        writer2.submitInitiatedTime( 9 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , ,3,5,9,20]
        // CT(2) [1,2, , , ,  ]
        writer2.submitInitiatedTime( 20 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 3L ) );
        assertThat( reader.completionTimeAsMilli(), is( 2L ) );

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , , ,5,9,20]
        // CT(2) [1,2,3, , ,  ]
        writer2.submitCompletedTime( 3 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 5L ) );
        assertThat( reader.completionTimeAsMilli(), is( 3L ) );

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , , ,5, ,20]
        // CT(2) [1,2,3, ,9,  ]
        writer2.submitCompletedTime( 9 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 5L ) );
        assertThat( reader.completionTimeAsMilli(), is( 3L ) );

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , , , , ,20]
        // CT(2) [1,2,3,5,9,  ]
        writer2.submitCompletedTime( 5 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 10L ) );
        assertThat( reader.completionTimeAsMilli(), is( 9L ) );

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , , , , ,  ]
        // CT(2) [1,2,3,5,9,20]
        writer2.submitCompletedTime( 20 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 10L ) );
        assertThat( reader.completionTimeAsMilli(), is( 9L ) );

        // IT(1) [  ,11]
        // CT(1) [10, ]
        // IT(2) [ , , , , ,  ]
        // CT(2) [1,2,3,5,9,20]
        writer1.submitInitiatedTime( 11 );
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 11L ) );
        assertThat( reader.completionTimeAsMilli(), is( 10L ) );
    }

    /**
     * ****************************************************
     * ****************************************************
     * ****************************************************
     * Tests with multiple writers, AND concurrency
     * ****************************************************
     * ****************************************************
     * ****************************************************
     */

    @Test
    public void concurrentScenario1() throws CompletionTimeException
    {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        // IT(1) [ , , , , ,6,7]
        // CT(1) [1,2,3,4,5, , ]
        List<Tuple2<CompletionTimeWriterThread.WriteType,Long>> writeStream1 = Lists.newArrayList(
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 2L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 3L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 4L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 5L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 2L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 3L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 4L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 5L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 6L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 7L )
        );

        // IT(2) [ , , , , ,100]
        // CT(2) [2,2,2,2,4,   ]
        List<Tuple2<CompletionTimeWriterThread.WriteType,Long>> writeStream2 = Lists.newArrayList(
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 2l ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 2l ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 2l ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 2l ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 4l ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 4l ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 2l ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 2l ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 2l ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 2l ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 100l )
        );

        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer1 = completionTimeStateManager.newCompletionTimeWriter();
        CompletionTimeWriter writer2 = completionTimeStateManager.newCompletionTimeWriter();

        CompletionTimeWriterThread thread1 =
                new CompletionTimeWriterThread( writer1, writeStream1.iterator(), errorReporter );
        CompletionTimeWriterThread thread2 =
                new CompletionTimeWriterThread( writer2, writeStream2.iterator(), errorReporter );

        thread1.start();
        thread2.start();

        boolean thread1CompletedOnTime = waitForCompletionTimeWriterThread( 1000, thread1 );
        boolean thread2CompletedOnTime = waitForCompletionTimeWriterThread( 500, thread2 );

        assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

        // clean up threads
        if ( !thread1CompletedOnTime || !thread2CompletedOnTime )
        {
            thread1.interrupt();
            thread2.interrupt();
        }
        assertThat( thread1CompletedOnTime, is( true ) );
        assertThat( thread2CompletedOnTime, is( true ) );

        // IT(1) [ , , , , ,6,7]
        // CT(1) [1,2,3,4,5, , ]
        // IT(2) [ , , , , ,100]
        // CT(2) [2,2,2,2,4,   ]
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 6L ) );
        assertThat( reader.completionTimeAsMilli(), is( 5L ) );

        thread1.shutdown();
        thread2.shutdown();
    }

    @Test
    public void concurrentScenario2() throws CompletionTimeException
    {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        // IT(1) [ , , , , ,6,7]
        // CT(1) [1,2,3,4,5, , ]
        List<Tuple2<CompletionTimeWriterThread.WriteType,Long>> writeStream1 = Lists.newArrayList(
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 2L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 3L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 4L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 5L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 2L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 3L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 4L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 5L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 6L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 7L )
        );

        // IT(2) [ , , , , ,100]
        // CT(2) [2,2,2,2,4,   ]
        List<Tuple2<CompletionTimeWriterThread.WriteType,Long>> writeStream2 = Lists.newArrayList(
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 2L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 2L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 2L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 2L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 4L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 4L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 2L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 2L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 2L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 2L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 100L )
        );

        // IT(3) []
        // CT(3) []
        List<Tuple2<CompletionTimeWriterThread.WriteType,Long>> writeStream3 = Lists.newArrayList(
        );

        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;

        CompletionTimeWriter writer1 = completionTimeStateManager.newCompletionTimeWriter();
        CompletionTimeWriter writer2 = completionTimeStateManager.newCompletionTimeWriter();
        CompletionTimeWriter writer3 = completionTimeStateManager.newCompletionTimeWriter();

        CompletionTimeWriterThread thread1 =
                new CompletionTimeWriterThread( writer1, writeStream1.iterator(), errorReporter );
        CompletionTimeWriterThread thread2 =
                new CompletionTimeWriterThread( writer2, writeStream2.iterator(), errorReporter );
        CompletionTimeWriterThread thread3 =
                new CompletionTimeWriterThread( writer3, writeStream3.iterator(), errorReporter );

        thread1.start();
        thread2.start();
        thread3.start();

        boolean thread1CompletedOnTime = waitForCompletionTimeWriterThread( 1000, thread1 );
        boolean thread2CompletedOnTime = waitForCompletionTimeWriterThread( 500, thread2 );
        boolean thread3CompletedOnTime = waitForCompletionTimeWriterThread( 500, thread3 );

        assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

        // clean up threads
        if ( !thread1CompletedOnTime || !thread2CompletedOnTime )
        {
            thread1.interrupt();
            thread2.interrupt();
            thread3.interrupt();
        }
        assertThat( thread1CompletedOnTime, is( true ) );
        assertThat( thread2CompletedOnTime, is( true ) );
        assertThat( thread3CompletedOnTime, is( true ) );

        // IT(1) [ , , , , ,6,7]
        // CT(1) [1,2,3,4,5, , ]
        // IT(2) [ , , , , ,100]
        // CT(2) [2,2,2,2,4,   ]
        // IT(3) []
        // CT(3) []
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( -1L ) );
        assertThat( reader.completionTimeAsMilli(), is( -1L ) );

        thread1.shutdown();
        thread2.shutdown();
        thread3.shutdown();
    }

    @Test
    public void concurrentScenario3() throws CompletionTimeException
    {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        // IT(1) [ , , , , , , , , , , , , , , , , , , , ,2]
        // CT(1) [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, ]
        List<Tuple2<CompletionTimeWriterThread.WriteType,Long>> writeStream1 = Lists.newArrayList(
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),

                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),

                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 2L )
        );
        // IT(2) [ , , , , , , , , , , , , , , , , , , , ,2]
        // CT(2) [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, ]
        List<Tuple2<CompletionTimeWriterThread.WriteType,Long>> writeStream2 = Lists.newArrayList(
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),

                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),

                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 2L )
        );
        // IT(3) [ , , , , , , , , , , , , , , , , , , , ,2]
        // CT(3) [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, ]
        List<Tuple2<CompletionTimeWriterThread.WriteType,Long>> writeStream3 = Lists.newArrayList(
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ),

                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),
                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ),

                Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 2L )
        );

        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;
        CompletionTimeWriter writer1 = completionTimeStateManager.newCompletionTimeWriter();
        CompletionTimeWriter writer2 = completionTimeStateManager.newCompletionTimeWriter();
        CompletionTimeWriter writer3 = completionTimeStateManager.newCompletionTimeWriter();

        CompletionTimeWriterThread thread1 =
                new CompletionTimeWriterThread( writer1, writeStream1.iterator(), errorReporter );
        CompletionTimeWriterThread thread2 =
                new CompletionTimeWriterThread( writer2, writeStream2.iterator(), errorReporter );
        CompletionTimeWriterThread thread3 =
                new CompletionTimeWriterThread( writer3, writeStream3.iterator(), errorReporter );

        thread1.start();
        thread2.start();
        thread3.start();

        boolean thread1CompletedOnTime = waitForCompletionTimeWriterThread( 1000, thread1 );
        boolean thread2CompletedOnTime = waitForCompletionTimeWriterThread( 500, thread2 );
        boolean thread3CompletedOnTime = waitForCompletionTimeWriterThread( 500, thread3 );

        assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

        // clean up threads
        if ( !thread1CompletedOnTime || !thread2CompletedOnTime )
        {
            thread1.interrupt();
            thread2.interrupt();
            thread3.interrupt();
        }
        assertThat( thread1CompletedOnTime, is( true ) );
        assertThat( thread2CompletedOnTime, is( true ) );
        assertThat( thread3CompletedOnTime, is( true ) );

        // IT(1) [ , , , , , , , , , , , , , , , , , , , ,2]
        // CT(1) [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, ]
        // IT(2) [ , , , , , , , , , , , , , , , , , , , ,2]
        // CT(2) [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, ]
        // IT(3) [ , , , , , , , , , , , , , , , , , , , ,2]
        // CT(3) [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, ]
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 2L ) );
        assertThat( reader.completionTimeAsMilli(), is( 1L ) );

        thread1.shutdown();
        thread2.shutdown();
        thread3.shutdown();
    }

    @Test
    public void concurrentScenario4() throws CompletionTimeException
    {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42L ) );

        Iterator<Tuple2<CompletionTimeWriterThread.WriteType,Long>> writeStream1 = Iterators.concat(
                gf.limit(
                        gf.constant( Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1L ) ),
                        10000 ),
                gf.limit(
                        gf.constant( Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 1L ) ),
                        10000 ),
                gf.limit(
                        gf.constant( Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 100L ) ),
                        10000 ),
                gf.limit(
                        gf.constant( Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 100L ) ),
                        10000 ),
                gf.limit(
                        gf.constant( Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 1000L ) ),
                        1 )
        );

        Iterator<Tuple2<CompletionTimeWriterThread.WriteType,Long>> writeStream2 = Iterators.concat(
                gf.limit(
                        gf.constant( Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 10L ) ),
                        10000 ),
                gf.limit(
                        gf.constant( Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 10L ) ),
                        10000 ),
                gf.limit(
                        gf.constant( Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 90L ) ),
                        1000 ),
                gf.limit(
                        gf.constant( Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 90L ) ),
                        1000 ),
                gf.limit(
                        gf.constant( Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 900L ) ),
                        1 ),
                gf.limit(
                        gf.constant( Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 900L ) ),
                        1 ),
                gf.limit(
                        gf.constant( Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 9000L ) ),
                        1000 ),
                gf.limit(
                        gf.constant( Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 9000L ) ),
                        1000 )
        );

        Iterator<Tuple2<CompletionTimeWriterThread.WriteType,Long>> writeStream3 = Iterators.concat(
                gf.limit(
                        gf.constant( Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 2L ) ),
                        10000 ),
                gf.limit(
                        gf.constant( Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_CT, 2L ) ),
                        10000 ),
                gf.limit(
                        gf.constant( Tuple.tuple2( CompletionTimeWriterThread.WriteType.WRITE_IT, 901L ) ),
                        1 )
        );

        MultiWriterCompletionTimeStateManager completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        CompletionTimeReader reader = completionTimeStateManager;

        CompletionTimeWriter writer1 = completionTimeStateManager.newCompletionTimeWriter();
        CompletionTimeWriter writer2 = completionTimeStateManager.newCompletionTimeWriter();
        CompletionTimeWriter writer3 = completionTimeStateManager.newCompletionTimeWriter();

        CompletionTimeWriterThread thread1 = new CompletionTimeWriterThread( writer1, writeStream1, errorReporter );
        CompletionTimeWriterThread thread2 = new CompletionTimeWriterThread( writer2, writeStream2, errorReporter );
        CompletionTimeWriterThread thread3 = new CompletionTimeWriterThread( writer3, writeStream3, errorReporter );

        thread1.start();
        thread2.start();
        thread3.start();

        boolean thread1CompletedOnTime = waitForCompletionTimeWriterThread( 5000, thread1 );
        boolean thread2CompletedOnTime = waitForCompletionTimeWriterThread( 5000, thread2 );
        boolean thread3CompletedOnTime = waitForCompletionTimeWriterThread( 5000, thread3 );

        assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

        // clean up threads
        if ( !thread1CompletedOnTime || !thread2CompletedOnTime )
        {
            thread1.interrupt();
            thread2.interrupt();
            thread3.interrupt();
        }
        assertThat( thread1CompletedOnTime, is( true ) );
        assertThat( thread2CompletedOnTime, is( true ) );
        assertThat( thread3CompletedOnTime, is( true ) );

        // IT(1) [    ,      ,1000]
        // CT(1) [1...,100...,    ]
        // IT(2) [     ,     ,   ,       ]
        // CT(2) [10...,90...,900,9000...]
        // IT(3) [    ,901]
        // CT(3) [2...,   ]
        assertThat( reader.lastKnownLowestInitiatedTimeAsMilli(), is( 901L ) );
        assertThat( reader.completionTimeAsMilli(), is( 900L ) );

        thread1.shutdown();
        thread2.shutdown();
        thread3.shutdown();
    }

    private boolean waitForCompletionTimeWriterThread(
            long timeoutDurationAsMilli,
            CompletionTimeWriterThread thread ) throws CompletionTimeException
    {
        TimeSource timeSource = new SystemTimeSource();
        long endTimeAsMilli = timeSource.nowAsMilli() + timeoutDurationAsMilli;
        while ( timeSource.nowAsMilli() < endTimeAsMilli )
        {
            if ( thread.hasCompletedExecution() )
            { break; }
            Spinner.powerNap( 100 );
        }
        return thread.hasCompletedExecution();
    }

    static class CompletionTimeWriterThread extends Thread
    {
        enum WriteType
        {
            WRITE_IT,
            WRITE_CT
        }

        private final CompletionTimeWriter writer;
        private final AtomicBoolean continueExecuting = new AtomicBoolean( true );
        private final AtomicBoolean hasCompletedExecution = new AtomicBoolean( false );
        private final Iterator<Tuple2<WriteType,Long>> writeStream;
        private final ConcurrentErrorReporter errorReporter;

        CompletionTimeWriterThread(
                CompletionTimeWriter writer,
                Iterator<Tuple2<WriteType,Long>> writeStream,
                ConcurrentErrorReporter errorReporter )
        {
            this.writer = writer;
            this.writeStream = writeStream;
            this.errorReporter = errorReporter;
        }

        void shutdown()
        {
            continueExecuting.set( false );
        }

        boolean hasCompletedExecution()
        {
            return hasCompletedExecution.get();
        }

        @Override
        public void run()
        {
            try
            {
                while ( continueExecuting.get() )
                {
                    if ( writeStream.hasNext() )
                    {
                        Tuple2<WriteType,Long> write = writeStream.next();
                        WriteType writeType = write._1();
                        long writeTimeAsMilli = write._2();
                        switch ( writeType )
                        {
                        case WRITE_IT:
                            writer.submitInitiatedTime( writeTimeAsMilli );
                            break;
                        case WRITE_CT:
                            writer.submitCompletedTime( writeTimeAsMilli );
                            break;
                        }
                    }
                    else
                    {
                        break;
                    }
                }
            }
            catch ( CompletionTimeException e )
            {
                errorReporter.reportError( this, ConcurrentErrorReporter.stackTraceToString( e ) );
            }
            hasCompletedExecution.set( true );
        }
    }
}
