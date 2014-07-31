package com.ldbc.driver.runtime.coordination;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.util.Tuple;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class MultiWriterLocalCompletionTimeConcurrentStateManagerTest {
    /* ****************************************************
     * ****************************************************
     * ****************************************************
     * Tests that are known to work with LocalCompletionTimeStateManager
     * ****************************************************
     * ****************************************************
     * ****************************************************
     */

    @Test
    public void shouldReturnNullWhenNoEventsHaveBeenInitiatedOrCompletedWithOneWriter() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager
                = new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader reader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter writer = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When
        // no events have been initiated or completed

        // Then
        assertThat(reader.lastKnownLowestInitiatedTime(), is(nullValue()));
        assertThat(reader.localCompletionTime(), is(nullValue()));
    }

    @Test
    public void shouldReturnNullWhenNoOperationsHaveCompletedWithOneWriter() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager
                = new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader reader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter writer = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When

        writer.submitLocalInitiatedTime(Time.fromSeconds(1));
        writer.submitLocalInitiatedTime(Time.fromSeconds(2));
        writer.submitLocalInitiatedTime(Time.fromSeconds(3));

        // Then
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(1)));
        assertThat(reader.localCompletionTime(), is(nullValue()));
    }

    @Test
    public void shouldReturnNullWhenEarliestInitiatedOperationHasNotCompletedWithOneWriter() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager
                = new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader reader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter writer = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When/Then

        writer.submitLocalInitiatedTime(Time.fromSeconds(1));
        writer.submitLocalInitiatedTime(Time.fromSeconds(2));
        writer.submitLocalInitiatedTime(Time.fromSeconds(3));

        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(1)));
        assertThat(reader.localCompletionTime(), is(nullValue()));

        writer.submitLocalCompletedTime(Time.fromSeconds(2));
        writer.submitLocalCompletedTime(Time.fromSeconds(3));

        // Then
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(1)));
        assertThat(reader.localCompletionTime(), is(nullValue()));
    }

    @Test
    public void shouldAdvanceToNextUncompletedEventAsInitiatedEventsAreCompletedWithOneWriter() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager
                = new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader reader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter writer = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When/Then
        writer.submitLocalInitiatedTime(Time.fromSeconds(1));
        writer.submitLocalInitiatedTime(Time.fromSeconds(2));
        writer.submitLocalInitiatedTime(Time.fromSeconds(3));

        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(1)));
        assertThat(reader.localCompletionTime(), is(nullValue()));

        writer.submitLocalCompletedTime(Time.fromSeconds(3));
        writer.submitLocalCompletedTime(Time.fromSeconds(1));

        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(2)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(1)));
    }

    @Test
    public void shouldAdvanceToNextUncompletedEventAsInitiatedEventsAreCompletedButOnlyIfNoUncompletedTimesExistAtSameTimeWithOneWriter() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager
                = new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader reader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter writer = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When/Then

        // IT [1,1,2,3]
        // CT []
        writer.submitLocalInitiatedTime(Time.fromSeconds(1));
        writer.submitLocalInitiatedTime(Time.fromSeconds(1));
        writer.submitLocalInitiatedTime(Time.fromSeconds(2));
        writer.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(1)));
        assertThat(reader.localCompletionTime(), is(nullValue()));

        // IT [ ,1,2, ]
        // CT [1, , ,3]
        writer.submitLocalCompletedTime(Time.fromSeconds(3));
        writer.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(1)));
        assertThat(reader.localCompletionTime(), is(nullValue()));

        // IT [ , ,2, ]
        // CT [1,1, ,3]
        writer.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(2)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(1)));

        // IT [ , , , ]
        // CT [1,1,2,3]
        writer.submitLocalCompletedTime(Time.fromSeconds(2));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(3)));
        // because we do not know if more Initiated Time == 3 will arrive
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(2)));

        // IT [ , , , ,3]
        // CT [1,1,2,3, ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(3)));
        // another Initiated Time == 3 arrived
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(2)));

        // IT [ , , , , ]
        // CT [1,1,2,3,3]
        writer.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(3)));
        // because we do not know if more Initiated Time == 3 will arrive
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(2)));

        // IT [ , , , , ,4]
        // CT [1,1,2,3,3, ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(4));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(4)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(3)));
    }

    @Test
    public void shouldAllowForSubmittedInitiatedTimeToEqualCurrentCompletionTimeButNotBeLowerWithOneWriter() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager
                = new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader reader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter writer = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When/Then
        // IT [1,2,3]
        // CT [ , , ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(1));
        writer.submitLocalInitiatedTime(Time.fromSeconds(2));
        writer.submitLocalInitiatedTime(Time.fromSeconds(3));

        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(1)));
        assertThat(reader.localCompletionTime(), is(nullValue()));

        // IT [ ,2,3]
        // CT [1, , ]
        writer.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(2)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(1)));

        //apply initiated time equal to highest submitted initiated time AND equal to LCT <- should be ok
        // IT [ ,2,3,3]
        // CT [1, , , ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(2)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(1)));

        // IT [ ,2, ,3]
        // CT [1, ,3, ]
        writer.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(2)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(1)));

        // IT [ ,2, ,3]
        // CT [1, ,3, ]
        boolean exceptionThrown = false;
        try {
            writer.submitLocalInitiatedTime(Time.fromSeconds(1));
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(2)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(1)));
    }

    @Test
    public void shouldAdvanceToNextUncompletedEventUntilAllInitiatedEventsAreCompletedWhenCompletedTimesComeInOutOfOrder1WithOneWriter() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager
                = new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader reader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter writer = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When/Then
        // IT [1,2,3,4]
        // CT [ , , , ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(1));
        writer.submitLocalInitiatedTime(Time.fromSeconds(2));
        writer.submitLocalInitiatedTime(Time.fromSeconds(3));
        writer.submitLocalInitiatedTime(Time.fromSeconds(4));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(1)));
        assertThat(reader.localCompletionTime(), is(nullValue()));

        // IT [1,2, , ]
        // CT [ , ,3,4]
        writer.submitLocalCompletedTime(Time.fromSeconds(4));
        writer.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(1)));
        assertThat(reader.localCompletionTime(), is(nullValue()));

        // IT [ ,2, , ]
        // CT [1, ,3,4]
        writer.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(2)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(1)));

        // IT [ , , , ]
        // CT [1,2,3,4]
        writer.submitLocalCompletedTime(Time.fromSeconds(2));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(4)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(3)));
    }

    @Test
    public void shouldAdvanceToNextUncompletedEventUntilAllInitiatedEventsAreCompletedWhenCompletedTimesComeInOutOfOrder2WithOneWriter() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager
                = new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader reader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter writer = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When/Then

        // IT [1,2,3,4]
        // CT [ , , , ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(1));
        writer.submitLocalInitiatedTime(Time.fromSeconds(2));
        writer.submitLocalInitiatedTime(Time.fromSeconds(3));
        writer.submitLocalInitiatedTime(Time.fromSeconds(4));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(1)));
        assertThat(reader.localCompletionTime(), is(nullValue()));

        // IT [1,2, , ]
        // CT [ , ,3,4]
        writer.submitLocalCompletedTime(Time.fromSeconds(3));
        writer.submitLocalCompletedTime(Time.fromSeconds(4));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(1)));
        assertThat(reader.localCompletionTime(), is(nullValue()));

        // IT [ ,2, , ]
        // CT [1, ,3,4]
        writer.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(2)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(1)));

        // IT [ , , , ]
        // CT [1,2,3,4]
        writer.submitLocalCompletedTime(Time.fromSeconds(2));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(4)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(3)));
    }

    @Test
    public void shouldAdvanceToNextUncompletedEventUntilAllInitiatedEventsAreCompletedWhenCompletedTimesComeInOutOfOrder3WithOneWriter() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager
                = new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader reader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter writer = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When/Then
        // IT [1,2,3,4]
        // CT [ , , , ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(1));
        writer.submitLocalInitiatedTime(Time.fromSeconds(2));
        writer.submitLocalInitiatedTime(Time.fromSeconds(3));
        writer.submitLocalInitiatedTime(Time.fromSeconds(4));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(1)));
        assertThat(reader.localCompletionTime(), is(nullValue()));

        // IT [1,2, , ]
        // CT [ , ,3,4]
        writer.submitLocalCompletedTime(Time.fromSeconds(4));
        writer.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(1)));
        assertThat(reader.localCompletionTime(), is(nullValue()));

        // IT [1, , , ]
        // CT [ ,2,3,4]
        writer.submitLocalCompletedTime(Time.fromSeconds(2));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(1)));
        assertThat(reader.localCompletionTime(), is(nullValue()));

        // IT [ , , , ]
        // CT [1,2,3,4]
        writer.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(4)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(3)));
    }

    @Test
    public void shouldReturnLatestInitiatedEventTimeWhenAllEventsHaveCompletedWithOneWriter() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager
                = new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader reader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter writer = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When/Then

        // IT [1,2,3]
        // CT [ , , ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(1));
        writer.submitLocalInitiatedTime(Time.fromSeconds(2));
        writer.submitLocalInitiatedTime(Time.fromSeconds(3));

        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(1)));
        assertThat(reader.localCompletionTime(), is(nullValue()));

        // IT [ , , ]
        // CT [1,2,3]
        writer.submitLocalCompletedTime(Time.fromSeconds(1));
        writer.submitLocalCompletedTime(Time.fromSeconds(2));
        writer.submitLocalCompletedTime(Time.fromSeconds(3));

        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(3)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(2)));
    }

    @Test
    public void shouldThrowExceptionWhenEventCompletesThatHasNoMatchingInitiatedEntryWithOneWriter() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager
                = new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader reader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter writer = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When/Then

        // IT [1,2,3]
        // CT [ , , ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(1));
        writer.submitLocalInitiatedTime(Time.fromSeconds(2));
        writer.submitLocalInitiatedTime(Time.fromSeconds(3));

        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(1)));
        assertThat(reader.localCompletionTime(), is(nullValue()));

        // IT [ ,2,3]
        // CT [1, , ]
        writer.submitLocalCompletedTime(Time.fromSeconds(1));

        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(2)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(1)));

        boolean exceptionThrown = false;
        try {
            // only one entry with DueTime=1 exists, this should throw exception
            writer.submitLocalCompletedTime(Time.fromSeconds(1));
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(2)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(1)));
    }

    @Test
    public void shouldReturnLatestTimeBehindWhichThereAreNoUncompletedITEventsWithOneWriter() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager
                = new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader reader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter writer = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When/Then
        // IT [1]
        // CT [ ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(1));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(1)));
        assertThat(reader.localCompletionTime(), is(nullValue()));
        // IT [ ]
        // CT [1]
        writer.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(1)));
        assertThat(reader.localCompletionTime(), is(nullValue()));

        // IT [ ,2]
        // CT [1, ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(2));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(2)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(1)));
        // IT [ , ]
        // CT [1,2]
        writer.submitLocalCompletedTime(Time.fromSeconds(2));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(2)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(1)));

        // IT [ , ,3]
        // CT [1,2, ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(3)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(2)));

        // IT [ , ,3,4]
        // CT [1,2, , ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(4));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(3)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(2)));

        // IT [ , ,3,4,5]
        // CT [1,2, , , ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(5));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(3)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(2)));
        // IT [ , ,3,4, ]
        // CT [1,2, , ,5]
        writer.submitLocalCompletedTime(Time.fromSeconds(5));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(3)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(2)));

        // IT [ , ,3,4, ,5]
        // CT [1,2, , ,5, ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(5));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(3)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(2)));
        // IT [ , ,3,4, , ]
        // CT [1,2, , ,5,5]
        writer.submitLocalCompletedTime(Time.fromSeconds(5));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(3)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(2)));

        // IT [ , ,3,4, , ,5]
        // CT [1,2, , ,5,5, ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(5));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(3)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(2)));
        // IT [ , ,3,4, , , ]
        // CT [1,2, , ,5,5,5]
        writer.submitLocalCompletedTime(Time.fromSeconds(5));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(3)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(2)));

        // IT [ , , ,4, , , ]
        // CT [1,2,3, ,5,5,5]
        writer.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(4)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(3)));

        // IT [ , , , , , , ]
        // CT [1,2,3,4,5,5,5]
        writer.submitLocalCompletedTime(Time.fromSeconds(4));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(5)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(4)));

        // IT [ , , , , , , ,6]
        // CT [1,2,3,4,5,5,5, ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(6));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(6)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(5)));
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNotMatchingCTEvenWhenMultipleEventsHaveSameInitiatedTimeWithOneWriter() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager
                = new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader reader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter writer = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When/Then
        // IT [1]
        // CT [ ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(1));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(1)));
        assertThat(reader.localCompletionTime(), is(nullValue()));
        // IT [ ]
        // CT [1]
        writer.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(1)));
        assertThat(reader.localCompletionTime(), is(nullValue()));

        // IT [ ,2]
        // CT [1, ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(2));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(2)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(1)));
        // IT [ , ]
        // CT [1,2]
        writer.submitLocalCompletedTime(Time.fromSeconds(2));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(2)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(1)));

        // IT [ , ,3]
        // CT [1,2, ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(3)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(2)));

        // IT [ , ,3,3]
        // CT [1,2, , ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(3)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(2)));

        // IT [ , ,3,3,4]
        // CT [1,2, , , ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(4));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(3)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(2)));

        // IT [ , , ,3,4]
        // CT [1,2,3, , ]
        writer.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(3)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(2)));

        // IT [ , , ,3, ]
        // CT [1,2,3, ,4]
        writer.submitLocalCompletedTime(Time.fromSeconds(4));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(3)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(2)));

        // IT [ , , , , ]
        // CT [1,2,3,3,4]
        writer.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(4)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(3)));

        // IT [ , , , , ,5]
        // CT [1,2,3,3,4, ]
        writer.submitLocalInitiatedTime(Time.fromSeconds(5));
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromSeconds(5)));
        assertThat(reader.localCompletionTime(), is(Time.fromSeconds(4)));
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
    public void shouldReturnNullWhenNoWriters() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager =
                new MultiWriterLocalCompletionTimeConcurrentStateManager();

        // When
        LocalCompletionTimeReader reader = multiWriterLocalCompletionTimeConcurrentStateManager;

        // Then
        assertThat(reader.lastKnownLowestInitiatedTime(), is(nullValue()));
        assertThat(reader.localCompletionTime(), is(nullValue()));
    }

    @Test
    public void shouldReturnNullWhenOneWriterAndNoInitiatedTimesAndNoCompletedTimes() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager =
                new MultiWriterLocalCompletionTimeConcurrentStateManager();

        // When
        LocalCompletionTimeReader localCompletionTimeReader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter localCompletionTimeWriter = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // Then
        // IT(1) []
        // CT(1) []
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(nullValue()));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));
    }

    @Test
    public void shouldReturnNullWhenMultipleWritersAndNoInitiatedTimesAndNoCompletedTimes() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager =
                new MultiWriterLocalCompletionTimeConcurrentStateManager();

        // When
        LocalCompletionTimeReader localCompletionTimeReader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter localCompletionTimeWriter1 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter localCompletionTimeWriter2 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter localCompletionTimeWriter3 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // Then
        // IT(1) []
        // CT(1) []
        // IT(2) []
        // CT(2) []
        // IT(3) []
        // CT(3) []
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(nullValue()));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));
    }

    @Test
    public void shouldReturnNullWhenOneWriterAndOneInitiatedTimeAndNoCompletedTimes() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager =
                new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader localCompletionTimeReader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter localCompletionTimeWriter = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromMilli(1));

        // Then
        // IT(1) []
        // CT(1) []
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(1)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));
    }

    @Test
    public void shouldReturnNullWhenOneWriterAndMultipleInitiatedTimeAndNoCompletedTimes() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager =
                new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader localCompletionTimeReader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter localCompletionTimeWriter = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When/Then
        // IT(1) [1,2,3]
        // CT(1) [ , , ]
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromMilli(1));
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromMilli(2));
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromMilli(3));

        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(1)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));
    }

    @Test
    public void shouldReturnNullWhenMultipleWritersAndOneInitiatedTimeAndNoCompletedTimes() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager =
                new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader localCompletionTimeReader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter localCompletionTimeWriter1 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter localCompletionTimeWriter2 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When/Then
        // IT(1) [1]
        // CT(1) [ ]
        // IT(2) []
        // CT(2) []
        localCompletionTimeWriter1.submitLocalInitiatedTime(Time.fromMilli(1));

        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(nullValue()));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));
    }

    @Test
    public void shouldReturnNullWhenMultipleWritersAndMultipleInitiatedTimeAndNoCompletedTimes() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager =
                new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader localCompletionTimeReader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter localCompletionTimeWriter1 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter localCompletionTimeWriter2 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When/Then
        // IT(1) [1]
        // CT(1) [ ]
        // IT(2) [1]
        // CT(2) [ ]
        localCompletionTimeWriter1.submitLocalInitiatedTime(Time.fromMilli(1));
        localCompletionTimeWriter2.submitLocalInitiatedTime(Time.fromMilli(1));

        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(1)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));
    }

    @Test
    public void shouldReturnNullWhenMultipleWritersAndMultipleInitiatedTimesPerWriterAndNoCompletedTimes() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager =
                new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader localCompletionTimeReader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter localCompletionTimeWriter1 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter localCompletionTimeWriter2 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When/Then
        // IT(1) [1,2,3]
        // CT(1) [ , , ]
        // IT(2) []
        // CT(2) []
        localCompletionTimeWriter1.submitLocalInitiatedTime(Time.fromMilli(1));
        localCompletionTimeWriter1.submitLocalInitiatedTime(Time.fromMilli(2));
        localCompletionTimeWriter1.submitLocalInitiatedTime(Time.fromMilli(3));

        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(nullValue()));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));

        // IT(1) [1,2,3]
        // CT(1) [ , , ]
        // IT(2) [1,2,3]
        // CT(2) [ , , ]
        localCompletionTimeWriter2.submitLocalInitiatedTime(Time.fromMilli(1));
        localCompletionTimeWriter2.submitLocalInitiatedTime(Time.fromMilli(2));
        localCompletionTimeWriter2.submitLocalInitiatedTime(Time.fromMilli(3));

        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(1)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));
    }

    @Test
    public void shouldReturnSubmittedTimeWhenOneWriterAndOneInitiatedTimeAndOneCompletedTime() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager =
                new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader localCompletionTimeReader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter localCompletionTimeWriter1 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When
        // IT(1) [ ]
        // CT(1) [1]
        localCompletionTimeWriter1.submitLocalInitiatedTime(Time.fromMilli(1));
        localCompletionTimeWriter1.submitLocalCompletedTime(Time.fromMilli(1));

        // Then
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(1)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));
    }

    @Test
    public void shouldReturnNullWhenMultipleWritersAndOneWriterHasInitiatedTimeAndCompletedTimeButOtherHasNeither() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager =
                new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader localCompletionTimeReader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter localCompletionTimeWriter1 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter localCompletionTimeWriter2 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When
        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) []
        // CT(2) []
        localCompletionTimeWriter1.submitLocalInitiatedTime(Time.fromMilli(1));
        localCompletionTimeWriter1.submitLocalCompletedTime(Time.fromMilli(1));

        // Then
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(nullValue()));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));
    }

    @Test
    public void shouldReturnSubmittedTimeWhenMultipleWritersAndOneInitiatedTimeEachAndOneCompletedTimeEach() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager =
                new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader localCompletionTimeReader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter localCompletionTimeWriter1 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter localCompletionTimeWriter2 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When/Then
        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) []
        // CT(2) []
        localCompletionTimeWriter1.submitLocalInitiatedTime(Time.fromMilli(1));
        localCompletionTimeWriter1.submitLocalCompletedTime(Time.fromMilli(1));

        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(nullValue()));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));

        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) []
        // CT(2) [1]
        localCompletionTimeWriter2.submitLocalInitiatedTime(Time.fromMilli(1));
        localCompletionTimeWriter2.submitLocalCompletedTime(Time.fromMilli(1));

        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(1)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));
    }

    @Test
    public void shouldReturnMinTimeWhenMultipleWritersAndOneInitiatedTimeEachAndOneCompletedTimeEach() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager =
                new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader localCompletionTimeReader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter localCompletionTimeWriter1 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter localCompletionTimeWriter2 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When/Then
        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) []
        // CT(2) []
        localCompletionTimeWriter1.submitLocalInitiatedTime(Time.fromMilli(1));
        localCompletionTimeWriter1.submitLocalCompletedTime(Time.fromMilli(1));

        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(nullValue()));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));

        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) []
        // CT(2) [2]
        localCompletionTimeWriter2.submitLocalInitiatedTime(Time.fromMilli(2));
        localCompletionTimeWriter2.submitLocalCompletedTime(Time.fromMilli(2));

        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(1)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));
    }

    @Test
    public void shouldPassComplexTwoWriterScenario1() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager =
                new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader localCompletionTimeReader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter writer1 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter writer2 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When / Then

        // IT(1) []
        // CT(1) []
        // IT(2) []
        // CT(2) []
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(nullValue()));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));

        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) []
        // CT(2) []
        writer1.submitLocalInitiatedTime(Time.fromMilli(1));
        writer1.submitLocalCompletedTime(Time.fromMilli(1));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(nullValue()));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));

        // IT(1) [ ,2]
        // CT(1) [1, ]
        // IT(2) []
        // CT(2) []
        writer1.submitLocalInitiatedTime(Time.fromMilli(2));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(nullValue()));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));

        // IT(1) [ ,2]
        // CT(1) [1, ]
        // IT(2) [ , ]
        // CT(2) [ ,2]
        writer2.submitLocalInitiatedTime(Time.fromMilli(2));
        writer2.submitLocalCompletedTime(Time.fromMilli(2));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(2)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(1)));

        // IT(1) [ ,2]
        // CT(1) [1, ]
        // IT(2) [ , , ]
        // CT(2) [ ,2,3]
        writer2.submitLocalInitiatedTime(Time.fromMilli(3));
        writer2.submitLocalCompletedTime(Time.fromMilli(3));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(2)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(1)));

        // IT(1) [ ,2]
        // CT(1) [1, ]
        // IT(2) [ , , ,3,3,3]
        // CT(2) [ ,2,3, , , ]
        writer2.submitLocalInitiatedTime(Time.fromMilli(3));
        writer2.submitLocalInitiatedTime(Time.fromMilli(3));
        writer2.submitLocalInitiatedTime(Time.fromMilli(3));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(2)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(1)));

        // IT(1) [ , ]
        // CT(1) [1,2]
        // IT(2) [ , , ,3,3,3]
        // CT(2) [ ,2,3, , , ]
        writer1.submitLocalCompletedTime(Time.fromMilli(2));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(2)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(1)));

        // IT(1) [ , ,3]
        // CT(1) [1,2, ]
        // IT(2) [ , , ,3,3,3]
        // CT(2) [ ,2,3, , , ]
        writer1.submitLocalInitiatedTime(Time.fromMilli(3));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(3)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(2)));

        // IT(1) [ , ,3,4]
        // CT(1) [1,2, , ]
        // IT(2) [ , , ,3,3,3]
        // CT(2) [ ,2,3, , , ]
        writer1.submitLocalInitiatedTime(Time.fromMilli(4));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(3)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(2)));

        // IT(1) [ , ,3,4]
        // CT(1) [1,2, , ]
        // IT(2) [ , , , , , ]
        // CT(2) [ ,2,3,3,3,3]
        writer2.submitLocalCompletedTime(Time.fromMilli(3));
        writer2.submitLocalCompletedTime(Time.fromMilli(3));
        writer2.submitLocalCompletedTime(Time.fromMilli(3));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(3)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(2)));

        // IT(1) [ , ,3,4]
        // CT(1) [1,2, , ]
        // IT(2) [ , , , , , , ]
        // CT(2) [ ,2,3,3,3,3,4]
        writer2.submitLocalInitiatedTime(Time.fromMilli(4));
        writer2.submitLocalCompletedTime(Time.fromMilli(4));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(3)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(2)));

        // IT(1) [ , ,3, ]
        // CT(1) [1,2, ,4]
        // IT(2) [ , , , , , , ]
        // CT(2) [ ,2,3,3,3,3,4]
        writer1.submitLocalCompletedTime(Time.fromMilli(4));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(3)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(2)));

        // IT(1) [ , ,3, ,5]
        // CT(1) [1,2, ,4, ]
        // IT(2) [ , , , , , , ]
        // CT(2) [ ,2,3,3,3,3,4]
        writer1.submitLocalInitiatedTime(Time.fromMilli(5));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(3)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(2)));

        // IT(1) [ , ,3, , ]
        // CT(1) [1,2, ,4,5]
        // IT(2) [ , , , , , , ]
        // CT(2) [ ,2,3,3,3,3,4]
        writer1.submitLocalCompletedTime(Time.fromMilli(5));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(3)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(2)));

        // IT(1) [ , ,3, , ]
        // CT(1) [1,2, ,4,5]
        // IT(2) [ , , , , , , , ]
        // CT(2) [ ,2,3,3,3,3,4,5]
        writer2.submitLocalInitiatedTime(Time.fromMilli(5));
        writer2.submitLocalCompletedTime(Time.fromMilli(5));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(3)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(2)));

        // IT(1) [ , , , , ]
        // CT(1) [1,2,3,4,5]
        // IT(2) [ , , , , , , , ]
        // CT(2) [ ,2,3,3,3,3,4,5]
        writer1.submitLocalCompletedTime(Time.fromMilli(3));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(5)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(4)));

        // IT(1) [ , , , , ]
        // CT(1) [1,2,3,4,5]
        // IT(2) [ , , , , , , , ]
        // CT(2) [ ,2,3,3,3,3,4,5]
        boolean exceptionThrown = false;
        try {
            writer2.submitLocalInitiatedTime(Time.fromMilli(4));
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(5)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(4)));
    }

    @Test
    public void shouldPassComplexTwoWriterScenario2() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager =
                new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader localCompletionTimeReader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter writer1 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter writer2 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When / Then

        // IT(1) []
        // CT(1) []
        // IT(2) []
        // CT(2) []
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(nullValue()));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));

        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) []
        // CT(2) []
        writer1.submitLocalInitiatedTime(Time.fromMilli(1));
        writer1.submitLocalCompletedTime(Time.fromMilli(1));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(nullValue()));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));

        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) []
        // CT(2) [1]
        writer2.submitLocalInitiatedTime(Time.fromMilli(1));
        writer2.submitLocalCompletedTime(Time.fromMilli(1));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(1)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));

        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) [ , ]
        // CT(2) [1,2]
        writer2.submitLocalInitiatedTime(Time.fromMilli(2));
        writer2.submitLocalCompletedTime(Time.fromMilli(2));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(1)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));

        // IT(1) [ , ]
        // CT(1) [1,2]
        // IT(2) [ , ]
        // CT(2) [1,2]
        writer1.submitLocalInitiatedTime(Time.fromMilli(2));
        writer1.submitLocalCompletedTime(Time.fromMilli(2));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(2)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(1)));

        // IT(1) [ , ,10]
        // CT(1) [1,2,  ]
        // IT(2) [ , ]
        // CT(2) [1,2]
        writer1.submitLocalInitiatedTime(Time.fromMilli(10));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(2)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(1)));

        // IT(1) [ , ,10]
        // CT(1) [1,2,  ]
        // IT(2) [ , , ]
        // CT(2) [1,2,3]
        writer2.submitLocalInitiatedTime(Time.fromMilli(3));
        writer2.submitLocalCompletedTime(Time.fromMilli(3));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(3)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(2)));

        // IT(1) [ , ,10]
        // CT(1) [1,2,  ]
        // IT(2) [ , , ,  ]
        // CT(2) [1,2,3,10]
        writer2.submitLocalInitiatedTime(Time.fromMilli(10));
        writer2.submitLocalCompletedTime(Time.fromMilli(10));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(10)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(3)));
    }

    @Test
    public void shouldPassComplexTwoWriterScenario3() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager =
                new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader localCompletionTimeReader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter writer1 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter writer2 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When / Then

        // IT(1) []
        // CT(1) []
        // IT(2) []
        // CT(2) []
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(nullValue()));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) []
        // CT(2) []
        writer1.submitLocalInitiatedTime(Time.fromMilli(10));
        writer1.submitLocalCompletedTime(Time.fromMilli(10));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(nullValue()));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [1]
        // CT(2) [ ]
        writer2.submitLocalInitiatedTime(Time.fromMilli(1));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(1)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [1,2]
        // CT(2) [ , ]
        writer2.submitLocalInitiatedTime(Time.fromMilli(2));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(1)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [1,2,3]
        // CT(2) [ , ]
        writer2.submitLocalInitiatedTime(Time.fromMilli(3));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(1)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(nullValue()));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ ,2,3]
        // CT(2) [1, , ]
        writer2.submitLocalCompletedTime(Time.fromMilli(1));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(2)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(1)));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , ,3]
        // CT(2) [1,2, ]
        writer2.submitLocalCompletedTime(Time.fromMilli(2));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(3)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(2)));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , ,3,5]
        // CT(2) [1,2, , ]
        writer2.submitLocalInitiatedTime(Time.fromMilli(5));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(3)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(2)));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , ,3,5,9]
        // CT(2) [1,2, , , ]
        writer2.submitLocalInitiatedTime(Time.fromMilli(9));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(3)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(2)));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , ,3,5,9,20]
        // CT(2) [1,2, , , ,  ]
        writer2.submitLocalInitiatedTime(Time.fromMilli(20));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(3)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(2)));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , , ,5,9,20]
        // CT(2) [1,2,3, , ,  ]
        writer2.submitLocalCompletedTime(Time.fromMilli(3));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(5)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(3)));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , , ,5, ,20]
        // CT(2) [1,2,3, ,9,  ]
        writer2.submitLocalCompletedTime(Time.fromMilli(9));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(5)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(3)));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , , , , ,20]
        // CT(2) [1,2,3,5,9,  ]
        writer2.submitLocalCompletedTime(Time.fromMilli(5));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(10)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(9)));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , , , , ,  ]
        // CT(2) [1,2,3,5,9,20]
        writer2.submitLocalCompletedTime(Time.fromMilli(20));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(10)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(9)));

        // IT(1) [  ,11]
        // CT(1) [10, ]
        // IT(2) [ , , , , ,  ]
        // CT(2) [1,2,3,5,9,20]
        writer1.submitLocalInitiatedTime(Time.fromMilli(11));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(11)));
        assertThat(localCompletionTimeReader.localCompletionTime(), is(Time.fromMilli(10)));
    }

    /* ****************************************************
     * ****************************************************
     * ****************************************************
     * Tests with multiple writers, AND concurrency
     * ****************************************************
     * ****************************************************
     * ****************************************************
     */

    @Test
    public void concurrentScenario1() throws CompletionTimeException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        // IT(1) [ , , , , ,6,7]
        // CT(1) [1,2,3,4,5, , ]
        List<Tuple.Tuple2<LocalCompletionTimeWriterThread.WriteType, Time>> writeStream1 = Lists.newArrayList(
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(2)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(3)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(4)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(5)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(2)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(3)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(4)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(5)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(6)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(7))
        );

        // IT(2) [ , , , , ,100]
        // CT(2) [2,2,2,2,4,   ]
        List<Tuple.Tuple2<LocalCompletionTimeWriterThread.WriteType, Time>> writeStream2 = Lists.newArrayList(
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(2)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(2)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(2)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(2)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(4)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(4)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(2)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(2)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(2)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(2)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(100))
        );

        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager =
                new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader reader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter writer1 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter writer2 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        LocalCompletionTimeWriterThread thread1 = new LocalCompletionTimeWriterThread(writer1, writeStream1.iterator(), errorReporter);
        LocalCompletionTimeWriterThread thread2 = new LocalCompletionTimeWriterThread(writer2, writeStream2.iterator(), errorReporter);

        thread1.start();
        thread2.start();

        boolean thread1CompletedOnTime = waitForLocalCompletionTimeWriterThread(Duration.fromSeconds(1), thread1);
        boolean thread2CompletedOnTime = waitForLocalCompletionTimeWriterThread(Duration.fromMilli(500), thread2);

        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

        // clean up threads
        if (false == thread1CompletedOnTime || false == thread2CompletedOnTime) {
            thread1.interrupt();
            thread2.interrupt();
        }
        assertThat(thread1CompletedOnTime, is(true));
        assertThat(thread2CompletedOnTime, is(true));

        // IT(1) [ , , , , ,6,7]
        // CT(1) [1,2,3,4,5, , ]
        // IT(2) [ , , , , ,100]
        // CT(2) [2,2,2,2,4,   ]
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(6)));
        assertThat(reader.localCompletionTime(), is(Time.fromMilli(5)));
    }

    @Test
    public void concurrentScenario2() throws CompletionTimeException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        // IT(1) [ , , , , ,6,7]
        // CT(1) [1,2,3,4,5, , ]
        List<Tuple.Tuple2<LocalCompletionTimeWriterThread.WriteType, Time>> writeStream1 = Lists.newArrayList(
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(2)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(3)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(4)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(5)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(2)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(3)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(4)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(5)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(6)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(7))
        );

        // IT(2) [ , , , , ,100]
        // CT(2) [2,2,2,2,4,   ]
        List<Tuple.Tuple2<LocalCompletionTimeWriterThread.WriteType, Time>> writeStream2 = Lists.newArrayList(
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(2)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(2)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(2)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(2)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(4)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(4)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(2)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(2)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(2)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(2)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(100))
        );

        // IT(3) []
        // CT(3) []
        List<Tuple.Tuple2<LocalCompletionTimeWriterThread.WriteType, Time>> writeStream3 = Lists.newArrayList(
        );

        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager =
                new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader reader = multiWriterLocalCompletionTimeConcurrentStateManager;

        LocalCompletionTimeWriter writer1 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter writer2 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter writer3 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        LocalCompletionTimeWriterThread thread1 = new LocalCompletionTimeWriterThread(writer1, writeStream1.iterator(), errorReporter);
        LocalCompletionTimeWriterThread thread2 = new LocalCompletionTimeWriterThread(writer2, writeStream2.iterator(), errorReporter);
        LocalCompletionTimeWriterThread thread3 = new LocalCompletionTimeWriterThread(writer3, writeStream3.iterator(), errorReporter);

        thread1.start();
        thread2.start();
        thread3.start();

        boolean thread1CompletedOnTime = waitForLocalCompletionTimeWriterThread(Duration.fromSeconds(1), thread1);
        boolean thread2CompletedOnTime = waitForLocalCompletionTimeWriterThread(Duration.fromMilli(500), thread2);
        boolean thread3CompletedOnTime = waitForLocalCompletionTimeWriterThread(Duration.fromMilli(500), thread3);

        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

        // clean up threads
        if (false == thread1CompletedOnTime || false == thread2CompletedOnTime) {
            thread1.interrupt();
            thread2.interrupt();
            thread3.interrupt();
        }
        assertThat(thread1CompletedOnTime, is(true));
        assertThat(thread2CompletedOnTime, is(true));
        assertThat(thread3CompletedOnTime, is(true));

        // IT(1) [ , , , , ,6,7]
        // CT(1) [1,2,3,4,5, , ]
        // IT(2) [ , , , , ,100]
        // CT(2) [2,2,2,2,4,   ]
        // IT(3) []
        // CT(3) []
        assertThat(reader.lastKnownLowestInitiatedTime(), is(nullValue()));
        assertThat(reader.localCompletionTime(), is(nullValue()));
    }

    @Test
    public void concurrentScenario3() throws CompletionTimeException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        // IT(1) [ , , , , , , , , , , , , , , , , , , , ,2]
        // CT(1) [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, ]
        List<Tuple.Tuple2<LocalCompletionTimeWriterThread.WriteType, Time>> writeStream1 = Lists.newArrayList(
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),

                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),

                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(2))
        );
        // IT(2) [ , , , , , , , , , , , , , , , , , , , ,2]
        // CT(2) [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, ]
        List<Tuple.Tuple2<LocalCompletionTimeWriterThread.WriteType, Time>> writeStream2 = Lists.newArrayList(
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),

                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),

                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(2))
        );
        // IT(3) [ , , , , , , , , , , , , , , , , , , , ,2]
        // CT(3) [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, ]
        List<Tuple.Tuple2<LocalCompletionTimeWriterThread.WriteType, Time>> writeStream3 = Lists.newArrayList(
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1)),

                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1)),

                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(2))
        );

        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager =
                new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader reader = multiWriterLocalCompletionTimeConcurrentStateManager;

        LocalCompletionTimeWriter writer1 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter writer2 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter writer3 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        LocalCompletionTimeWriterThread thread1 = new LocalCompletionTimeWriterThread(writer1, writeStream1.iterator(), errorReporter);
        LocalCompletionTimeWriterThread thread2 = new LocalCompletionTimeWriterThread(writer2, writeStream2.iterator(), errorReporter);
        LocalCompletionTimeWriterThread thread3 = new LocalCompletionTimeWriterThread(writer3, writeStream3.iterator(), errorReporter);

        thread1.start();
        thread2.start();
        thread3.start();

        boolean thread1CompletedOnTime = waitForLocalCompletionTimeWriterThread(Duration.fromSeconds(1), thread1);
        boolean thread2CompletedOnTime = waitForLocalCompletionTimeWriterThread(Duration.fromMilli(500), thread2);
        boolean thread3CompletedOnTime = waitForLocalCompletionTimeWriterThread(Duration.fromMilli(500), thread3);

        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

        // clean up threads
        if (false == thread1CompletedOnTime || false == thread2CompletedOnTime) {
            thread1.interrupt();
            thread2.interrupt();
            thread3.interrupt();
        }
        assertThat(thread1CompletedOnTime, is(true));
        assertThat(thread2CompletedOnTime, is(true));
        assertThat(thread3CompletedOnTime, is(true));

        // IT(1) [ , , , , , , , , , , , , , , , , , , , ,2]
        // CT(1) [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, ]
        // IT(2) [ , , , , , , , , , , , , , , , , , , , ,2]
        // CT(2) [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, ]
        // IT(3) [ , , , , , , , , , , , , , , , , , , , ,2]
        // CT(3) [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, ]
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(2)));
        assertThat(reader.localCompletionTime(), is(Time.fromMilli(1)));
    }

    @Test
    public void concurrentScenario4() throws CompletionTimeException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));

        Iterator<Tuple.Tuple2<LocalCompletionTimeWriterThread.WriteType, Time>> writeStream1 = Iterators.concat(
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1))),
                        10000),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(1))),
                        10000),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(100))),
                        10000),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(100))),
                        10000),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(1000))),
                        1)
        );

        Iterator<Tuple.Tuple2<LocalCompletionTimeWriterThread.WriteType, Time>> writeStream2 = Iterators.concat(
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(10))),
                        10000),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(10))),
                        10000),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(90))),
                        1000),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(90))),
                        1000),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(900))),
                        1),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(900))),
                        1),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(9000))),
                        1000),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(9000))),
                        1000)
        );

        Iterator<Tuple.Tuple2<LocalCompletionTimeWriterThread.WriteType, Time>> writeStream3 = Iterators.concat(
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(2))),
                        10000),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, Time.fromMilli(2))),
                        10000),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, Time.fromMilli(901))),
                        1)
        );

        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager =
                new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader reader = multiWriterLocalCompletionTimeConcurrentStateManager;

        LocalCompletionTimeWriter writer1 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter writer2 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter writer3 = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        LocalCompletionTimeWriterThread thread1 = new LocalCompletionTimeWriterThread(writer1, writeStream1, errorReporter);
        LocalCompletionTimeWriterThread thread2 = new LocalCompletionTimeWriterThread(writer2, writeStream2, errorReporter);
        LocalCompletionTimeWriterThread thread3 = new LocalCompletionTimeWriterThread(writer3, writeStream3, errorReporter);

        thread1.start();
        thread2.start();
        thread3.start();

        boolean thread1CompletedOnTime = waitForLocalCompletionTimeWriterThread(Duration.fromSeconds(5), thread1);
        boolean thread2CompletedOnTime = waitForLocalCompletionTimeWriterThread(Duration.fromSeconds(5), thread2);
        boolean thread3CompletedOnTime = waitForLocalCompletionTimeWriterThread(Duration.fromSeconds(5), thread3);

        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

        // clean up threads
        if (false == thread1CompletedOnTime || false == thread2CompletedOnTime) {
            thread1.interrupt();
            thread2.interrupt();
            thread3.interrupt();
        }
        assertThat(thread1CompletedOnTime, is(true));
        assertThat(thread2CompletedOnTime, is(true));
        assertThat(thread3CompletedOnTime, is(true));

        // IT(1) [    ,      ,1000]
        // CT(1) [1...,100...,    ]
        // IT(2) [     ,     ,   ,       ]
        // CT(2) [10...,90...,900,9000...]
        // IT(3) [    ,901]
        // CT(3) [2...,   ]
        assertThat(reader.lastKnownLowestInitiatedTime(), is(Time.fromMilli(901)));
        assertThat(reader.localCompletionTime(), is(Time.fromMilli(900)));
    }

    boolean waitForLocalCompletionTimeWriterThread(Duration timeoutDuration, LocalCompletionTimeWriterThread thread) throws CompletionTimeException {
        TimeSource timeSource = new SystemTimeSource();
        long endTimeAsMilli = timeSource.now().plus(timeoutDuration).asMilli();
        while (timeSource.nowAsMilli() < endTimeAsMilli) {
            if (thread.hasCompletedExecution()) break;
            Spinner.powerNap(Duration.fromMilli(100).asMilli());
        }
        return thread.hasCompletedExecution();
    }

    static class LocalCompletionTimeWriterThread extends Thread {
        static enum WriteType {
            WRITE_LIT,
            WRITE_LCT
        }

        private final LocalCompletionTimeWriter writer;
        private final AtomicBoolean continueExecuting = new AtomicBoolean(true);
        private final AtomicBoolean hasCompletedExecution = new AtomicBoolean(false);
        private final Iterator<Tuple.Tuple2<WriteType, Time>> writeStream;
        private final ConcurrentErrorReporter errorReporter;

        LocalCompletionTimeWriterThread(LocalCompletionTimeWriter writer,
                                        Iterator<Tuple.Tuple2<WriteType, Time>> writeStream,
                                        ConcurrentErrorReporter errorReporter) {
            this.writer = writer;
            this.writeStream = writeStream;
            this.errorReporter = errorReporter;
        }

        void shutdown() {
            continueExecuting.set(false);
        }

        boolean hasCompletedExecution() {
            return hasCompletedExecution.get();
        }

        @Override
        public void run() {
            try {
                while (continueExecuting.get()) {
                    if (writeStream.hasNext()) {
                        Tuple.Tuple2<WriteType, Time> write = writeStream.next();
                        WriteType writeType = write._1();
                        Time writeTime = write._2();
                        switch (writeType) {
                            case WRITE_LIT:
                                writer.submitLocalInitiatedTime(writeTime);
                                break;
                            case WRITE_LCT:
                                writer.submitLocalCompletedTime(writeTime);
                                break;
                        }
                    } else {
                        break;
                    }
                }
            } catch (CompletionTimeException e) {
                errorReporter.reportError(this, ConcurrentErrorReporter.stackTraceToString(e));
            }
            hasCompletedExecution.set(true);
        }
    }
}
