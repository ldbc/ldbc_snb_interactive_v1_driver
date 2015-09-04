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
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(-1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));
    }

    @Test
    public void shouldReturnNullWhenNoOperationsHaveCompletedWithOneWriter() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager
                = new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader reader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter writer = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When

        writer.submitLocalInitiatedTime(1);
        writer.submitLocalInitiatedTime(2);
        writer.submitLocalInitiatedTime(3);

        // Then
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));
    }

    @Test
    public void shouldReturnNullWhenEarliestInitiatedOperationHasNotCompletedWithOneWriter() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager
                = new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader reader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter writer = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When/Then

        writer.submitLocalInitiatedTime(1);
        writer.submitLocalInitiatedTime(2);
        writer.submitLocalInitiatedTime(3);

        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));

        writer.submitLocalCompletedTime(2);
        writer.submitLocalCompletedTime(3);

        // Then
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));
    }

    @Test
    public void shouldAdvanceToNextUncompletedEventAsInitiatedEventsAreCompletedWithOneWriter() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager
                = new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader reader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter writer = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When/Then
        writer.submitLocalInitiatedTime(1);
        writer.submitLocalInitiatedTime(2);
        writer.submitLocalInitiatedTime(3);

        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));

        writer.submitLocalCompletedTime(3);
        writer.submitLocalCompletedTime(1);

        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(2l));
        assertThat(reader.localCompletionTimeAsMilli(), is(1l));
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
        writer.submitLocalInitiatedTime(1);
        writer.submitLocalInitiatedTime(1);
        writer.submitLocalInitiatedTime(2);
        writer.submitLocalInitiatedTime(3);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));

        // IT [ ,1,2, ]
        // CT [1, , ,3]
        writer.submitLocalCompletedTime(3);
        writer.submitLocalCompletedTime(1);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));

        // IT [ , ,2, ]
        // CT [1,1, ,3]
        writer.submitLocalCompletedTime(1);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(2l));
        assertThat(reader.localCompletionTimeAsMilli(), is(1l));

        // IT [ , , , ]
        // CT [1,1,2,3]
        writer.submitLocalCompletedTime(2);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        // because we do not know if more Initiated Time == 3 will arrive
        assertThat(reader.localCompletionTimeAsMilli(), is(2l));

        // IT [ , , , ,3]
        // CT [1,1,2,3, ]
        writer.submitLocalInitiatedTime(3);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        // another Initiated Time == 3 arrived
        assertThat(reader.localCompletionTimeAsMilli(), is(2l));

        // IT [ , , , , ]
        // CT [1,1,2,3,3]
        writer.submitLocalCompletedTime(3);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        // because we do not know if more Initiated Time == 3 will arrive
        assertThat(reader.localCompletionTimeAsMilli(), is(2l));

        // IT [ , , , , ,4]
        // CT [1,1,2,3,3, ]
        writer.submitLocalInitiatedTime(4);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(4l));
        assertThat(reader.localCompletionTimeAsMilli(), is(3l));
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
        writer.submitLocalInitiatedTime(1);
        writer.submitLocalInitiatedTime(2);
        writer.submitLocalInitiatedTime(3);

        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));

        // IT [ ,2,3]
        // CT [1, , ]
        writer.submitLocalCompletedTime(1);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(2l));
        assertThat(reader.localCompletionTimeAsMilli(), is(1l));

        //apply initiated time equal to highest submitted initiated time AND equal to LCT <- should be ok
        // IT [ ,2,3,3]
        // CT [1, , , ]
        writer.submitLocalInitiatedTime(3);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(2l));
        assertThat(reader.localCompletionTimeAsMilli(), is(1l));

        // IT [ ,2, ,3]
        // CT [1, ,3, ]
        writer.submitLocalCompletedTime(3);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(2l));
        assertThat(reader.localCompletionTimeAsMilli(), is(1l));

        // IT [ ,2, ,3]
        // CT [1, ,3, ]
        boolean exceptionThrown = false;
        try {
            writer.submitLocalInitiatedTime(1);
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(2l));
        assertThat(reader.localCompletionTimeAsMilli(), is(1l));
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
        writer.submitLocalInitiatedTime(1);
        writer.submitLocalInitiatedTime(2);
        writer.submitLocalInitiatedTime(3);
        writer.submitLocalInitiatedTime(4);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));

        // IT [1,2, , ]
        // CT [ , ,3,4]
        writer.submitLocalCompletedTime(4);
        writer.submitLocalCompletedTime(3);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));

        // IT [ ,2, , ]
        // CT [1, ,3,4]
        writer.submitLocalCompletedTime(1);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(2l));
        assertThat(reader.localCompletionTimeAsMilli(), is(1l));

        // IT [ , , , ]
        // CT [1,2,3,4]
        writer.submitLocalCompletedTime(2);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(4l));
        assertThat(reader.localCompletionTimeAsMilli(), is(3l));
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
        writer.submitLocalInitiatedTime(1);
        writer.submitLocalInitiatedTime(2);
        writer.submitLocalInitiatedTime(3);
        writer.submitLocalInitiatedTime(4);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));

        // IT [1,2, , ]
        // CT [ , ,3,4]
        writer.submitLocalCompletedTime(3);
        writer.submitLocalCompletedTime(4);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));

        // IT [ ,2, , ]
        // CT [1, ,3,4]
        writer.submitLocalCompletedTime(1);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(2l));
        assertThat(reader.localCompletionTimeAsMilli(), is(1l));

        // IT [ , , , ]
        // CT [1,2,3,4]
        writer.submitLocalCompletedTime(2);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(4l));
        assertThat(reader.localCompletionTimeAsMilli(), is(3l));
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
        writer.submitLocalInitiatedTime(1);
        writer.submitLocalInitiatedTime(2);
        writer.submitLocalInitiatedTime(3);
        writer.submitLocalInitiatedTime(4);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));

        // IT [1,2, , ]
        // CT [ , ,3,4]
        writer.submitLocalCompletedTime(4);
        writer.submitLocalCompletedTime(3);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));

        // IT [1, , , ]
        // CT [ ,2,3,4]
        writer.submitLocalCompletedTime(2);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));

        // IT [ , , , ]
        // CT [1,2,3,4]
        writer.submitLocalCompletedTime(1);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(4l));
        assertThat(reader.localCompletionTimeAsMilli(), is(3l));
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
        writer.submitLocalInitiatedTime(1);
        writer.submitLocalInitiatedTime(2);
        writer.submitLocalInitiatedTime(3);

        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));

        // IT [ , , ]
        // CT [1,2,3]
        writer.submitLocalCompletedTime(1);
        writer.submitLocalCompletedTime(2);
        writer.submitLocalCompletedTime(3);

        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(reader.localCompletionTimeAsMilli(), is(2l));
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
        writer.submitLocalInitiatedTime(1);
        writer.submitLocalInitiatedTime(2);
        writer.submitLocalInitiatedTime(3);

        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));

        // IT [ ,2,3]
        // CT [1, , ]
        writer.submitLocalCompletedTime(1);

        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(2l));
        assertThat(reader.localCompletionTimeAsMilli(), is(1l));

        boolean exceptionThrown = false;
        try {
            // only one entry with DueTime=1 exists, this should throw exception
            writer.submitLocalCompletedTime(1);
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(2l));
        assertThat(reader.localCompletionTimeAsMilli(), is(1l));
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
        writer.submitLocalInitiatedTime(1);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));
        // IT [ ]
        // CT [1]
        writer.submitLocalCompletedTime(1);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));

        // IT [ ,2]
        // CT [1, ]
        writer.submitLocalInitiatedTime(2);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(2l));
        assertThat(reader.localCompletionTimeAsMilli(), is(1l));
        // IT [ , ]
        // CT [1,2]
        writer.submitLocalCompletedTime(2);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(2l));
        assertThat(reader.localCompletionTimeAsMilli(), is(1l));

        // IT [ , ,3]
        // CT [1,2, ]
        writer.submitLocalInitiatedTime(3);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(reader.localCompletionTimeAsMilli(), is(2l));

        // IT [ , ,3,4]
        // CT [1,2, , ]
        writer.submitLocalInitiatedTime(4);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(reader.localCompletionTimeAsMilli(), is(2l));

        // IT [ , ,3,4,5]
        // CT [1,2, , , ]
        writer.submitLocalInitiatedTime(5);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(reader.localCompletionTimeAsMilli(), is(2l));
        // IT [ , ,3,4, ]
        // CT [1,2, , ,5]
        writer.submitLocalCompletedTime(5);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(reader.localCompletionTimeAsMilli(), is(2l));

        // IT [ , ,3,4, ,5]
        // CT [1,2, , ,5, ]
        writer.submitLocalInitiatedTime(5);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(reader.localCompletionTimeAsMilli(), is(2l));
        // IT [ , ,3,4, , ]
        // CT [1,2, , ,5,5]
        writer.submitLocalCompletedTime(5);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(reader.localCompletionTimeAsMilli(), is(2l));

        // IT [ , ,3,4, , ,5]
        // CT [1,2, , ,5,5, ]
        writer.submitLocalInitiatedTime(5);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(reader.localCompletionTimeAsMilli(), is(2l));
        // IT [ , ,3,4, , , ]
        // CT [1,2, , ,5,5,5]
        writer.submitLocalCompletedTime(5);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(reader.localCompletionTimeAsMilli(), is(2l));

        // IT [ , , ,4, , , ]
        // CT [1,2,3, ,5,5,5]
        writer.submitLocalCompletedTime(3);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(4l));
        assertThat(reader.localCompletionTimeAsMilli(), is(3l));

        // IT [ , , , , , , ]
        // CT [1,2,3,4,5,5,5]
        writer.submitLocalCompletedTime(4);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(5l));
        assertThat(reader.localCompletionTimeAsMilli(), is(4l));

        // IT [ , , , , , , ,6]
        // CT [1,2,3,4,5,5,5, ]
        writer.submitLocalInitiatedTime(6);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(6l));
        assertThat(reader.localCompletionTimeAsMilli(), is(5l));
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
        writer.submitLocalInitiatedTime(1);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));
        // IT [ ]
        // CT [1]
        writer.submitLocalCompletedTime(1);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));

        // IT [ ,2]
        // CT [1, ]
        writer.submitLocalInitiatedTime(2);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(2l));
        assertThat(reader.localCompletionTimeAsMilli(), is(1l));
        // IT [ , ]
        // CT [1,2]
        writer.submitLocalCompletedTime(2);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(2l));
        assertThat(reader.localCompletionTimeAsMilli(), is(1l));

        // IT [ , ,3]
        // CT [1,2, ]
        writer.submitLocalInitiatedTime(3);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(reader.localCompletionTimeAsMilli(), is(2l));

        // IT [ , ,3,3]
        // CT [1,2, , ]
        writer.submitLocalInitiatedTime(3);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(reader.localCompletionTimeAsMilli(), is(2l));

        // IT [ , ,3,3,4]
        // CT [1,2, , , ]
        writer.submitLocalInitiatedTime(4);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(reader.localCompletionTimeAsMilli(), is(2l));

        // IT [ , , ,3,4]
        // CT [1,2,3, , ]
        writer.submitLocalCompletedTime(3);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(reader.localCompletionTimeAsMilli(), is(2l));

        // IT [ , , ,3, ]
        // CT [1,2,3, ,4]
        writer.submitLocalCompletedTime(4);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(reader.localCompletionTimeAsMilli(), is(2l));

        // IT [ , , , , ]
        // CT [1,2,3,3,4]
        writer.submitLocalCompletedTime(3);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(4l));
        assertThat(reader.localCompletionTimeAsMilli(), is(3l));

        // IT [ , , , , ,5]
        // CT [1,2,3,3,4, ]
        writer.submitLocalInitiatedTime(5);
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(5l));
        assertThat(reader.localCompletionTimeAsMilli(), is(4l));
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
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(-1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));
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
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(-1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));
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
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(-1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));
    }

    @Test
    public void shouldReturnNullWhenOneWriterAndOneInitiatedTimeAndNoCompletedTimes() throws CompletionTimeException {
        // Given
        MultiWriterLocalCompletionTimeConcurrentStateManager multiWriterLocalCompletionTimeConcurrentStateManager =
                new MultiWriterLocalCompletionTimeConcurrentStateManager();
        LocalCompletionTimeReader localCompletionTimeReader = multiWriterLocalCompletionTimeConcurrentStateManager;
        LocalCompletionTimeWriter localCompletionTimeWriter = multiWriterLocalCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();

        // When
        localCompletionTimeWriter.submitLocalInitiatedTime(1);

        // Then
        // IT(1) []
        // CT(1) []
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));
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
        localCompletionTimeWriter.submitLocalInitiatedTime(1);
        localCompletionTimeWriter.submitLocalInitiatedTime(2);
        localCompletionTimeWriter.submitLocalInitiatedTime(3);

        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));
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
        localCompletionTimeWriter1.submitLocalInitiatedTime(1);

        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(-1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));
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
        localCompletionTimeWriter1.submitLocalInitiatedTime(1);
        localCompletionTimeWriter2.submitLocalInitiatedTime(1);

        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));
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
        localCompletionTimeWriter1.submitLocalInitiatedTime(1);
        localCompletionTimeWriter1.submitLocalInitiatedTime(2);
        localCompletionTimeWriter1.submitLocalInitiatedTime(3);

        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(-1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));

        // IT(1) [1,2,3]
        // CT(1) [ , , ]
        // IT(2) [1,2,3]
        // CT(2) [ , , ]
        localCompletionTimeWriter2.submitLocalInitiatedTime(1);
        localCompletionTimeWriter2.submitLocalInitiatedTime(2);
        localCompletionTimeWriter2.submitLocalInitiatedTime(3);

        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));
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
        localCompletionTimeWriter1.submitLocalInitiatedTime(1);
        localCompletionTimeWriter1.submitLocalCompletedTime(1);

        // Then
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));
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
        localCompletionTimeWriter1.submitLocalInitiatedTime(1);
        localCompletionTimeWriter1.submitLocalCompletedTime(1);

        // Then
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(-1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));
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
        localCompletionTimeWriter1.submitLocalInitiatedTime(1);
        localCompletionTimeWriter1.submitLocalCompletedTime(1);

        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(-1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));

        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) []
        // CT(2) [1]
        localCompletionTimeWriter2.submitLocalInitiatedTime(1);
        localCompletionTimeWriter2.submitLocalCompletedTime(1);

        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));
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
        localCompletionTimeWriter1.submitLocalInitiatedTime(1);
        localCompletionTimeWriter1.submitLocalCompletedTime(1);

        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(-1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));

        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) []
        // CT(2) [2]
        localCompletionTimeWriter2.submitLocalInitiatedTime(2);
        localCompletionTimeWriter2.submitLocalCompletedTime(2);

        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));
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
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(-1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));

        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) []
        // CT(2) []
        writer1.submitLocalInitiatedTime(1);
        writer1.submitLocalCompletedTime(1);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(-1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));

        // IT(1) [ ,2]
        // CT(1) [1, ]
        // IT(2) []
        // CT(2) []
        writer1.submitLocalInitiatedTime(2);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(-1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));

        // IT(1) [ ,2]
        // CT(1) [1, ]
        // IT(2) [ , ]
        // CT(2) [ ,2]
        writer2.submitLocalInitiatedTime(2);
        writer2.submitLocalCompletedTime(2);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(2l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(1l));

        // IT(1) [ ,2]
        // CT(1) [1, ]
        // IT(2) [ , , ]
        // CT(2) [ ,2,3]
        writer2.submitLocalInitiatedTime(3);
        writer2.submitLocalCompletedTime(3);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(2l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(1l));

        // IT(1) [ ,2]
        // CT(1) [1, ]
        // IT(2) [ , , ,3,3,3]
        // CT(2) [ ,2,3, , , ]
        writer2.submitLocalInitiatedTime(3);
        writer2.submitLocalInitiatedTime(3);
        writer2.submitLocalInitiatedTime(3);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(2l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(1l));

        // IT(1) [ , ]
        // CT(1) [1,2]
        // IT(2) [ , , ,3,3,3]
        // CT(2) [ ,2,3, , , ]
        writer1.submitLocalCompletedTime(2);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(2l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(1l));

        // IT(1) [ , ,3]
        // CT(1) [1,2, ]
        // IT(2) [ , , ,3,3,3]
        // CT(2) [ ,2,3, , , ]
        writer1.submitLocalInitiatedTime(3);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(2l));

        // IT(1) [ , ,3,4]
        // CT(1) [1,2, , ]
        // IT(2) [ , , ,3,3,3]
        // CT(2) [ ,2,3, , , ]
        writer1.submitLocalInitiatedTime(4);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(2l));

        // IT(1) [ , ,3,4]
        // CT(1) [1,2, , ]
        // IT(2) [ , , , , , ]
        // CT(2) [ ,2,3,3,3,3]
        writer2.submitLocalCompletedTime(3);
        writer2.submitLocalCompletedTime(3);
        writer2.submitLocalCompletedTime(3);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(2l));

        // IT(1) [ , ,3,4]
        // CT(1) [1,2, , ]
        // IT(2) [ , , , , , , ]
        // CT(2) [ ,2,3,3,3,3,4]
        writer2.submitLocalInitiatedTime(4);
        writer2.submitLocalCompletedTime(4);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(2l));

        // IT(1) [ , ,3, ]
        // CT(1) [1,2, ,4]
        // IT(2) [ , , , , , , ]
        // CT(2) [ ,2,3,3,3,3,4]
        writer1.submitLocalCompletedTime(4);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(2l));

        // IT(1) [ , ,3, ,5]
        // CT(1) [1,2, ,4, ]
        // IT(2) [ , , , , , , ]
        // CT(2) [ ,2,3,3,3,3,4]
        writer1.submitLocalInitiatedTime(5);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(2l));

        // IT(1) [ , ,3, , ]
        // CT(1) [1,2, ,4,5]
        // IT(2) [ , , , , , , ]
        // CT(2) [ ,2,3,3,3,3,4]
        writer1.submitLocalCompletedTime(5);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(2l));

        // IT(1) [ , ,3, , ]
        // CT(1) [1,2, ,4,5]
        // IT(2) [ , , , , , , , ]
        // CT(2) [ ,2,3,3,3,3,4,5]
        writer2.submitLocalInitiatedTime(5);
        writer2.submitLocalCompletedTime(5);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(2l));

        // IT(1) [ , , , , ]
        // CT(1) [1,2,3,4,5]
        // IT(2) [ , , , , , , , ]
        // CT(2) [ ,2,3,3,3,3,4,5]
        writer1.submitLocalCompletedTime(3);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(5l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(4l));

        // IT(1) [ , , , , ]
        // CT(1) [1,2,3,4,5]
        // IT(2) [ , , , , , , , ]
        // CT(2) [ ,2,3,3,3,3,4,5]
        boolean exceptionThrown = false;
        try {
            writer2.submitLocalInitiatedTime(4);
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(5l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(4l));
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
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(-1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));

        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) []
        // CT(2) []
        writer1.submitLocalInitiatedTime(1);
        writer1.submitLocalCompletedTime(1);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(-1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));

        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) []
        // CT(2) [1]
        writer2.submitLocalInitiatedTime(1);
        writer2.submitLocalCompletedTime(1);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));

        // IT(1) [ ]
        // CT(1) [1]
        // IT(2) [ , ]
        // CT(2) [1,2]
        writer2.submitLocalInitiatedTime(2);
        writer2.submitLocalCompletedTime(2);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));

        // IT(1) [ , ]
        // CT(1) [1,2]
        // IT(2) [ , ]
        // CT(2) [1,2]
        writer1.submitLocalInitiatedTime(2);
        writer1.submitLocalCompletedTime(2);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(2l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(1l));

        // IT(1) [ , ,10]
        // CT(1) [1,2,  ]
        // IT(2) [ , ]
        // CT(2) [1,2]
        writer1.submitLocalInitiatedTime(10);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(2l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(1l));

        // IT(1) [ , ,10]
        // CT(1) [1,2,  ]
        // IT(2) [ , , ]
        // CT(2) [1,2,3]
        writer2.submitLocalInitiatedTime(3);
        writer2.submitLocalCompletedTime(3);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(2l));

        // IT(1) [ , ,10]
        // CT(1) [1,2,  ]
        // IT(2) [ , , ,  ]
        // CT(2) [1,2,3,10]
        writer2.submitLocalInitiatedTime(10);
        writer2.submitLocalCompletedTime(10);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(10l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(3l));
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
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(-1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) []
        // CT(2) []
        writer1.submitLocalInitiatedTime(10);
        writer1.submitLocalCompletedTime(10);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(-1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [1]
        // CT(2) [ ]
        writer2.submitLocalInitiatedTime(1);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [1,2]
        // CT(2) [ , ]
        writer2.submitLocalInitiatedTime(2);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [1,2,3]
        // CT(2) [ , ]
        writer2.submitLocalInitiatedTime(3);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(1l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(-1l));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ ,2,3]
        // CT(2) [1, , ]
        writer2.submitLocalCompletedTime(1);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(2l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(1l));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , ,3]
        // CT(2) [1,2, ]
        writer2.submitLocalCompletedTime(2);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(2l));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , ,3,5]
        // CT(2) [1,2, , ]
        writer2.submitLocalInitiatedTime(5);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(2l));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , ,3,5,9]
        // CT(2) [1,2, , , ]
        writer2.submitLocalInitiatedTime(9);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(2l));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , ,3,5,9,20]
        // CT(2) [1,2, , , ,  ]
        writer2.submitLocalInitiatedTime(20);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(3l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(2l));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , , ,5,9,20]
        // CT(2) [1,2,3, , ,  ]
        writer2.submitLocalCompletedTime(3);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(5l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(3l));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , , ,5, ,20]
        // CT(2) [1,2,3, ,9,  ]
        writer2.submitLocalCompletedTime(9);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(5l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(3l));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , , , , ,20]
        // CT(2) [1,2,3,5,9,  ]
        writer2.submitLocalCompletedTime(5);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(10l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(9l));

        // IT(1) [ ]
        // CT(1) [10]
        // IT(2) [ , , , , ,  ]
        // CT(2) [1,2,3,5,9,20]
        writer2.submitLocalCompletedTime(20);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(10l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(9l));

        // IT(1) [  ,11]
        // CT(1) [10, ]
        // IT(2) [ , , , , ,  ]
        // CT(2) [1,2,3,5,9,20]
        writer1.submitLocalInitiatedTime(11);
        assertThat(localCompletionTimeReader.lastKnownLowestInitiatedTimeAsMilli(), is(11l));
        assertThat(localCompletionTimeReader.localCompletionTimeAsMilli(), is(10l));
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
        List<Tuple2<LocalCompletionTimeWriterThread.WriteType, Long>> writeStream1 = Lists.newArrayList(
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 2l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 3l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 4l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 5l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 2l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 3l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 4l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 5l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 6l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 7l)
        );

        // IT(2) [ , , , , ,100]
        // CT(2) [2,2,2,2,4,   ]
        List<Tuple2<LocalCompletionTimeWriterThread.WriteType, Long>> writeStream2 = Lists.newArrayList(
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 2l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 2l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 2l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 2l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 4l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 4l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 2l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 2l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 2l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 2l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 100l)
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

        boolean thread1CompletedOnTime = waitForLocalCompletionTimeWriterThread(1000, thread1);
        boolean thread2CompletedOnTime = waitForLocalCompletionTimeWriterThread(500, thread2);

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
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(6l));
        assertThat(reader.localCompletionTimeAsMilli(), is(5l));
    }

    @Test
    public void concurrentScenario2() throws CompletionTimeException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        // IT(1) [ , , , , ,6,7]
        // CT(1) [1,2,3,4,5, , ]
        List<Tuple2<LocalCompletionTimeWriterThread.WriteType, Long>> writeStream1 = Lists.newArrayList(
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 2l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 3l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 4l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 5l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 2l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 3l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 4l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 5l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 6l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 7l)
        );

        // IT(2) [ , , , , ,100]
        // CT(2) [2,2,2,2,4,   ]
        List<Tuple2<LocalCompletionTimeWriterThread.WriteType, Long>> writeStream2 = Lists.newArrayList(
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 2l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 2l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 2l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 2l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 4l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 4l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 2l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 2l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 2l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 2l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 100l)
        );

        // IT(3) []
        // CT(3) []
        List<Tuple2<LocalCompletionTimeWriterThread.WriteType, Long>> writeStream3 = Lists.newArrayList(
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

        boolean thread1CompletedOnTime = waitForLocalCompletionTimeWriterThread(1000, thread1);
        boolean thread2CompletedOnTime = waitForLocalCompletionTimeWriterThread(500, thread2);
        boolean thread3CompletedOnTime = waitForLocalCompletionTimeWriterThread(500, thread3);

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
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(-1l));
        assertThat(reader.localCompletionTimeAsMilli(), is(-1l));
    }

    @Test
    public void concurrentScenario3() throws CompletionTimeException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        // IT(1) [ , , , , , , , , , , , , , , , , , , , ,2]
        // CT(1) [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, ]
        List<Tuple2<LocalCompletionTimeWriterThread.WriteType, Long>> writeStream1 = Lists.newArrayList(
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),

                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),

                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 2l)
        );
        // IT(2) [ , , , , , , , , , , , , , , , , , , , ,2]
        // CT(2) [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, ]
        List<Tuple2<LocalCompletionTimeWriterThread.WriteType, Long>> writeStream2 = Lists.newArrayList(
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),

                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),

                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 2l)
        );
        // IT(3) [ , , , , , , , , , , , , , , , , , , , ,2]
        // CT(3) [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, ]
        List<Tuple2<LocalCompletionTimeWriterThread.WriteType, Long>> writeStream3 = Lists.newArrayList(
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l),

                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),
                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l),

                Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 2l)
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

        boolean thread1CompletedOnTime = waitForLocalCompletionTimeWriterThread(1000, thread1);
        boolean thread2CompletedOnTime = waitForLocalCompletionTimeWriterThread(500, thread2);
        boolean thread3CompletedOnTime = waitForLocalCompletionTimeWriterThread(500, thread3);

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
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(2l));
        assertThat(reader.localCompletionTimeAsMilli(), is(1l));
    }

    @Test
    public void concurrentScenario4() throws CompletionTimeException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));

        Iterator<Tuple2<LocalCompletionTimeWriterThread.WriteType, Long>> writeStream1 = Iterators.concat(
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1l)),
                        10000),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 1l)),
                        10000),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 100l)),
                        10000),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 100l)),
                        10000),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 1000l)),
                        1)
        );

        Iterator<Tuple2<LocalCompletionTimeWriterThread.WriteType, Long>> writeStream2 = Iterators.concat(
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 10l)),
                        10000),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 10l)),
                        10000),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 90l)),
                        1000),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 90l)),
                        1000),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 900l)),
                        1),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 900l)),
                        1),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 9000l)),
                        1000),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 9000l)),
                        1000)
        );

        Iterator<Tuple2<LocalCompletionTimeWriterThread.WriteType, Long>> writeStream3 = Iterators.concat(
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 2l)),
                        10000),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LCT, 2l)),
                        10000),
                gf.limit(
                        gf.constant(Tuple.tuple2(LocalCompletionTimeWriterThread.WriteType.WRITE_LIT, 901l)),
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

        boolean thread1CompletedOnTime = waitForLocalCompletionTimeWriterThread(5000, thread1);
        boolean thread2CompletedOnTime = waitForLocalCompletionTimeWriterThread(5000, thread2);
        boolean thread3CompletedOnTime = waitForLocalCompletionTimeWriterThread(5000, thread3);

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
        assertThat(reader.lastKnownLowestInitiatedTimeAsMilli(), is(901l));
        assertThat(reader.localCompletionTimeAsMilli(), is(900l));
    }

    boolean waitForLocalCompletionTimeWriterThread(long timeoutDurationAsMilli, LocalCompletionTimeWriterThread thread) throws CompletionTimeException {
        TimeSource timeSource = new SystemTimeSource();
        long endTimeAsMilli = timeSource.nowAsMilli() + timeoutDurationAsMilli;
        while (timeSource.nowAsMilli() < endTimeAsMilli) {
            if (thread.hasCompletedExecution()) break;
            Spinner.powerNap(100);
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
        private final Iterator<Tuple2<WriteType, Long>> writeStream;
        private final ConcurrentErrorReporter errorReporter;

        LocalCompletionTimeWriterThread(LocalCompletionTimeWriter writer,
                                        Iterator<Tuple2<WriteType, Long>> writeStream,
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
                        Tuple2<WriteType, Long> write = writeStream.next();
                        WriteType writeType = write._1();
                        long writeTimeAsMilli = write._2();
                        switch (writeType) {
                            case WRITE_LIT:
                                writer.submitLocalInitiatedTime(writeTimeAsMilli);
                                break;
                            case WRITE_LCT:
                                writer.submitLocalCompletedTime(writeTimeAsMilli);
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
