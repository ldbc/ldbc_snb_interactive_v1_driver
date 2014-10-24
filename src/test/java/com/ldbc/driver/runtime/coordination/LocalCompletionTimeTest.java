package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.temporal.Time;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class LocalCompletionTimeTest {
    @Test
    public void shouldReturnNullWhenNoEventsHaveBeenInitiatedOrCompleted() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When / Then
        // no events have been initiated or completed

        // IT []
        // CT []
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(nullValue()));
        assertThat(lct.localCompletionTimeAsMilli(), is(nullValue()));
    }

    @Test
    public void shouldReturnNullWhenNoOperationsHaveCompleted() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When / Then

        // IT [1,2,3]
        // CT [ , , ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(1));
        lct.submitLocalInitiatedTime(Time.fromSeconds(2));
        lct.submitLocalInitiatedTime(Time.fromSeconds(3));

        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(1)));
        assertThat(lct.localCompletionTimeAsMilli(), is(nullValue()));
    }

    @Test
    public void shouldReturnNullWhenEarliestInitiatedOperationHasNotCompleted() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When / Then

        // IT [1,2,3]
        // CT [ , , ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(1));
        lct.submitLocalInitiatedTime(Time.fromSeconds(2));
        lct.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(1)));
        assertThat(lct.localCompletionTimeAsMilli(), is(nullValue()));

        // IT [1, , ]
        // CT [ ,2,3]
        lct.submitLocalCompletedTime(Time.fromSeconds(2));
        lct.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(1)));
        assertThat(lct.localCompletionTimeAsMilli(), is(nullValue()));
    }

    @Test
    public void shouldAdvanceToNextUncompletedEventAsInitiatedEventsAreCompleted() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When / Then
        // IT [1,2,3]
        // CT [ , , ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(1));
        lct.submitLocalInitiatedTime(Time.fromSeconds(2));
        lct.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(1)));
        assertThat(lct.localCompletionTimeAsMilli(), is(nullValue()));

        // IT [ ,2, ]
        // CT [1, ,3]
        lct.submitLocalCompletedTime(Time.fromSeconds(3));
        lct.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(2)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(1)));
    }

    @Test
    public void shouldAdvanceToNextUncompletedEventAsInitiatedEventsAreCompletedButOnlyIfNoUncompletedTimesExistAtSameTime() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When/Then

        // IT [1,1,2,3]
        // CT [ , , , ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(1));
        lct.submitLocalInitiatedTime(Time.fromSeconds(1));
        lct.submitLocalInitiatedTime(Time.fromSeconds(2));
        lct.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(1)));
        assertThat(lct.localCompletionTimeAsMilli(), is(nullValue()));

        // IT [ ,1,2, ]
        // CT [1, , ,3]
        lct.submitLocalCompletedTime(Time.fromSeconds(3));
        lct.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(1)));
        assertThat(lct.localCompletionTimeAsMilli(), is(nullValue()));

        // IT [ , ,2, ]
        // CT [1,1, ,3]
        lct.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(2)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(1)));

        // IT [ , , , ]
        // CT [1,1,2,3]
        lct.submitLocalCompletedTime(Time.fromSeconds(2));
        // because we do not know if more Initiated Time == 3 will arrive
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(3)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(2)));

        // IT [ , , , ,3]
        // CT [1,1,2,3, ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(3));
        // another Initiated Time == 3 arrived
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(3)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(2)));

        // IT [ , , , , ]
        // CT [1,1,2,3,3]
        lct.submitLocalCompletedTime(Time.fromSeconds(3));
        // because we do not know if more Initiated Time == 3 will arrive
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(3)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(2)));

        // IT [ , , , , ,4]
        // CT [1,1,2,3,3, ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(4));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(4)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(3)));
    }

    @Test
    public void shouldAllowForSubmittedInitiatedTimeToEqualCurrentCompletionTimeButNotBeLower() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When/Then
        // IT [1,2,3]
        // CT [ , , ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(1));
        lct.submitLocalInitiatedTime(Time.fromSeconds(2));
        lct.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(1)));
        assertThat(lct.localCompletionTimeAsMilli(), is(nullValue()));

        // IT [ ,2,3]
        // CT [1, , ]
        lct.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(2)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(1)));

        //apply initiated time equal to highest submitted initiated time AND equal to GCT <- should be ok
        // IT [ ,2,3,3]
        // CT [1, , , ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(2)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(1)));

        // IT [ ,2, ,3]
        // CT [1, ,3, ]
        lct.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(2)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(1)));

        // IT [ ,2, ,3]
        // CT [1, ,3, ]
        boolean exceptionThrown = false;
        try {
            lct.submitLocalInitiatedTime(Time.fromSeconds(1));
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(2)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(1)));
        assertThat(exceptionThrown, is(true));
    }

    @Test
    public void shouldAdvanceToNextUncompletedEventUntilAllInitiatedEventsAreCompletedWhenCompletedTimesComeInOutOfOrder1() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When/Then
        // IT [1,2,3,4]
        // CT [ , , , ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(1));
        lct.submitLocalInitiatedTime(Time.fromSeconds(2));
        lct.submitLocalInitiatedTime(Time.fromSeconds(3));
        lct.submitLocalInitiatedTime(Time.fromSeconds(4));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(1)));
        assertThat(lct.localCompletionTimeAsMilli(), is(nullValue()));

        // IT [1,2, , ]
        // CT [ , ,3,4]
        lct.submitLocalCompletedTime(Time.fromSeconds(4));
        lct.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(1)));
        assertThat(lct.localCompletionTimeAsMilli(), is(nullValue()));

        // IT [ ,2, , ]
        // CT [1, ,3,4]
        lct.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(2)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(1)));

        // IT [ , , , ]
        // CT [1,2,3,4]
        lct.submitLocalCompletedTime(Time.fromSeconds(2));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(4)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(3)));
    }

    @Test
    public void shouldAdvanceToNextUncompletedEventUntilAllInitiatedEventsAreCompletedWhenCompletedTimesComeInOutOfOrder2() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When/Then
        // IT [1,2,3,4]
        // CT [ , , , ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(1));
        lct.submitLocalInitiatedTime(Time.fromSeconds(2));
        lct.submitLocalInitiatedTime(Time.fromSeconds(3));
        lct.submitLocalInitiatedTime(Time.fromSeconds(4));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(1)));
        assertThat(lct.localCompletionTimeAsMilli(), is(nullValue()));

        // IT [1,2, , ]
        // CT [ , ,3,4]
        lct.submitLocalCompletedTime(Time.fromSeconds(3));
        lct.submitLocalCompletedTime(Time.fromSeconds(4));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(1)));
        assertThat(lct.localCompletionTimeAsMilli(), is(nullValue()));

        // IT [ ,2, , ]
        // CT [1, ,3,4]
        lct.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(2)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(1)));

        // IT [ , , , ]
        // CT [1,2,3,4]
        lct.submitLocalCompletedTime(Time.fromSeconds(2));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(4)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(3)));
    }

    @Test
    public void shouldAdvanceToNextUncompletedEventUntilAllInitiatedEventsAreCompletedWhenCompletedTimesComeInOutOfOrder3() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When/Then
        // IT [1,2,3,4]
        // CT [ , , , ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(1));
        lct.submitLocalInitiatedTime(Time.fromSeconds(2));
        lct.submitLocalInitiatedTime(Time.fromSeconds(3));
        lct.submitLocalInitiatedTime(Time.fromSeconds(4));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(1)));
        assertThat(lct.localCompletionTimeAsMilli(), is(nullValue()));

        // IT [1,2, , ]
        // CT [ , ,3,4]
        lct.submitLocalCompletedTime(Time.fromSeconds(4));
        lct.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(1)));
        assertThat(lct.localCompletionTimeAsMilli(), is(nullValue()));

        // IT [1, , , ]
        // CT [ ,2,3,4]
        lct.submitLocalCompletedTime(Time.fromSeconds(2));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(1)));
        assertThat(lct.localCompletionTimeAsMilli(), is(nullValue()));

        // IT [ , , , ]
        // CT [1,2,3,4]
        lct.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(4)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(3)));
    }

    @Test
    public void shouldReturnLatestInitiatedEventTimeWhenAllEventsHaveCompleted() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When/Then

        // IT [1,2,3]
        // CT [ , , ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(1));
        lct.submitLocalInitiatedTime(Time.fromSeconds(2));
        lct.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(1)));
        assertThat(lct.localCompletionTimeAsMilli(), is(nullValue()));

        // IT [ , , ]
        // CT [1,2,3]
        lct.submitLocalCompletedTime(Time.fromSeconds(1));
        lct.submitLocalCompletedTime(Time.fromSeconds(2));
        lct.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(3)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(2)));
    }

    public void shouldThrowExceptionWhenEventCompletesThatHasNoMatchingInitiatedEntry() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When/Then

        // IT [1,2,3]
        // CT [ , , ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(1));
        lct.submitLocalInitiatedTime(Time.fromSeconds(2));
        lct.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(1)));
        assertThat(lct.localCompletionTimeAsMilli(), is(nullValue()));

        // IT [ ,2,3]
        // CT [1, , ]
        lct.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(2)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromMilli(1)));

        boolean exceptionThrown = false;
        try {
            // only one entry with DueTime=1 exists, this should throw exception
            lct.submitLocalCompletedTime(Time.fromSeconds(1));
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(2)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromMilli(1)));
    }

    @Test
    public void shouldReturnLatestTimeBehindWhichThereAreNoUncompletedITEvents() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When/Then
        // IT [1]
        // CT []
        lct.submitLocalInitiatedTime(Time.fromSeconds(1));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(1)));
        assertThat(lct.localCompletionTimeAsMilli(), is(nullValue()));
        // IT [ ]
        // CT [1]
        lct.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(1)));
        assertThat(lct.localCompletionTimeAsMilli(), is(nullValue()));

        // IT [ ,2]
        // CT [1, ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(2));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(2)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(1)));
        // IT [ , ]
        // CT [1,2]
        lct.submitLocalCompletedTime(Time.fromSeconds(2));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(2)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(1)));

        // IT [ , ,3]
        // CT [1,2, ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(3)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(2)));

        // IT [ , ,3,4]
        // CT [1,2, , ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(4));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(3)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(2)));

        // IT [ , ,3,4,5]
        // CT [1,2, , , ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(5));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(3)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(2)));

        // IT [ , ,3,4, ]
        // CT [1,2, , ,5]
        lct.submitLocalCompletedTime(Time.fromSeconds(5));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(3)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(2)));

        // IT [ , ,3,4, ,5]
        // CT [1,2, , ,5, ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(5));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(3)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(2)));

        // IT [ , ,3,4, , ]
        // CT [1,2, , ,5,5]
        lct.submitLocalCompletedTime(Time.fromSeconds(5));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(3)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(2)));

        // IT [ , ,3,4, , ,5]
        // CT [1,2, , ,5,5, ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(5));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(3)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(2)));

        // IT [ , ,3,4, , , ]
        // CT [1,2, , ,5,5,5]
        lct.submitLocalCompletedTime(Time.fromSeconds(5));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(3)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(2)));

        // IT [ , , ,4, , , ]
        // CT [1,2,3, ,5,5,5]
        lct.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(4)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(3)));

        // IT [ , , , , , , ]
        // CT [1,2,3,4,5,5,5]
        lct.submitLocalCompletedTime(Time.fromSeconds(4));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(5)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(4)));

        // IT [ , , , , , , ,6]
        // CT [1,2,3,4,5,5,5, ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(6));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(6)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(5)));
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNotMatchingCTEvenWhenMultipleEventsHaveSameInitiatedTime() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When/Then
        // IT [1]
        // CT [ ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(1));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(1)));
        assertThat(lct.localCompletionTimeAsMilli(), is(nullValue()));

        // IT [ ]
        // CT [1]
        lct.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(1)));
        assertThat(lct.localCompletionTimeAsMilli(), is(nullValue()));

        // IT [ ,2]
        // CT [1, ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(2));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(2)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(1)));

        // IT [ , ]
        // CT [1,2]
        lct.submitLocalCompletedTime(Time.fromSeconds(2));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(2)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(1)));

        // IT [ , ,3]
        // CT [1,2, ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(3)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(2)));

        // IT [ , ,3,3]
        // CT [1,2, , ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(3)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(2)));

        // IT [ , ,3,3,4]
        // CT [1,2, , , ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(4));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(3)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(2)));

        // IT [ , , ,3,4]
        // CT [1,2,3, , ]
        lct.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(3)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(2)));

        // IT [ , , ,3, ]
        // CT [1,2,3, ,4]
        lct.submitLocalCompletedTime(Time.fromSeconds(4));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(3)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(2)));

        // IT [ , , , , ]
        // CT [1,2,3,3,4]
        lct.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(4)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(3)));

        // IT [ , , , , ,5]
        // CT [1,2,3,3,4, ]
        lct.submitLocalInitiatedTime(Time.fromSeconds(5));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(Time.fromSeconds(5)));
        assertThat(lct.localCompletionTimeAsMilli(), is(Time.fromSeconds(4)));
    }
}