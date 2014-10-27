package com.ldbc.driver.runtime.coordination;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
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
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(-1l));
        assertThat(lct.localCompletionTimeAsMilli(), is(-1l));
    }

    @Test
    public void shouldReturnNullWhenNoOperationsHaveCompleted() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When / Then

        // IT [1,2,3]
        // CT [ , , ]
        lct.submitLocalInitiatedTime(1000l);
        lct.submitLocalInitiatedTime(2000l);
        lct.submitLocalInitiatedTime(3000l);

        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(1000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(-1l));
    }

    @Test
    public void shouldReturnNullWhenEarliestInitiatedOperationHasNotCompleted() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When / Then

        // IT [1,2,3]
        // CT [ , , ]
        lct.submitLocalInitiatedTime(1000l);
        lct.submitLocalInitiatedTime(2000l);
        lct.submitLocalInitiatedTime(3000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(1000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(-1l));

        // IT [1, , ]
        // CT [ ,2,3]
        lct.submitLocalCompletedTime(2000l);
        lct.submitLocalCompletedTime(3000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(1000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(-1l));
    }

    @Test
    public void shouldAdvanceToNextUncompletedEventAsInitiatedEventsAreCompleted() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When / Then
        // IT [1,2,3]
        // CT [ , , ]
        lct.submitLocalInitiatedTime(1000l);
        lct.submitLocalInitiatedTime(2000l);
        lct.submitLocalInitiatedTime(3000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(1000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(-1l));

        // IT [ ,2, ]
        // CT [1, ,3]
        lct.submitLocalCompletedTime(3000l);
        lct.submitLocalCompletedTime(1000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(2000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(1000l));
    }

    @Test
    public void shouldAdvanceToNextUncompletedEventAsInitiatedEventsAreCompletedButOnlyIfNoUncompletedTimesExistAtSameTime() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When/Then

        // IT [1,1,2,3]
        // CT [ , , , ]
        lct.submitLocalInitiatedTime(1000l);
        lct.submitLocalInitiatedTime(1000l);
        lct.submitLocalInitiatedTime(2000l);
        lct.submitLocalInitiatedTime(3000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(1000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(-1l));

        // IT [ ,1,2, ]
        // CT [1, , ,3]
        lct.submitLocalCompletedTime(3000l);
        lct.submitLocalCompletedTime(1000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(1000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(-1l));

        // IT [ , ,2, ]
        // CT [1,1, ,3]
        lct.submitLocalCompletedTime(1000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(2000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(1000l));

        // IT [ , , , ]
        // CT [1,1,2,3]
        lct.submitLocalCompletedTime(2000l);
        // because we do not know if more Initiated Time == 3 will arrive
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(3000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(2000l));

        // IT [ , , , ,3]
        // CT [1,1,2,3, ]
        lct.submitLocalInitiatedTime(3000l);
        // another Initiated Time == 3 arrived
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(3000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(2000l));

        // IT [ , , , , ]
        // CT [1,1,2,3,3]
        lct.submitLocalCompletedTime(3000l);
        // because we do not know if more Initiated Time == 3 will arrive
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(3000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(2000l));

        // IT [ , , , , ,4]
        // CT [1,1,2,3,3, ]
        lct.submitLocalInitiatedTime(4000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(4000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(3000l));
    }

    @Test
    public void shouldAllowForSubmittedInitiatedTimeToEqualCurrentCompletionTimeButNotBeLower() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When/Then
        // IT [1,2,3]
        // CT [ , , ]
        lct.submitLocalInitiatedTime(1000l);
        lct.submitLocalInitiatedTime(2000l);
        lct.submitLocalInitiatedTime(3000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(1000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(-1l));

        // IT [ ,2,3]
        // CT [1, , ]
        lct.submitLocalCompletedTime(1000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(2000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(1000l));

        //apply initiated time equal to highest submitted initiated time AND equal to GCT <- should be ok
        // IT [ ,2,3,3]
        // CT [1, , , ]
        lct.submitLocalInitiatedTime(3000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(2000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(1000l));

        // IT [ ,2, ,3]
        // CT [1, ,3, ]
        lct.submitLocalCompletedTime(3000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(2000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(1000l));

        // IT [ ,2, ,3]
        // CT [1, ,3, ]
        boolean exceptionThrown = false;
        try {
            lct.submitLocalInitiatedTime(1000l);
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(2000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(1000l));
        assertThat(exceptionThrown, is(true));
    }

    @Test
    public void shouldAdvanceToNextUncompletedEventUntilAllInitiatedEventsAreCompletedWhenCompletedTimesComeInOutOfOrder1() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When/Then
        // IT [1,2,3,4]
        // CT [ , , , ]
        lct.submitLocalInitiatedTime(1000l);
        lct.submitLocalInitiatedTime(2000l);
        lct.submitLocalInitiatedTime(3000l);
        lct.submitLocalInitiatedTime(4000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(1000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(-1l));

        // IT [1,2, , ]
        // CT [ , ,3,4]
        lct.submitLocalCompletedTime(4000l);
        lct.submitLocalCompletedTime(3000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(1000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(-1l));

        // IT [ ,2, , ]
        // CT [1, ,3,4]
        lct.submitLocalCompletedTime(1000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(2000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(1000l));

        // IT [ , , , ]
        // CT [1,2,3,4]
        lct.submitLocalCompletedTime(2000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(4000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(3000l));
    }

    @Test
    public void shouldAdvanceToNextUncompletedEventUntilAllInitiatedEventsAreCompletedWhenCompletedTimesComeInOutOfOrder2() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When/Then
        // IT [1,2,3,4]
        // CT [ , , , ]
        lct.submitLocalInitiatedTime(1000l);
        lct.submitLocalInitiatedTime(2000l);
        lct.submitLocalInitiatedTime(3000l);
        lct.submitLocalInitiatedTime(4000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(1000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(-1l));

        // IT [1,2, , ]
        // CT [ , ,3,4]
        lct.submitLocalCompletedTime(3000l);
        lct.submitLocalCompletedTime(4000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(1000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(-1l));

        // IT [ ,2, , ]
        // CT [1, ,3,4]
        lct.submitLocalCompletedTime(1000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(2000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(1000l));

        // IT [ , , , ]
        // CT [1,2,3,4]
        lct.submitLocalCompletedTime(2000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(4000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(3000l));
    }

    @Test
    public void shouldAdvanceToNextUncompletedEventUntilAllInitiatedEventsAreCompletedWhenCompletedTimesComeInOutOfOrder3() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When/Then
        // IT [1,2,3,4]
        // CT [ , , , ]
        lct.submitLocalInitiatedTime(1000l);
        lct.submitLocalInitiatedTime(2000l);
        lct.submitLocalInitiatedTime(3000l);
        lct.submitLocalInitiatedTime(4000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(1000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(-1l));

        // IT [1,2, , ]
        // CT [ , ,3,4]
        lct.submitLocalCompletedTime(4000l);
        lct.submitLocalCompletedTime(3000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(1000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(-1l));

        // IT [1, , , ]
        // CT [ ,2,3,4]
        lct.submitLocalCompletedTime(2000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(1000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(-1l));

        // IT [ , , , ]
        // CT [1,2,3,4]
        lct.submitLocalCompletedTime(1000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(4000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(3000l));
    }

    @Test
    public void shouldReturnLatestInitiatedEventTimeWhenAllEventsHaveCompleted() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When/Then

        // IT [1,2,3]
        // CT [ , , ]
        lct.submitLocalInitiatedTime(1000l);
        lct.submitLocalInitiatedTime(2000l);
        lct.submitLocalInitiatedTime(3000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(1000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(-1l));

        // IT [ , , ]
        // CT [1,2,3]
        lct.submitLocalCompletedTime(1000l);
        lct.submitLocalCompletedTime(2000l);
        lct.submitLocalCompletedTime(3000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(3000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(2000l));
    }

    @Test
    public void shouldThrowExceptionWhenEventCompletesThatHasNoMatchingInitiatedEntry() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When/Then

        // IT [1,2,3]
        // CT [ , , ]
        lct.submitLocalInitiatedTime(1000l);
        lct.submitLocalInitiatedTime(2000l);
        lct.submitLocalInitiatedTime(3000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(1000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(-1l));

        // IT [ ,2,3]
        // CT [1, , ]
        lct.submitLocalCompletedTime(1000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(2000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(1000l));

        boolean exceptionThrown = false;
        try {
            // only one entry with DueTime=1 exists, this should throw exception
            lct.submitLocalCompletedTime(1000l);
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(2000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(1000l));
    }

    @Test
    public void shouldReturnLatestTimeBehindWhichThereAreNoUncompletedITEvents() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When/Then
        // IT [1]
        // CT []
        lct.submitLocalInitiatedTime(1000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(1000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(-1l));
        // IT [ ]
        // CT [1]
        lct.submitLocalCompletedTime(1000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(1000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(-1l));

        // IT [ ,2]
        // CT [1, ]
        lct.submitLocalInitiatedTime(2000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(2000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(1000l));
        // IT [ , ]
        // CT [1,2]
        lct.submitLocalCompletedTime(2000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(2000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(1000l));

        // IT [ , ,3]
        // CT [1,2, ]
        lct.submitLocalInitiatedTime(3000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(3000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(2000l));

        // IT [ , ,3,4]
        // CT [1,2, , ]
        lct.submitLocalInitiatedTime(4000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(3000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(2000l));

        // IT [ , ,3,4,5]
        // CT [1,2, , , ]
        lct.submitLocalInitiatedTime(5000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(3000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(2000l));

        // IT [ , ,3,4, ]
        // CT [1,2, , ,5]
        lct.submitLocalCompletedTime(5000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(3000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(2000l));

        // IT [ , ,3,4, ,5]
        // CT [1,2, , ,5, ]
        lct.submitLocalInitiatedTime(5000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(3000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(2000l));

        // IT [ , ,3,4, , ]
        // CT [1,2, , ,5,5]
        lct.submitLocalCompletedTime(5000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(3000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(2000l));

        // IT [ , ,3,4, , ,5]
        // CT [1,2, , ,5,5, ]
        lct.submitLocalInitiatedTime(5000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(3000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(2000l));

        // IT [ , ,3,4, , , ]
        // CT [1,2, , ,5,5,5]
        lct.submitLocalCompletedTime(5000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(3000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(2000l));

        // IT [ , , ,4, , , ]
        // CT [1,2,3, ,5,5,5]
        lct.submitLocalCompletedTime(3000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(4000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(3000l));

        // IT [ , , , , , , ]
        // CT [1,2,3,4,5,5,5]
        lct.submitLocalCompletedTime(4000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(5000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(4000l));

        // IT [ , , , , , , ,6]
        // CT [1,2,3,4,5,5,5, ]
        lct.submitLocalInitiatedTime(6000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(6000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(5000l));
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNotMatchingCTEvenWhenMultipleEventsHaveSameInitiatedTime() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager lct = new LocalCompletionTimeStateManager();

        // When/Then
        // IT [1]
        // CT [ ]
        lct.submitLocalInitiatedTime(1000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(1000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(-1l));

        // IT [ ]
        // CT [1]
        lct.submitLocalCompletedTime(1000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(1000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(-1l));

        // IT [ ,2]
        // CT [1, ]
        lct.submitLocalInitiatedTime(2000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(2000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(1000l));

        // IT [ , ]
        // CT [1,2]
        lct.submitLocalCompletedTime(2000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(2000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(1000l));

        // IT [ , ,3]
        // CT [1,2, ]
        lct.submitLocalInitiatedTime(3000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(3000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(2000l));

        // IT [ , ,3,3]
        // CT [1,2, , ]
        lct.submitLocalInitiatedTime(3000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(3000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(2000l));

        // IT [ , ,3,3,4]
        // CT [1,2, , , ]
        lct.submitLocalInitiatedTime(4000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(3000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(2000l));

        // IT [ , , ,3,4]
        // CT [1,2,3, , ]
        lct.submitLocalCompletedTime(3000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(3000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(2000l));

        // IT [ , , ,3, ]
        // CT [1,2,3, ,4]
        lct.submitLocalCompletedTime(4000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(3000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(2000l));

        // IT [ , , , , ]
        // CT [1,2,3,3,4]
        lct.submitLocalCompletedTime(3000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(4000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(3000l));

        // IT [ , , , , ,5]
        // CT [1,2,3,3,4, ]
        lct.submitLocalInitiatedTime(5000l);
        assertThat(lct.lastKnownLowestInitiatedTimeAsMilli(), is(5000l));
        assertThat(lct.localCompletionTimeAsMilli(), is(4000l));
    }
}