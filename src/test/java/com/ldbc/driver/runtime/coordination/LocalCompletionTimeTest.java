package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.temporal.Time;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

//Completion Time = min( min(Initiated Events), max(Completed Events) )
public class LocalCompletionTimeTest {
    LocalCompletionTime lct = null;

    @Before
    public void createCompletionTime() {
        lct = new LocalCompletionTime();
    }

    @Test
    public void shouldReturnNullWhenNoEventsHaveBeenInitiatedOrCompleted() throws CompletionTimeException {
        // Given
        // lct parameter

        // When
        // no events have been initiated or completed

        // Then
        assertThat(lct.completionTime(), is(nullValue()));
    }

    @Test
    public void shouldReturnMinimumInitiatedTimeWhenNoOperationsHaveCompleted() throws CompletionTimeException {
        /*
            event1 DueTime = 1  initiated
            event2 DueTime = 2  initiated
            event3 DueTime = 3  initiated
        */

        // Given
        // lct parameter

        // When

        lct.applyInitiatedTime(Time.fromSeconds(1));
        lct.applyInitiatedTime(Time.fromSeconds(2));
        lct.applyInitiatedTime(Time.fromSeconds(3));

        // Then
        assertThat(lct.completionTime(), is(Time.fromSeconds(1)));
    }

    @Test
    public void shouldReturnMinimumInitiatedTimeWhenEarliestInitiatedOperationHasNotCompleted() throws CompletionTimeException {
        /*
            event1 DueTime = 1  initiated
            event2 DueTime = 2  initiated
            event3 DueTime = 3  initiated
            event2              completed
            event3              completed
        */
        // Given
        // lct parameter

        // When

        lct.applyInitiatedTime(Time.fromSeconds(1));
        lct.applyInitiatedTime(Time.fromSeconds(2));
        lct.applyInitiatedTime(Time.fromSeconds(3));
        lct.applyCompletedTime(Time.fromSeconds(2));
        lct.applyCompletedTime(Time.fromSeconds(3));

        // Then
        assertThat(lct.completionTime(), is(Time.fromSeconds(1)));
    }

    @Test
    public void shouldAdvanceToNextUncompletedEventAsInitiatedEventsAreCompleted() throws CompletionTimeException {
        /*
            event1 DueTime = 1  initiated
            event2 DueTime = 2  initiated
            event3 DueTime = 3  initiated
            event3              completed
            event1              completed
        */
        // Given
        // lct parameter

        // When

        lct.applyInitiatedTime(Time.fromSeconds(1));
        lct.applyInitiatedTime(Time.fromSeconds(2));
        lct.applyInitiatedTime(Time.fromSeconds(3));
        lct.applyCompletedTime(Time.fromSeconds(3));
        lct.applyCompletedTime(Time.fromSeconds(1));

        // Then
        assertThat(lct.completionTime(), is(Time.fromSeconds(2)));
    }

    @Test
    public void shouldReturnLatestInitiatedEventTimeWhenAllEventsHaveCompleted() throws CompletionTimeException {
        /*
            event1 DueTime = 1  initiated
            event2 DueTime = 2  initiated
            event3 DueTime = 3  initiated
            event1              completed
            event2              completed
            event3              completed
        */
        // Given
        // lct parameter

        // When

        lct.applyInitiatedTime(Time.fromSeconds(1));
        lct.applyInitiatedTime(Time.fromSeconds(2));
        lct.applyInitiatedTime(Time.fromSeconds(3));
        lct.applyCompletedTime(Time.fromSeconds(1));
        lct.applyCompletedTime(Time.fromSeconds(2));
        lct.applyCompletedTime(Time.fromSeconds(3));

        // Then
        assertThat(lct.completionTime(), is(Time.fromSeconds(3)));
    }

    @Test(expected = CompletionTimeException.class)
    public void shouldThrowExceptionWhenEventCompletesThatHasNoMatchingInitiatedEntry() throws CompletionTimeException {
        /*
            event1 DueTime = 1  initiated
            event2 DueTime = 2  initiated
            event3 DueTime = 3  initiated
            event1              completed
            event1              completed
        */
        // Given
        // lct parameter

        // When

        lct.applyInitiatedTime(Time.fromSeconds(1));
        lct.applyInitiatedTime(Time.fromSeconds(2));
        lct.applyInitiatedTime(Time.fromSeconds(3));
        lct.applyCompletedTime(Time.fromSeconds(1));
        // only one entry with DueTime=1 exists, this should throw exception
        lct.applyCompletedTime(Time.fromSeconds(1));

        // Then
        // exception should have been thrown by this stage
    }
}
