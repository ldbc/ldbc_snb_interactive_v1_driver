package com.ldbc.driver.coordination;

import com.ldbc.driver.temporal.Time;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

//Completion Time = min( min(Initiated Events), max(Completed Events) )
public class LocalCompletionTimeTest {
    CompletionTime ct = null;

    @Before
    public void createCompletionTime() {
        ct = new LocalCompletionTime();
    }

    @Test
    public void shouldReturnNullWhenNoEventsHaveBeenInitiatedOrCompleted() throws CompletionTimeException {
        // Given
        // ct parameter

        // When
        // no events have been initiated or completed

        // Then
        assertThat(ct.get(), is(nullValue()));
    }

    @Test
    public void shouldReturnMinimumInitiatedTimeWhenNoOperationsHaveCompleted() throws CompletionTimeException {
        /*
            event1 DueTime = 1  initiated
            event2 DueTime = 2  initiated
            event3 DueTime = 3  initiated
        */

        // Given
        // ct parameter

        // When

        ct.applyInitiatedTime(Time.fromSeconds(1));
        ct.applyInitiatedTime(Time.fromSeconds(2));
        ct.applyInitiatedTime(Time.fromSeconds(3));

        // Then
        assertThat(ct.get(), is(Time.fromSeconds(1)));
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
        // ct parameter

        // When

        ct.applyInitiatedTime(Time.fromSeconds(1));
        ct.applyInitiatedTime(Time.fromSeconds(2));
        ct.applyInitiatedTime(Time.fromSeconds(3));
        ct.applyCompletedTime(Time.fromSeconds(2));
        ct.applyCompletedTime(Time.fromSeconds(3));

        // Then
        assertThat(ct.get(), is(Time.fromSeconds(1)));
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
        // ct parameter

        // When

        ct.applyInitiatedTime(Time.fromSeconds(1));
        ct.applyInitiatedTime(Time.fromSeconds(2));
        ct.applyInitiatedTime(Time.fromSeconds(3));
        ct.applyCompletedTime(Time.fromSeconds(3));
        ct.applyCompletedTime(Time.fromSeconds(1));

        // Then
        assertThat(ct.get(), is(Time.fromSeconds(2)));
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
        // ct parameter

        // When

        ct.applyInitiatedTime(Time.fromSeconds(1));
        ct.applyInitiatedTime(Time.fromSeconds(2));
        ct.applyInitiatedTime(Time.fromSeconds(3));
        ct.applyCompletedTime(Time.fromSeconds(1));
        ct.applyCompletedTime(Time.fromSeconds(2));
        ct.applyCompletedTime(Time.fromSeconds(3));

        // Then
        assertThat(ct.get(), is(Time.fromSeconds(3)));
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
        // ct parameter

        // When

        ct.applyInitiatedTime(Time.fromSeconds(1));
        ct.applyInitiatedTime(Time.fromSeconds(2));
        ct.applyInitiatedTime(Time.fromSeconds(3));
        ct.applyCompletedTime(Time.fromSeconds(1));
        // only one entry with DueTime=1 exists, this should throw exception
        ct.applyCompletedTime(Time.fromSeconds(1));

        // Then
        // exception should have been thrown by this stage
    }
}
