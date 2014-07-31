package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.temporal.Time;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class CollectionLocalCompletedTimeTrackerTest {
    @Ignore
    @Test
    public void testWithListToo() throws CompletionTimeException {
        assertThat(true, is(false));
    }

    @Ignore
    @Test
    public void benchmarkListVersusTreeMultiSet() throws CompletionTimeException {
        assertThat(true, is(false));
    }

    @Test
    public void shouldReturnNullsWhenNoTimesHaveBeenSubmitted() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager.CollectionLocalCompletedTimeTracker tracker =
                LocalCompletionTimeStateManager.CollectionLocalCompletedTimeTracker.createUsingTreeMultiSet();

        // When
        // nothing

        // Then
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromNano(Long.MAX_VALUE)), is(nullValue()));
    }

    @Test
    public void shouldRemoveTimesCorrectlyWhenThereIsOnlyOneTime() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager.CollectionLocalCompletedTimeTracker tracker =
                LocalCompletionTimeStateManager.CollectionLocalCompletedTimeTracker.createUsingTreeMultiSet();

        // When
        tracker.addCompletedTime(Time.fromMilli(1));

        // Then
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(0)), is(nullValue()));
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(1)), is(nullValue()));
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromNano(Long.MAX_VALUE)), is(Time.fromMilli(1)));
    }

    @Test
    public void shouldRemoveTimesCorrectlyWhenThereIsAreMultipleTimesThatAreAddedInOrder() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager.CollectionLocalCompletedTimeTracker tracker =
                LocalCompletionTimeStateManager.CollectionLocalCompletedTimeTracker.createUsingTreeMultiSet();

        // When/Then
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(0)), is(nullValue()));
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromNano(Long.MAX_VALUE)), is(nullValue()));

        // [1]
        tracker.addCompletedTime(Time.fromMilli(1));

        // [1]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(0)), is(nullValue()));
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(1)), is(nullValue()));

        // [1,2]
        tracker.addCompletedTime(Time.fromMilli(2));

        // [1,2]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(0)), is(nullValue()));
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(1)), is(nullValue()));

        // [1,2,3]
        tracker.addCompletedTime(Time.fromMilli(3));

        // [ ,2,3]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(2)), is(Time.fromMilli(1)));

        // [ ,2,3,4]
        tracker.addCompletedTime(Time.fromMilli(4));

        // [ ,2,3,4,5,6,7]
        tracker.addCompletedTime(Time.fromMilli(5));
        tracker.addCompletedTime(Time.fromMilli(6));
        tracker.addCompletedTime(Time.fromMilli(7));

        // [ , , , ,5,6,7]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(5)), is(Time.fromMilli(4)));

        // [ , , , ,5,6,7,8,9,10]
        tracker.addCompletedTime(Time.fromMilli(8));
        tracker.addCompletedTime(Time.fromMilli(9));
        tracker.addCompletedTime(Time.fromMilli(10));

        // [ , , , ,5,6,7,8,9,10,11,14,15]
        tracker.addCompletedTime(Time.fromMilli(11));
        tracker.addCompletedTime(Time.fromMilli(14));
        tracker.addCompletedTime(Time.fromMilli(15));

        // [ , , , , , , , , , , ,14,15]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(13)), is(Time.fromMilli(11)));

        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromNano(Long.MAX_VALUE)), is(Time.fromMilli(15)));
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromNano(Long.MAX_VALUE)), is(nullValue()));
    }

    @Test
    public void shouldRemoveTimesCorrectlyWhenThereIsAreMultipleTimesThatAreAddedOutOfOrder() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager.CollectionLocalCompletedTimeTracker tracker =
                LocalCompletionTimeStateManager.CollectionLocalCompletedTimeTracker.createUsingTreeMultiSet();

        // When/Then
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(0)), is(nullValue()));
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromNano(Long.MAX_VALUE)), is(nullValue()));

        // [1]
        tracker.addCompletedTime(Time.fromMilli(1));

        // [0,0,1,1]
        tracker.addCompletedTime(Time.fromMilli(0));
        tracker.addCompletedTime(Time.fromMilli(0));
        tracker.addCompletedTime(Time.fromMilli(1));

        // [0,0,1,1,9,2,6]
        tracker.addCompletedTime(Time.fromMilli(9));
        tracker.addCompletedTime(Time.fromMilli(2));
        tracker.addCompletedTime(Time.fromMilli(6));

        // [ , , , ,9, ,6]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(4)), is(Time.fromMilli(2)));

        // [ , , , ,9, ,6,1,0,0,4]
        tracker.addCompletedTime(Time.fromMilli(1));
        tracker.addCompletedTime(Time.fromMilli(0));
        tracker.addCompletedTime(Time.fromMilli(0));
        tracker.addCompletedTime(Time.fromMilli(4));

        // [ , , , ,9, ,6, , , ,4]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(4)), is(Time.fromMilli(1)));

        // [ , , , ,9, ,6, , , ,4,1,2,3,4,5,6,7,8,9]
        tracker.addCompletedTime(Time.fromMilli(1));
        tracker.addCompletedTime(Time.fromMilli(2));
        tracker.addCompletedTime(Time.fromMilli(3));
        tracker.addCompletedTime(Time.fromMilli(4));
        tracker.addCompletedTime(Time.fromMilli(5));
        tracker.addCompletedTime(Time.fromMilli(6));
        tracker.addCompletedTime(Time.fromMilli(7));
        tracker.addCompletedTime(Time.fromMilli(8));
        tracker.addCompletedTime(Time.fromMilli(9));

        // [ , , , ,9, ,6, , , , , , , , , ,6,7,8,9]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(6)), is(Time.fromMilli(5)));

        // [ , , , ,9, ,6, , , , , , , , , ,6,7,8,9,10]
        tracker.addCompletedTime(Time.fromMilli(10));

        // [ , , , ,9, ,6, , , , , , , , , ,6,7,8,9,10]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(6)), is(nullValue()));

        // [ , , , ,9, , , , , , , , , , , , , ,8,9,10]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(8)), is(Time.fromMilli(7)));

        // [ , , , ,9, , , , , , , , , , , , , ,8,9,10,0]
        tracker.addCompletedTime(Time.fromMilli(0));

        // [ , , , ,9, , , , , , , , , , , , , ,8,9,10,0,15]
        tracker.addCompletedTime(Time.fromMilli(15));

        // [ , , , ,9, , , , , , , , , , , , , ,8,9,10, ,15]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(6)), is(Time.fromMilli(0)));

        // [ , , , , , , , , , , , , , , , , , , , , , ,15]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(12)), is(Time.fromMilli(10)));

        // [ , , , , , , , , , , , , , , , , , , , , , ,15]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(15)), is(nullValue()));

        // [ , , , , , , , , , , , , , , , , , , , , , , ]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(16)), is(Time.fromMilli(15)));

        // [ , , , , , , , , , , , , , , , , , , , , , , ]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromNano(Long.MAX_VALUE)), is(nullValue()));
    }
}
