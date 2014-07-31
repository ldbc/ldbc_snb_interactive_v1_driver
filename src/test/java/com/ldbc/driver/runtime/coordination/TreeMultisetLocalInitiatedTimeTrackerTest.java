package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.temporal.Time;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class TreeMultisetLocalInitiatedTimeTrackerTest {
    @Ignore
    @Test
    public void implementUsingListToo() {
        assertThat(true, is(false));
    }

    @Ignore
    @Test
    public void testBothListAndTreeMultiSetImplementations() {
        assertThat(true, is(false));
    }

    @Ignore
    @Test
    public void comparePerformanceOfListAndTreeMultiSetImplementations() {
        assertThat(true, is(false));
    }

    @Test
    public void shouldReturnNullsWhenNoTimesHaveBeenSubmitted() {
        // Given
        LocalCompletionTimeStateManager.TreeMultisetLocalInitiatedTimeTracker tracker =
                new LocalCompletionTimeStateManager.TreeMultisetLocalInitiatedTimeTracker();

        // When
        // nothing

        // Then
        assertThat(tracker.highestInitiatedTime(), is(nullValue()));
        boolean exceptionThrown = false;
        try {
            tracker.removeTimeAndReturnLastKnownLowestTime(Time.fromMilli(1));
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
        assertThat(tracker.uncompletedInitiatedTimes(), is(0));
    }

    @Test
    public void shouldBehaveAsExpectedUnderScenario1() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager.TreeMultisetLocalInitiatedTimeTracker tracker =
                new LocalCompletionTimeStateManager.TreeMultisetLocalInitiatedTimeTracker();

        // When/Then
        assertThat(tracker.highestInitiatedTime(), is(nullValue()));
        boolean exceptionThrown = false;
        try {
            tracker.removeTimeAndReturnLastKnownLowestTime(Time.fromMilli(1));
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
        assertThat(tracker.uncompletedInitiatedTimes(), is(0));

        // [0]
        tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(0));

        assertThat(tracker.highestInitiatedTime(), is(Time.fromMilli(0)));
        assertThat(tracker.uncompletedInitiatedTimes(), is(1));

        // [0]
        exceptionThrown = false;
        try {
            tracker.removeTimeAndReturnLastKnownLowestTime(Time.fromMilli(1));
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));

        // [0,0]
        tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(0));

        assertThat(tracker.highestInitiatedTime(), is(Time.fromMilli(0)));
        assertThat(tracker.uncompletedInitiatedTimes(), is(2));

        // [0,0]
        exceptionThrown = false;
        try {
            tracker.removeTimeAndReturnLastKnownLowestTime(Time.fromMilli(1));
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));

        // [0,0,1,1,4,5]
        tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(1));
        tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(1));
        tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(4));
        tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(5));

        assertThat(tracker.highestInitiatedTime(), is(Time.fromMilli(5)));
        assertThat(tracker.uncompletedInitiatedTimes(), is(6));

        // [0,0,1,1,4,5,7,9,10,15]
        tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(7));
        tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(9));
        tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(10));
        tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(15));

        assertThat(tracker.highestInitiatedTime(), is(Time.fromMilli(15)));
        assertThat(tracker.uncompletedInitiatedTimes(), is(10));

        // [0,0,1,1,4,5,7,9,10,15]
        exceptionThrown = false;
        try {
            tracker.removeTimeAndReturnLastKnownLowestTime(Time.fromMilli(2));
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
        exceptionThrown = false;
        try {
            tracker.removeTimeAndReturnLastKnownLowestTime(Time.fromMilli(3));
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));

        // [ ,0,1,1,4,5,7,9,10,15]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTime(Time.fromMilli(0)), is(Time.fromMilli(0)));
        // [ ,0,1,1,4, ,7,9,10,15]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTime(Time.fromMilli(5)), is(Time.fromMilli(0)));
        // [ ,0,1,1,4, ,7,9, ,15]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTime(Time.fromMilli(10)), is(Time.fromMilli(0)));

        // [ ,0,1,1,4, ,7,9, ,15]
        assertThat(tracker.highestInitiatedTime(), is(Time.fromMilli(15)));
        assertThat(tracker.uncompletedInitiatedTimes(), is(7));

        // [ , ,1,1,4, ,7,9, ,15]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTime(Time.fromMilli(0)), is(Time.fromMilli(1)));

        // [ , ,1,1, , ,7,9, ,15]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTime(Time.fromMilli(4)), is(Time.fromMilli(1)));

        // [ , ,1,1, , ,7, , ,15]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTime(Time.fromMilli(9)), is(Time.fromMilli(1)));

        // [ , ,1,1, , ,7, , ,15]
        assertThat(tracker.highestInitiatedTime(), is(Time.fromMilli(15)));
        assertThat(tracker.uncompletedInitiatedTimes(), is(4));

        // [ , ,1,1, , ,7, , ,15,15,15]
        exceptionThrown = false;
        try {
            tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(14));
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
        tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(15));
        tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(15));

        assertThat(tracker.highestInitiatedTime(), is(Time.fromMilli(15)));
        assertThat(tracker.uncompletedInitiatedTimes(), is(6));

        // [ , ,1,1, , ,7, , , ,15,15]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTime(Time.fromMilli(15)), is(Time.fromMilli(1)));

        // [ , ,1,1, , ,7, , , , ,15]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTime(Time.fromMilli(15)), is(Time.fromMilli(1)));

        // [ , ,1,1, , ,7, , , , , ]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTime(Time.fromMilli(15)), is(Time.fromMilli(1)));

        // [ , ,1,1, , ,7, , , , , ]
        assertThat(tracker.highestInitiatedTime(), is(Time.fromMilli(15)));
        assertThat(tracker.uncompletedInitiatedTimes(), is(3));

        // [ , , ,1, , ,7, , , , , ]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTime(Time.fromMilli(1)), is(Time.fromMilli(1)));

        // [ , , , , , ,7, , , , , ]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTime(Time.fromMilli(1)), is(Time.fromMilli(7)));

        // [ , , , , , ,7, , , , , ]
        assertThat(tracker.highestInitiatedTime(), is(Time.fromMilli(15)));
        assertThat(tracker.uncompletedInitiatedTimes(), is(1));

        // [ , , , , , , , , , , , ]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTime(Time.fromMilli(7)), is(Time.fromMilli(15)));

        // [ , , , , , , , , , , , ]
        assertThat(tracker.highestInitiatedTime(), is(Time.fromMilli(15)));
        assertThat(tracker.uncompletedInitiatedTimes(), is(0));
    }
}
