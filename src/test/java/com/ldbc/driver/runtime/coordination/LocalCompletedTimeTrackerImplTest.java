package com.ldbc.driver.runtime.coordination;

import com.google.common.collect.Lists;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class LocalCompletedTimeTrackerImplTest {
    /*
     ------- PERFORMANCE -------
     */

    @Ignore
    @Test
    public void comparePerformanceOfImplementations() throws CompletionTimeException {
        int timesCountBigger = 1000000;
        int timesCountSmaller = 10000;
        int benchmarkRepetitions = 10;

        Duration totalDuration_TreeMultiset_AddedRemovedImmediately = Duration.fromMilli(0);
        Duration totalDuration_List_AddedRemovedImmediately = Duration.fromMilli(0);

        Duration totalDuration_TreeMultiset_AddAllThenRemoveAllSequentially = Duration.fromMilli(0);
        Duration totalDuration_List_AddAllThenRemoveAllSequentially = Duration.fromMilli(0);

        Duration totalDuration_TreeMultiSet_JustAdd = Duration.fromMilli(0);
        Duration totalDuration_List_JustAdd = Duration.fromMilli(0);

        Duration totalDuration_TreeMultiset_AddAllThenRemoveAllRandomly = Duration.fromMilli(0);
        Duration totalDuration_List_AddAllThenRemoveAllRandomly = Duration.fromMilli(0);

        for (int i = 0; i < benchmarkRepetitions; i++) {
            // ******
            // Added Removed Immediately
            // ******
            totalDuration_TreeMultiset_AddedRemovedImmediately =
                    totalDuration_TreeMultiset_AddedRemovedImmediately.plus(
                            benchmarkTimesAddedAndRemovedImmediately(
                                    LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingTreeMultiSet(),
                                    timesCountBigger
                            )
                    );

            totalDuration_List_AddedRemovedImmediately =
                    totalDuration_List_AddedRemovedImmediately.plus(
                            benchmarkTimesAddedAndRemovedImmediately(
                                    LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingArrayList(),
                                    timesCountBigger
                            )
                    );

            // ******
            // Add All Then Remove All Sequentially
            // ******
            totalDuration_TreeMultiset_AddAllThenRemoveAllSequentially =
                    totalDuration_TreeMultiset_AddAllThenRemoveAllSequentially.plus(
                            benchmarkTimesAllAddedThenAllRemovedSequentially(
                                    LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingTreeMultiSet(),
                                    timesCountSmaller
                            )
                    );

            totalDuration_List_AddAllThenRemoveAllSequentially =
                    totalDuration_List_AddAllThenRemoveAllSequentially.plus(
                            benchmarkTimesAllAddedThenAllRemovedSequentially(
                                    LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingArrayList(),
                                    timesCountSmaller
                            )
                    );

            // ******
            // Just Add
            // ******
            totalDuration_TreeMultiSet_JustAdd =
                    totalDuration_TreeMultiSet_JustAdd.plus(
                            benchmarkTimesJustAdded(
                                    LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingTreeMultiSet(),
                                    timesCountBigger
                            )
                    );

            totalDuration_List_JustAdd =
                    totalDuration_List_JustAdd.plus(
                            benchmarkTimesJustAdded(
                                    LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingArrayList(),
                                    timesCountBigger
                            )
                    );

            // ******
            // Add All Then Remove All Randomly
            // ******
            totalDuration_TreeMultiset_AddAllThenRemoveAllRandomly =
                    totalDuration_TreeMultiset_AddAllThenRemoveAllRandomly.plus(
                            benchmarkTimesAllAddedThenAllRemovedRandomly(
                                    LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingTreeMultiSet(),
                                    timesCountSmaller
                            )
                    );

            totalDuration_List_AddAllThenRemoveAllRandomly =
                    totalDuration_List_AddAllThenRemoveAllRandomly.plus(
                            benchmarkTimesAllAddedThenAllRemovedRandomly(
                                    LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingArrayList(),
                                    timesCountSmaller
                            )
                    );
        }

        System.out.println("TreeMultiSet (add " + timesCountBigger + "):\t\t\t\t\t\t\t\t\t" + totalDuration_TreeMultiSet_JustAdd + " <- add to large/growing");
        System.out.println("List (add " + timesCountBigger + "):\t\t\t\t\t\t\t\t\t\t\t" + totalDuration_List_JustAdd + " <- add to large/growing");
        System.out.println();
        System.out.println("TreeMultiSet (add one/remove one x " + timesCountBigger + "):\t\t\t\t" + totalDuration_TreeMultiset_AddedRemovedImmediately + " <- add to small`,remove from small sequentially");
        System.out.println("List (add one/remove one):\t\t\t\t\t\t\t\t\t" + totalDuration_List_AddedRemovedImmediately + " <- add to small,remove from small sequentially");
        System.out.println();
        System.out.println("TreeMultiSet (add " + timesCountSmaller + "/remove " + timesCountSmaller + " sequentially):\t\t\t" + totalDuration_TreeMultiset_AddAllThenRemoveAllSequentially + " <- add to large/growing, removal from large/growing sequentially");
        System.out.println("List (add " + timesCountSmaller + "/remove " + timesCountSmaller + " sequentially):\t\t\t\t\t" + totalDuration_List_AddAllThenRemoveAllSequentially + " <- add to large/growing, removal from large/growing sequentially");
        System.out.println();
        System.out.println("TreeMultiSet (add " + timesCountSmaller + "/remove " + timesCountSmaller + " randomly):\t\t\t\t" + totalDuration_TreeMultiset_AddAllThenRemoveAllRandomly + " <- add to large/growing, removal from large/growing randomly");
        System.out.println("List (add " + timesCountSmaller + "/remove " + timesCountSmaller + " randomly):\t\t\t\t\t\t" + totalDuration_List_AddAllThenRemoveAllRandomly + " <- add to large/growing, removal from large/growing randomly");
    }

    public Duration benchmarkTimesAddedAndRemovedImmediately(
            LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl tracker,
            int timesCount) throws CompletionTimeException {
        TimeSource timeSource = new SystemTimeSource();
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));

        Iterator<Time> times = gf.limit(
                gf.constantIncrementTime(Time.fromMilli(0), Duration.fromMilli(1)),
                timesCount);

        Time startTime = timeSource.now();

        while (times.hasNext()) {
            Time time = times.next();
            tracker.addCompletedTimeAsMilli(time);
            tracker.removeTimesLowerThanAndReturnHighestRemoved(time);
        }

        Time finishTime = timeSource.now();

        return finishTime.durationGreaterThan(startTime);
    }

    public Duration benchmarkTimesAllAddedThenAllRemovedSequentially(
            LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl tracker,
            int timesCount) throws CompletionTimeException {
        TimeSource timeSource = new SystemTimeSource();
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));

        List<Time> times = Lists.newArrayList(
                gf.limit(
                        gf.constantIncrementTime(Time.fromMilli(0), Duration.fromMilli(1)),
                        timesCount)
        );

        Time startTime = timeSource.now();

        // add all times
        for (Time time : times) {
            tracker.addCompletedTimeAsMilli(time);
        }

        // remove all times
        for (Time time : times) {
            tracker.removeTimesLowerThanAndReturnHighestRemoved(time);
        }

        Time finishTime = timeSource.now();

        return finishTime.durationGreaterThan(startTime);
    }

    public Duration benchmarkTimesAllAddedThenAllRemovedRandomly(
            LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl tracker,
            int timesCount) throws CompletionTimeException {
        TimeSource timeSource = new SystemTimeSource();
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));

        final List<Time> times = Lists.newArrayList(
                gf.limit(
                        gf.constantIncrementTime(Time.fromMilli(0), Duration.fromMilli(1)),
                        timesCount)
        );

        // create iterator to add times from, in sequential order <-- requirement of tracker
        Iterator<Time> timesToAdd = Lists.newArrayList(times).iterator();

        // create iterator to remove times from, in random order
        List<Time> timesToRemoveList = new ArrayList<>();
        Iterator<Double> uniforms = gf.uniform(0.0, 1.0);
        while (false == times.isEmpty()) {
            int index = (int) Math.round(Math.floor(uniforms.next() * times.size()));
            Time timeToRemove = times.remove(index);
            assertThat(timeToRemove, is(notNullValue()));
            timesToRemoveList.add(timeToRemove);
        }
        assertThat(timesToRemoveList.size(), is(timesCount));
        Iterator<Time> timesToRemove = timesToRemoveList.iterator();

        Time startTime = timeSource.now();

        // add all times
        while (timesToAdd.hasNext()) {
            Time time = timesToAdd.next();
            tracker.addCompletedTimeAsMilli(time);
        }

        // remove all times
        while (timesToRemove.hasNext()) {
            Time time = timesToRemove.next();
            tracker.removeTimesLowerThanAndReturnHighestRemoved(time);
        }

        Time finishTime = timeSource.now();

        return finishTime.durationGreaterThan(startTime);
    }

    public Duration benchmarkTimesJustAdded(
            LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl tracker,
            int timesCount) throws CompletionTimeException {
        TimeSource timeSource = new SystemTimeSource();
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));

        List<Time> times = Lists.newArrayList(
                gf.limit(
                        gf.constantIncrementTime(Time.fromMilli(0), Duration.fromMilli(1)),
                        timesCount)
        );

        Time startTime = timeSource.now();

        // add all times
        for (Time time : times) {
            tracker.addCompletedTimeAsMilli(time);
        }

        Time finishTime = timeSource.now();

        return finishTime.durationGreaterThan(startTime);
    }

    /*
     ------- CORRECTNESS -------
     */

    @Test
    public void shouldReturnNullsWhenNoTimesHaveBeenSubmitted() throws CompletionTimeException {
        // Given
        LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl tracker =
                LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingTreeMultiSet();

        // When
        // nothing

        // Then
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromNano(Long.MAX_VALUE)), is(nullValue()));
    }

    @Test
    public void shouldRemoveTimesCorrectlyWhenThereIsOnlyOneTime_UsingTreeMultiSet() throws CompletionTimeException {
        shouldRemoveTimesCorrectlyWhenThereIsOnlyOneTime(LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingTreeMultiSet());
    }

    @Test
    public void shouldRemoveTimesCorrectlyWhenThereIsOnlyOneTime_UsingList() throws CompletionTimeException {
        shouldRemoveTimesCorrectlyWhenThereIsOnlyOneTime(LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingArrayList());
    }

    public void shouldRemoveTimesCorrectlyWhenThereIsOnlyOneTime(LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl tracker) throws CompletionTimeException {
        // Given
        // tracker

        // When
        tracker.addCompletedTimeAsMilli(Time.fromMilli(1));

        // Then
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(0)), is(nullValue()));
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(1)), is(nullValue()));
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromNano(Long.MAX_VALUE)), is(Time.fromMilli(1)));
    }

    @Test
    public void shouldRemoveTimesCorrectlyWhenThereIsAreMultipleTimesThatAreAddedInOrder_UsingTreeMultiSet() throws CompletionTimeException {
        shouldRemoveTimesCorrectlyWhenThereIsAreMultipleTimesThatAreAddedInOrder(LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingTreeMultiSet());
    }

    @Test
    public void shouldRemoveTimesCorrectlyWhenThereIsAreMultipleTimesThatAreAddedInOrder_UsingList() throws CompletionTimeException {
        shouldRemoveTimesCorrectlyWhenThereIsAreMultipleTimesThatAreAddedInOrder(LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingArrayList());
    }

    public void shouldRemoveTimesCorrectlyWhenThereIsAreMultipleTimesThatAreAddedInOrder(LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl tracker) throws CompletionTimeException {
        // Given
        // tracker

        // When/Then
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(0)), is(nullValue()));
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromNano(Long.MAX_VALUE)), is(nullValue()));

        // [1]
        tracker.addCompletedTimeAsMilli(Time.fromMilli(1));

        // [1]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(0)), is(nullValue()));
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(1)), is(nullValue()));

        // [1,2]
        tracker.addCompletedTimeAsMilli(Time.fromMilli(2));

        // [1,2]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(0)), is(nullValue()));
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(1)), is(nullValue()));

        // [1,2,3]
        tracker.addCompletedTimeAsMilli(Time.fromMilli(3));

        // [ ,2,3]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(2)), is(Time.fromMilli(1)));

        // [ ,2,3,4]
        tracker.addCompletedTimeAsMilli(Time.fromMilli(4));

        // [ ,2,3,4,5,6,7]
        tracker.addCompletedTimeAsMilli(Time.fromMilli(5));
        tracker.addCompletedTimeAsMilli(Time.fromMilli(6));
        tracker.addCompletedTimeAsMilli(Time.fromMilli(7));

        // [ , , , ,5,6,7]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(5)), is(Time.fromMilli(4)));

        // [ , , , ,5,6,7,8,9,10]
        tracker.addCompletedTimeAsMilli(Time.fromMilli(8));
        tracker.addCompletedTimeAsMilli(Time.fromMilli(9));
        tracker.addCompletedTimeAsMilli(Time.fromMilli(10));

        // [ , , , ,5,6,7,8,9,10,11,14,15]
        tracker.addCompletedTimeAsMilli(Time.fromMilli(11));
        tracker.addCompletedTimeAsMilli(Time.fromMilli(14));
        tracker.addCompletedTimeAsMilli(Time.fromMilli(15));

        // [ , , , , , , , , , , ,14,15]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(13)), is(Time.fromMilli(11)));

        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromNano(Long.MAX_VALUE)), is(Time.fromMilli(15)));
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromNano(Long.MAX_VALUE)), is(nullValue()));
    }

    @Test
    public void shouldRemoveTimesCorrectlyWhenThereIsAreMultipleTimesThatAreAddedOutOfOrder_UsingTreeMultiSet() throws CompletionTimeException {
        shouldRemoveTimesCorrectlyWhenThereIsAreMultipleTimesThatAreAddedOutOfOrder(LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingTreeMultiSet());
    }

    @Test
    public void shouldRemoveTimesCorrectlyWhenThereIsAreMultipleTimesThatAreAddedOutOfOrder_UsingList() throws CompletionTimeException {
        shouldRemoveTimesCorrectlyWhenThereIsAreMultipleTimesThatAreAddedOutOfOrder(LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingArrayList());
    }

    public void shouldRemoveTimesCorrectlyWhenThereIsAreMultipleTimesThatAreAddedOutOfOrder(LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl tracker) throws CompletionTimeException {
        // Given
        // tracker

        // When/Then
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(0)), is(nullValue()));
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromNano(Long.MAX_VALUE)), is(nullValue()));

        // [1]
        tracker.addCompletedTimeAsMilli(Time.fromMilli(1));

        // [0,0,1,1]
        tracker.addCompletedTimeAsMilli(Time.fromMilli(0));
        tracker.addCompletedTimeAsMilli(Time.fromMilli(0));
        tracker.addCompletedTimeAsMilli(Time.fromMilli(1));

        // [0,0,1,1,9,2,6]
        tracker.addCompletedTimeAsMilli(Time.fromMilli(9));
        tracker.addCompletedTimeAsMilli(Time.fromMilli(2));
        tracker.addCompletedTimeAsMilli(Time.fromMilli(6));

        // [ , , , ,9, ,6]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(4)), is(Time.fromMilli(2)));

        // [ , , , ,9, ,6,1,0,0,4]
        tracker.addCompletedTimeAsMilli(Time.fromMilli(1));
        tracker.addCompletedTimeAsMilli(Time.fromMilli(0));
        tracker.addCompletedTimeAsMilli(Time.fromMilli(0));
        tracker.addCompletedTimeAsMilli(Time.fromMilli(4));

        // [ , , , ,9, ,6, , , ,4]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(4)), is(Time.fromMilli(1)));

        // [ , , , ,9, ,6, , , ,4,1,2,3,4,5,6,7,8,9]
        tracker.addCompletedTimeAsMilli(Time.fromMilli(1));
        tracker.addCompletedTimeAsMilli(Time.fromMilli(2));
        tracker.addCompletedTimeAsMilli(Time.fromMilli(3));
        tracker.addCompletedTimeAsMilli(Time.fromMilli(4));
        tracker.addCompletedTimeAsMilli(Time.fromMilli(5));
        tracker.addCompletedTimeAsMilli(Time.fromMilli(6));
        tracker.addCompletedTimeAsMilli(Time.fromMilli(7));
        tracker.addCompletedTimeAsMilli(Time.fromMilli(8));
        tracker.addCompletedTimeAsMilli(Time.fromMilli(9));

        // [ , , , ,9, ,6, , , , , , , , , ,6,7,8,9]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(6)), is(Time.fromMilli(5)));

        // [ , , , ,9, ,6, , , , , , , , , ,6,7,8,9,10]
        tracker.addCompletedTimeAsMilli(Time.fromMilli(10));

        // [ , , , ,9, ,6, , , , , , , , , ,6,7,8,9,10]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(6)), is(nullValue()));

        // [ , , , ,9, , , , , , , , , , , , , ,8,9,10]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Time.fromMilli(8)), is(Time.fromMilli(7)));

        // [ , , , ,9, , , , , , , , , , , , , ,8,9,10,0]
        tracker.addCompletedTimeAsMilli(Time.fromMilli(0));

        // [ , , , ,9, , , , , , , , , , , , , ,8,9,10,0,15]
        tracker.addCompletedTimeAsMilli(Time.fromMilli(15));

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
