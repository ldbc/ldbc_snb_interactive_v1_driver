package com.ldbc.driver.runtime.coordination;

import com.google.common.collect.Lists;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
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

        long totalDuration_TreeMultiset_AddedRemovedImmediatelyAsMilli = 0l;
        long totalDuration_List_AddedRemovedImmediatelyAsMilli = 0l;

        long totalDuration_TreeMultiset_AddAllThenRemoveAllSequentiallyAsMilli = 0l;
        long totalDuration_List_AddAllThenRemoveAllSequentiallyAsMilli = 0l;

        long totalDuration_TreeMultiSet_JustAddAsMilli = 0l;
        long totalDuration_List_JustAddAsMilli = 0l;

        long totalDuration_TreeMultiset_AddAllThenRemoveAllRandomlyAsMilli = 0l;
        long totalDuration_List_AddAllThenRemoveAllRandomlyAsMilli = 0l;

        for (int i = 0; i < benchmarkRepetitions; i++) {
            // ******
            // Added Removed Immediately
            // ******
            totalDuration_TreeMultiset_AddedRemovedImmediatelyAsMilli =
                    totalDuration_TreeMultiset_AddedRemovedImmediatelyAsMilli +
                            benchmarkTimesAddedAndRemovedImmediatelyAsMilli(
                                    LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingTreeMultiSet(),
                                    timesCountBigger
                            )
            ;

            totalDuration_List_AddedRemovedImmediatelyAsMilli =
                    totalDuration_List_AddedRemovedImmediatelyAsMilli +
                            benchmarkTimesAddedAndRemovedImmediatelyAsMilli(
                                    LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingArrayList(),
                                    timesCountBigger
                            );

            // ******
            // Add All Then Remove All Sequentially
            // ******
            totalDuration_TreeMultiset_AddAllThenRemoveAllSequentiallyAsMilli =
                    totalDuration_TreeMultiset_AddAllThenRemoveAllSequentiallyAsMilli +
                            benchmarkTimesAllAddedThenAllRemovedSequentiallyAsMilli(
                                    LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingTreeMultiSet(),
                                    timesCountSmaller
                            );

            totalDuration_List_AddAllThenRemoveAllSequentiallyAsMilli =
                    totalDuration_List_AddAllThenRemoveAllSequentiallyAsMilli +
                            benchmarkTimesAllAddedThenAllRemovedSequentiallyAsMilli(
                                    LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingArrayList(),
                                    timesCountSmaller
                            );

            // ******
            // Just Add
            // ******
            totalDuration_TreeMultiSet_JustAddAsMilli =
                    totalDuration_TreeMultiSet_JustAddAsMilli +
                            benchmarkTimesJustAddedAsMilli(
                                    LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingTreeMultiSet(),
                                    timesCountBigger
                            );

            totalDuration_List_JustAddAsMilli =
                    totalDuration_List_JustAddAsMilli +
                            benchmarkTimesJustAddedAsMilli(
                                    LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingArrayList(),
                                    timesCountBigger
                            );

            // ******
            // Add All Then Remove All Randomly
            // ******
            totalDuration_TreeMultiset_AddAllThenRemoveAllRandomlyAsMilli =
                    totalDuration_TreeMultiset_AddAllThenRemoveAllRandomlyAsMilli +
                            benchmarkTimesAllAddedThenAllRemovedRandomlyAsMilli(
                                    LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingTreeMultiSet(),
                                    timesCountSmaller
                            );

            totalDuration_List_AddAllThenRemoveAllRandomlyAsMilli =
                    totalDuration_List_AddAllThenRemoveAllRandomlyAsMilli +
                            benchmarkTimesAllAddedThenAllRemovedRandomlyAsMilli(
                                    LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl.createUsingArrayList(),
                                    timesCountSmaller
                            );
        }

        System.out.println("TreeMultiSet (add " + timesCountBigger + "):\t\t\t\t\t\t\t\t\t" + totalDuration_TreeMultiSet_JustAddAsMilli + " <- add to large/growing");
        System.out.println("List (add " + timesCountBigger + "):\t\t\t\t\t\t\t\t\t\t\t" + totalDuration_List_JustAddAsMilli + " <- add to large/growing");
        System.out.println();
        System.out.println("TreeMultiSet (add one/remove one x " + timesCountBigger + "):\t\t\t\t" + totalDuration_TreeMultiset_AddedRemovedImmediatelyAsMilli + " <- add to small`,remove from small sequentially");
        System.out.println("List (add one/remove one):\t\t\t\t\t\t\t\t\t" + totalDuration_List_AddedRemovedImmediatelyAsMilli + " <- add to small,remove from small sequentially");
        System.out.println();
        System.out.println("TreeMultiSet (add " + timesCountSmaller + "/remove " + timesCountSmaller + " sequentially):\t\t\t" + totalDuration_TreeMultiset_AddAllThenRemoveAllSequentiallyAsMilli + " <- add to large/growing, removal from large/growing sequentially");
        System.out.println("List (add " + timesCountSmaller + "/remove " + timesCountSmaller + " sequentially):\t\t\t\t\t" + totalDuration_List_AddAllThenRemoveAllSequentiallyAsMilli + " <- add to large/growing, removal from large/growing sequentially");
        System.out.println();
        System.out.println("TreeMultiSet (add " + timesCountSmaller + "/remove " + timesCountSmaller + " randomly):\t\t\t\t" + totalDuration_TreeMultiset_AddAllThenRemoveAllRandomlyAsMilli + " <- add to large/growing, removal from large/growing randomly");
        System.out.println("List (add " + timesCountSmaller + "/remove " + timesCountSmaller + " randomly):\t\t\t\t\t\t" + totalDuration_List_AddAllThenRemoveAllRandomlyAsMilli + " <- add to large/growing, removal from large/growing randomly");
    }

    public long benchmarkTimesAddedAndRemovedImmediatelyAsMilli(
            LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl tracker,
            int timesCount) throws CompletionTimeException {
        TimeSource timeSource = new SystemTimeSource();
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));

        Iterator<Long> times = gf.limit(
                gf.incrementing(0l, 1l),
                timesCount
        );

        long startTimeAsMilli = timeSource.nowAsMilli();

        while (times.hasNext()) {
            long timeAsMilli = times.next();
            tracker.addCompletedTimeAsMilli(timeAsMilli);
            tracker.removeTimesLowerThanAndReturnHighestRemoved(timeAsMilli);
        }

        long finishTimeAsMilli = timeSource.nowAsMilli();

        return finishTimeAsMilli - startTimeAsMilli;
    }

    public long benchmarkTimesAllAddedThenAllRemovedSequentiallyAsMilli(
            LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl tracker,
            int timesCount) throws CompletionTimeException {
        TimeSource timeSource = new SystemTimeSource();
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));

        List<Long> timesAsMilli = Lists.newArrayList(
                gf.limit(
                        gf.incrementing(0l, 1l),
                        timesCount
                )
        );

        long startTimeAsMilli = timeSource.nowAsMilli();

        // add all times
        for (long timeAsMilli : timesAsMilli) {
            tracker.addCompletedTimeAsMilli(timeAsMilli);
        }

        // remove all times
        for (long timeAsMilli : timesAsMilli) {
            tracker.removeTimesLowerThanAndReturnHighestRemoved(timeAsMilli);
        }

        long finishTimeAsMilli = timeSource.nowAsMilli();

        return finishTimeAsMilli - startTimeAsMilli;
    }

    public long benchmarkTimesAllAddedThenAllRemovedRandomlyAsMilli(
            LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl tracker,
            int timesCount) throws CompletionTimeException {
        TimeSource timeSource = new SystemTimeSource();
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));

        final List<Long> times = Lists.newArrayList(
                gf.limit(
                        gf.incrementing(0l, 1l),
                        timesCount)
        );

        // create iterator to add times from, in sequential order <-- requirement of tracker
        Iterator<Long> timesToAdd = Lists.newArrayList(times).iterator();

        // create iterator to remove times from, in random order
        List<Long> timesToRemoveList = new ArrayList<>();
        Iterator<Double> uniforms = gf.uniform(0.0, 1.0);
        while (false == times.isEmpty()) {
            int index = (int) Math.round(Math.floor(uniforms.next() * times.size()));
            long timeToRemoveAsMilli = times.remove(index);
            assertThat(timeToRemoveAsMilli, is(notNullValue()));
            timesToRemoveList.add(timeToRemoveAsMilli);
        }
        assertThat(timesToRemoveList.size(), is(timesCount));
        Iterator<Long> timesToRemove = timesToRemoveList.iterator();

        long startTimeAsMilli = timeSource.nowAsMilli();

        // add all times
        while (timesToAdd.hasNext()) {
            long timeAsMilli = timesToAdd.next();
            tracker.addCompletedTimeAsMilli(timeAsMilli);
        }

        // remove all times
        while (timesToRemove.hasNext()) {
            long timeAsMilli = timesToRemove.next();
            tracker.removeTimesLowerThanAndReturnHighestRemoved(timeAsMilli);
        }

        long finishTimeAsMilli = timeSource.nowAsMilli();

        return finishTimeAsMilli - startTimeAsMilli;
    }

    public long benchmarkTimesJustAddedAsMilli(
            LocalCompletionTimeStateManager.LocalCompletedTimeTrackerImpl tracker,
            int timesCount) throws CompletionTimeException {
        TimeSource timeSource = new SystemTimeSource();
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));

        List<Long> times = Lists.newArrayList(
                gf.limit(
                        gf.incrementing(0l, 1l),
                        timesCount)
        );

        long startTimeAsMilli = timeSource.nowAsMilli();

        // add all times
        for (long timeAsMilli : times) {
            tracker.addCompletedTimeAsMilli(timeAsMilli);
        }

        long finishTimeAsMilli = timeSource.nowAsMilli();

        return finishTimeAsMilli - startTimeAsMilli;
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
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Long.MAX_VALUE), is(-1l));
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
        tracker.addCompletedTimeAsMilli(1l);

        // Then
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(0l), is(-1l));
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(1l), is(-1l));
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Long.MAX_VALUE), is(1l));
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
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(0l), is(-1l));
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Long.MAX_VALUE), is(-1l));

        // [1]
        tracker.addCompletedTimeAsMilli(1l);

        // [1]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(0l), is(-1l));
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(1l), is(-1l));

        // [1,2]
        tracker.addCompletedTimeAsMilli(2l);

        // [1,2]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(0l), is(-1l));
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(1l), is(-1l));

        // [1,2,3]
        tracker.addCompletedTimeAsMilli(3l);

        // [ ,2,3]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(2l), is(1l));

        // [ ,2,3,4]
        tracker.addCompletedTimeAsMilli(4l);

        // [ ,2,3,4,5,6,7]
        tracker.addCompletedTimeAsMilli(5l);
        tracker.addCompletedTimeAsMilli(6l);
        tracker.addCompletedTimeAsMilli(7l);

        // [ , , , ,5,6,7]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(5l), is(4l));

        // [ , , , ,5,6,7,8,9,10]
        tracker.addCompletedTimeAsMilli(8l);
        tracker.addCompletedTimeAsMilli(9l);
        tracker.addCompletedTimeAsMilli(10l);

        // [ , , , ,5,6,7,8,9,10,11,14,15]
        tracker.addCompletedTimeAsMilli(11l);
        tracker.addCompletedTimeAsMilli(14l);
        tracker.addCompletedTimeAsMilli(15l);

        // [ , , , , , , , , , , ,14,15]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(13l), is(11l));

        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Long.MAX_VALUE), is(15l));
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Long.MAX_VALUE), is(-1l));
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
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(0l), is(-1l));
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Long.MAX_VALUE), is(-1l));

        // [1]
        tracker.addCompletedTimeAsMilli(1l);

        // [0,0,1,1]
        tracker.addCompletedTimeAsMilli(0l);
        tracker.addCompletedTimeAsMilli(0l);
        tracker.addCompletedTimeAsMilli(1l);

        // [0,0,1,1,9,2,6]
        tracker.addCompletedTimeAsMilli(9l);
        tracker.addCompletedTimeAsMilli(2l);
        tracker.addCompletedTimeAsMilli(6l);

        // [ , , , ,9, ,6]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(4l), is(2l));

        // [ , , , ,9, ,6,1,0,0,4]
        tracker.addCompletedTimeAsMilli(1l);
        tracker.addCompletedTimeAsMilli(0l);
        tracker.addCompletedTimeAsMilli(0l);
        tracker.addCompletedTimeAsMilli(4l);

        // [ , , , ,9, ,6, , , ,4]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(4l), is(1l));

        // [ , , , ,9, ,6, , , ,4,1,2,3,4,5,6,7,8,9]
        tracker.addCompletedTimeAsMilli(1l);
        tracker.addCompletedTimeAsMilli(2l);
        tracker.addCompletedTimeAsMilli(3l);
        tracker.addCompletedTimeAsMilli(4l);
        tracker.addCompletedTimeAsMilli(5l);
        tracker.addCompletedTimeAsMilli(6l);
        tracker.addCompletedTimeAsMilli(7l);
        tracker.addCompletedTimeAsMilli(8l);
        tracker.addCompletedTimeAsMilli(9l);

        // [ , , , ,9, ,6, , , , , , , , , ,6,7,8,9]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(6l), is(5l));

        // [ , , , ,9, ,6, , , , , , , , , ,6,7,8,9,10]
        tracker.addCompletedTimeAsMilli(10l);

        // [ , , , ,9, ,6, , , , , , , , , ,6,7,8,9,10]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(6l), is(-1l));

        // [ , , , ,9, , , , , , , , , , , , , ,8,9,10]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(8l), is(7l));

        // [ , , , ,9, , , , , , , , , , , , , ,8,9,10,0]
        tracker.addCompletedTimeAsMilli(0l);

        // [ , , , ,9, , , , , , , , , , , , , ,8,9,10,0,15]
        tracker.addCompletedTimeAsMilli(15l);

        // [ , , , ,9, , , , , , , , , , , , , ,8,9,10, ,15]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(6l), is(0l));

        // [ , , , , , , , , , , , , , , , , , , , , , ,15]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(12l), is(10l));

        // [ , , , , , , , , , , , , , , , , , , , , , ,15]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(15l), is(-1l));

        // [ , , , , , , , , , , , , , , , , , , , , , , ]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(16l), is(15l));

        // [ , , , , , , , , , , , , , , , , , , , , , , ]
        assertThat(tracker.removeTimesLowerThanAndReturnHighestRemoved(Long.MAX_VALUE), is(-1l));
    }
}
