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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class LocalInitiatedTimeTrackerImplTest {
    /*
     ------- PERFORMANCE -------
     */

    @Ignore
    @Test
    public void comparePerformanceOfImplementations() throws CompletionTimeException {
        int timesCountBigger = 1000000;
        int timesCountSmaller = 10000;
        int benchmarkRepetitions = 5;

        long totalDuration_TreeMultiset_AddedRemovedImmediately = 0l;
        long totalDuration_List_AddedRemovedImmediately = 0l;
        long totalDuration_MinMaxPriorityQueue_AddedRemovedImmediately = 0l;

        long totalDuration_TreeMultiset_AddAllThenRemoveAllSequentially = 0l;
        long totalDuration_List_AddAllThenRemoveAllSequentially = 0l;
        long totalDuration_MinMaxPriorityQueue_AddAllThenRemoveAllSequentially = 0l;

        long totalDuration_TreeMultiSet_JustAdd = 0l;
        long totalDuration_List_JustAdd = 0l;
        long totalDuration_MinMaxPriorityQueue_JustAdd = 0l;

        long totalDuration_TreeMultiset_AddAllThenRemoveAllRandomly = 0l;
        long totalDuration_List_AddAllThenRemoveAllRandomly = 0l;
        long totalDuration_MinMaxPriorityQueue_AddAllThenRemoveAllRandomly = 0l;

        for (int i = 0; i < benchmarkRepetitions; i++) {
            // ******
            // Added Removed Immediately
            // ******
            totalDuration_TreeMultiset_AddedRemovedImmediately =
                    totalDuration_TreeMultiset_AddedRemovedImmediately +
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAddedAndRemovedImmediately(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingTreeMultiSet(),
                                    timesCountBigger
                            );

            totalDuration_List_AddedRemovedImmediately =
                    totalDuration_List_AddedRemovedImmediately +
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAddedAndRemovedImmediately(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingArrayList(),
                                    timesCountBigger
                            );
            totalDuration_MinMaxPriorityQueue_AddedRemovedImmediately =
                    totalDuration_MinMaxPriorityQueue_AddedRemovedImmediately +
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAddedAndRemovedImmediately(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingMinMaxPriorityQueue(),
                                    timesCountBigger
                            );

            // ******
            // Add All Then Remove All Sequentially
            // ******
            totalDuration_TreeMultiset_AddAllThenRemoveAllSequentially =
                    totalDuration_TreeMultiset_AddAllThenRemoveAllSequentially +
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAllAddedThenAllRemovedSequentially(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingTreeMultiSet(),
                                    timesCountSmaller
                            );

            totalDuration_List_AddAllThenRemoveAllSequentially =
                    totalDuration_List_AddAllThenRemoveAllSequentially +
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAllAddedThenAllRemovedSequentially(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingArrayList(),
                                    timesCountSmaller
                            );
            totalDuration_MinMaxPriorityQueue_AddAllThenRemoveAllSequentially =
                    totalDuration_MinMaxPriorityQueue_AddAllThenRemoveAllSequentially +
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAllAddedThenAllRemovedSequentially(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingMinMaxPriorityQueue(),
                                    timesCountSmaller
                            );

            // ******
            // Just Add
            // ******
            totalDuration_TreeMultiSet_JustAdd =
                    totalDuration_TreeMultiSet_JustAdd +
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesJustAdd(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingTreeMultiSet(),
                                    timesCountBigger
                            );

            totalDuration_List_JustAdd =
                    totalDuration_List_JustAdd +
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesJustAdd(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingArrayList(),
                                    timesCountBigger
                            );

            totalDuration_MinMaxPriorityQueue_JustAdd =
                    totalDuration_MinMaxPriorityQueue_JustAdd +
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesJustAdd(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingMinMaxPriorityQueue(),
                                    timesCountBigger
                            );

            // ******
            // Add All Then Remove All Randomly
            // ******
            totalDuration_TreeMultiset_AddAllThenRemoveAllRandomly =
                    totalDuration_TreeMultiset_AddAllThenRemoveAllRandomly +
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAllAddedThenAllRemovedRandomly(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingTreeMultiSet(),
                                    timesCountSmaller
                            );

            totalDuration_List_AddAllThenRemoveAllRandomly =
                    totalDuration_List_AddAllThenRemoveAllRandomly +
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAllAddedThenAllRemovedRandomly(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingArrayList(),
                                    timesCountSmaller
                            );

            totalDuration_MinMaxPriorityQueue_AddAllThenRemoveAllRandomly =
                    totalDuration_MinMaxPriorityQueue_AddAllThenRemoveAllRandomly +
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAllAddedThenAllRemovedRandomly(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingMinMaxPriorityQueue(),
                                    timesCountSmaller
                            );
        }

        System.out.println("TreeMultiSet (add " + timesCountBigger + "):\t\t\t\t\t\t\t\t\t" + totalDuration_TreeMultiSet_JustAdd + " <- add to large/growing");
        System.out.println("List (add " + timesCountBigger + "):\t\t\t\t\t\t\t\t\t\t\t" + totalDuration_List_JustAdd + " <- add to large/growing");
        System.out.println("MinMaxPriorityQueue (add " + timesCountBigger + "):\t\t\t\t\t\t\t" + totalDuration_MinMaxPriorityQueue_JustAdd + " <- add to large/growing");
        System.out.println();
        System.out.println("TreeMultiSet (add one/remove one x " + timesCountBigger + "):\t\t\t\t" + totalDuration_TreeMultiset_AddedRemovedImmediately + " <- add to small`,remove from small sequentially");
        System.out.println("List (add one/remove one):\t\t\t\t\t\t\t\t\t" + totalDuration_List_AddedRemovedImmediately + " <- add to small,remove from small sequentially");
        System.out.println("MinMaxPriorityQueue (add one/remove one x " + timesCountBigger + "):\t\t\t" + totalDuration_MinMaxPriorityQueue_AddedRemovedImmediately + " <- add to small,remove from small sequentially");
        System.out.println();
        System.out.println("TreeMultiSet (add " + timesCountSmaller + "/remove " + timesCountSmaller + " sequentially):\t\t\t" + totalDuration_TreeMultiset_AddAllThenRemoveAllSequentially + " <- add to large/growing, removal from large/growing sequentially");
        System.out.println("List (add " + timesCountSmaller + "/remove " + timesCountSmaller + " sequentially):\t\t\t\t\t" + totalDuration_List_AddAllThenRemoveAllSequentially + " <- add to large/growing, removal from large/growing sequentially");
        System.out.println("MinMaxPriorityQueue (add " + timesCountSmaller + "/remove " + timesCountSmaller + " sequentially):\t" + totalDuration_MinMaxPriorityQueue_AddAllThenRemoveAllSequentially + " <- add to large/growing, removal from large/growing sequentially");
        System.out.println();
        System.out.println("TreeMultiSet (add " + timesCountSmaller + "/remove " + timesCountSmaller + " randomly):\t\t\t\t" + totalDuration_TreeMultiset_AddAllThenRemoveAllRandomly + " <- add to large/growing, removal from large/growing randomly");
        System.out.println("List (add " + timesCountSmaller + "/remove " + timesCountSmaller + " randomly):\t\t\t\t\t\t" + totalDuration_List_AddAllThenRemoveAllRandomly + " <- add to large/growing, removal from large/growing randomly");
        System.out.println("MinMaxPriorityQueue (add " + timesCountSmaller + "/remove " + timesCountSmaller + " randomly):\t\t" + totalDuration_MinMaxPriorityQueue_AddAllThenRemoveAllRandomly + " <- add to large/growing, removal from large/growing randomly");
    }

    public long performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAddedAndRemovedImmediately(
            LocalCompletionTimeStateManager.LocalInitiatedTimeTracker tracker,
            int timesCount) throws CompletionTimeException {
        TimeSource timeSource = new SystemTimeSource();
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));

        Iterator<Long> times = gf.limit(
                gf.incrementing(0l, 1l),
                timesCount
        );

        long startTimeAsMilli = timeSource.nowAsMilli();

        while (times.hasNext()) {
            long time = times.next();
            tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli(time);
            tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(time);
            tracker.uncompletedInitiatedTimes();
            tracker.highestInitiatedTimeAsMilli();
        }

        long finishTime = timeSource.nowAsMilli();

        return finishTime - startTimeAsMilli;
    }

    public long performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAllAddedThenAllRemovedSequentially(
            LocalCompletionTimeStateManager.LocalInitiatedTimeTracker tracker,
            int timesCount) throws CompletionTimeException {
        TimeSource timeSource = new SystemTimeSource();
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));

        List<Long> times = Lists.newArrayList(
                gf.limit(
                        gf.incrementing(0l, 1l),
                        timesCount
                )
        );

        long startTime = timeSource.nowAsMilli();

        // add all times
        for (long time : times) {
            tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli(time);
            tracker.uncompletedInitiatedTimes();
            tracker.highestInitiatedTimeAsMilli();
        }

        // remove all times
        for (long time : times) {
            tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(time);
            tracker.uncompletedInitiatedTimes();
            tracker.highestInitiatedTimeAsMilli();
        }

        long finishTime = timeSource.nowAsMilli();

        return finishTime - startTime;
    }

    public long performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAllAddedThenAllRemovedRandomly(
            LocalCompletionTimeStateManager.LocalInitiatedTimeTracker tracker,
            int timesCount) throws CompletionTimeException {
        TimeSource timeSource = new SystemTimeSource();
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));

        final List<Long> times = Lists.newArrayList(
                gf.limit(
                        gf.incrementing(0l, 1l),
                        timesCount
                )
        );

        // create iterator to add times from, in sequential order <-- requirement of tracker
        Iterator<Long> timesToAdd = Lists.newArrayList(times).iterator();

        // create iterator to remove times from, in random order
        List<Long> timesToRemoveList = new ArrayList<>();
        Iterator<Double> uniforms = gf.uniform(0.0, 1.0);
        while (false == times.isEmpty()) {
            int index = (int) Math.round(Math.floor(uniforms.next() * times.size()));
            long timeToRemove = times.remove(index);
            assertThat(timeToRemove, is(not(-1l)));
            timesToRemoveList.add(timeToRemove);
        }
        assertThat(timesToRemoveList.size(), is(timesCount));
        Iterator<Long> timesToRemove = timesToRemoveList.iterator();

        long startTime = timeSource.nowAsMilli();

        // add all times
        while (timesToAdd.hasNext()) {
            long time = timesToAdd.next();
            tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli(time);
            tracker.uncompletedInitiatedTimes();
            tracker.highestInitiatedTimeAsMilli();
        }

        assertThat(tracker.uncompletedInitiatedTimes(), is(timesCount));

        // remove all times
        while (timesToRemove.hasNext()) {
            long time = timesToRemove.next();
            tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(time);
            tracker.uncompletedInitiatedTimes();
            tracker.highestInitiatedTimeAsMilli();
        }

        long finishTime = timeSource.nowAsMilli();

        assertThat(tracker.uncompletedInitiatedTimes(), is(0));

        return finishTime - startTime;
    }

    public long performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesJustAdd(
            LocalCompletionTimeStateManager.LocalInitiatedTimeTracker tracker,
            int timesCount) throws CompletionTimeException {
        TimeSource timeSource = new SystemTimeSource();
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));

        List<Long> times = Lists.newArrayList(
                gf.limit(
                        gf.incrementing(0l, 1l),
                        timesCount
                )
        );

        long startTime = timeSource.nowAsMilli();

        // add all times
        for (long time : times) {
            tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli(time);
            tracker.uncompletedInitiatedTimes();
            tracker.highestInitiatedTimeAsMilli();
        }

        long finishTime = timeSource.nowAsMilli();

        return finishTime - startTime;
    }

    /*
     ------- CORRECTNESS -------
     */

    @Test
    public void shouldReturnNullsWhenNoTimesHaveBeenSubmitted_TreeMultiSetImplementation() {
        shouldReturnNullsWhenNoTimesHaveBeenSubmitted(LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingTreeMultiSet());
    }

    @Test
    public void shouldReturnNullsWhenNoTimesHaveBeenSubmitted_ListImplementation() {
        shouldReturnNullsWhenNoTimesHaveBeenSubmitted(LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingArrayList());
    }

    @Test
    public void shouldReturnNullsWhenNoTimesHaveBeenSubmitted_MinMaxPriorityQueueImplementation() {
        shouldReturnNullsWhenNoTimesHaveBeenSubmitted(LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingMinMaxPriorityQueue());
    }

    public void shouldReturnNullsWhenNoTimesHaveBeenSubmitted(LocalCompletionTimeStateManager.LocalInitiatedTimeTracker tracker) {
        // Given
        // tracker

        // When
        // nothing

        // Then
        assertThat(tracker.highestInitiatedTimeAsMilli(), is(-1l));
        boolean exceptionThrown = false;
        try {
            tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(1l);
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
        assertThat(tracker.uncompletedInitiatedTimes(), is(0));
    }

    @Test
    public void shouldBehaveAsExpectedUnderScenario1_TreeMultiSetImplementation() throws CompletionTimeException {
        shouldBehaveAsExpectedUnderScenario1(LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingTreeMultiSet());
    }

    @Test
    public void shouldBehaveAsExpectedUnderScenario1_ListImplementation() throws CompletionTimeException {
        shouldBehaveAsExpectedUnderScenario1(LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingArrayList());
    }

    @Test
    public void shouldBehaveAsExpectedUnderScenario1_MinMaxPriorityQueueImplementation() throws CompletionTimeException {
        shouldBehaveAsExpectedUnderScenario1(LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingMinMaxPriorityQueue());
    }

    public void shouldBehaveAsExpectedUnderScenario1(LocalCompletionTimeStateManager.LocalInitiatedTimeTracker tracker) throws CompletionTimeException {
        // Given
        // tracker

        // When/Then
        assertThat(tracker.highestInitiatedTimeAsMilli(), is(-1l));
        boolean exceptionThrown = false;
        try {
            tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(1l);
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
        assertThat(tracker.uncompletedInitiatedTimes(), is(0));

        // [0]
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli(0l), equalTo(0l));

        assertThat(tracker.highestInitiatedTimeAsMilli(), is(0l));
        assertThat(tracker.uncompletedInitiatedTimes(), is(1));

        // [0]
        exceptionThrown = false;
        try {
            tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(1l);
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));

        // [0,0]
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli(0l), equalTo(0l));

        assertThat(tracker.highestInitiatedTimeAsMilli(), is(0l));
        assertThat(tracker.uncompletedInitiatedTimes(), is(2));

        // [0,0]
        exceptionThrown = false;
        try {
            tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(1l);
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));

        // [0,0,1,1,4,5]
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli(1l), equalTo(0l));
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli(1l), equalTo(0l));
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli(4l), equalTo(0l));
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli(5l), equalTo(0l));

        assertThat(tracker.highestInitiatedTimeAsMilli(), is(5l));
        assertThat(tracker.uncompletedInitiatedTimes(), is(6));

        // [0,0,1,1,4,5,7,9,10,15]
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli(7l), equalTo(0l));
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli(9l), equalTo(0l));
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli(10l), equalTo(0l));
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli(15l), equalTo(0l));

        assertThat(tracker.highestInitiatedTimeAsMilli(), is(15l));
        assertThat(tracker.uncompletedInitiatedTimes(), is(10));

        // [0,0,1,1,4,5,7,9,10,15]
        exceptionThrown = false;
        try {
            tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(2l);
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
        exceptionThrown = false;
        try {
            tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(3l);
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));

        // [ ,0,1,1,4,5,7,9,10,15]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(0l), is(0l));
        // [ ,0,1,1,4, ,7,9,10,15]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(5l), is(0l));
        // [ ,0,1,1,4, ,7,9, ,15]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(10l), is(0l));

        // [ ,0,1,1,4, ,7,9, ,15]
        assertThat(tracker.highestInitiatedTimeAsMilli(), is(15l));
        assertThat(tracker.uncompletedInitiatedTimes(), is(7));

        // [ , ,1,1,4, ,7,9, ,15]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(0l), is(1l));

        // [ , ,1,1, , ,7,9, ,15]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(4l), is(1l));

        // [ , ,1,1, , ,7, , ,15]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(9l), is(1l));

        // [ , ,1,1, , ,7, , ,15]
        assertThat(tracker.highestInitiatedTimeAsMilli(), is(15l));
        assertThat(tracker.uncompletedInitiatedTimes(), is(4));

        // [ , ,1,1, , ,7, , ,15,15,15]
        exceptionThrown = false;
        try {
            tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli(14l);
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli(15l), equalTo(1l));
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli(15l), equalTo(1l));

        assertThat(tracker.highestInitiatedTimeAsMilli(), is(15l));
        assertThat(tracker.uncompletedInitiatedTimes(), is(6));

        // [ , ,1,1, , ,7, , , ,15,15]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(15l), is(1l));

        // [ , ,1,1, , ,7, , , , ,15]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(15l), is(1l));

        // [ , ,1,1, , ,7, , , , , ]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(15l), is(1l));

        // [ , ,1,1, , ,7, , , , , ]
        assertThat(tracker.highestInitiatedTimeAsMilli(), is(15l));
        assertThat(tracker.uncompletedInitiatedTimes(), is(3));

        // [ , , ,1, , ,7, , , , , ]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(1l), is(1l));

        // [ , , , , , ,7, , , , , ]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(1l), is(7l));

        // [ , , , , , ,7, , , , , ]
        assertThat(tracker.highestInitiatedTimeAsMilli(), is(15l));
        assertThat(tracker.uncompletedInitiatedTimes(), is(1));

        // [ , , , , , , , , , , , ]
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(7l), is(15l));

        // [ , , , , , , , , , , , ]
        assertThat(tracker.highestInitiatedTimeAsMilli(), is(15l));
        assertThat(tracker.uncompletedInitiatedTimes(), is(0));

        for (int i = 16; i < 10000; i++) {
            assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli(i), equalTo(16l));
        }
        for (int i = 16; i < 9999; i++) {
            assertThat(tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(i), is(i + 1l));
        }
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTimeAsMilli(9999l), is(9999l));
    }
}
