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

        Duration totalDuration_TreeMultiset_AddedRemovedImmediately = Duration.fromMilli(0);
        Duration totalDuration_List_AddedRemovedImmediately = Duration.fromMilli(0);
        Duration totalDuration_MinMaxPriorityQueue_AddedRemovedImmediately = Duration.fromMilli(0);

        Duration totalDuration_TreeMultiset_AddAllThenRemoveAllSequentially = Duration.fromMilli(0);
        Duration totalDuration_List_AddAllThenRemoveAllSequentially = Duration.fromMilli(0);
        Duration totalDuration_MinMaxPriorityQueue_AddAllThenRemoveAllSequentially = Duration.fromMilli(0);

        Duration totalDuration_TreeMultiSet_JustAdd = Duration.fromMilli(0);
        Duration totalDuration_List_JustAdd = Duration.fromMilli(0);
        Duration totalDuration_MinMaxPriorityQueue_JustAdd = Duration.fromMilli(0);

        Duration totalDuration_TreeMultiset_AddAllThenRemoveAllRandomly = Duration.fromMilli(0);
        Duration totalDuration_List_AddAllThenRemoveAllRandomly = Duration.fromMilli(0);
        Duration totalDuration_MinMaxPriorityQueue_AddAllThenRemoveAllRandomly = Duration.fromMilli(0);

        for (int i = 0; i < benchmarkRepetitions; i++) {
            // ******
            // Added Removed Immediately
            // ******
            totalDuration_TreeMultiset_AddedRemovedImmediately =
                    totalDuration_TreeMultiset_AddedRemovedImmediately.plus(
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAddedAndRemovedImmediately(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingTreeMultiSet(),
                                    timesCountBigger
                            )
                    );

            totalDuration_List_AddedRemovedImmediately =
                    totalDuration_List_AddedRemovedImmediately.plus(
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAddedAndRemovedImmediately(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingArrayList(),
                                    timesCountBigger
                            )
                    );
            totalDuration_MinMaxPriorityQueue_AddedRemovedImmediately =
                    totalDuration_MinMaxPriorityQueue_AddedRemovedImmediately.plus(
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAddedAndRemovedImmediately(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingMinMaxPriorityQueue(),
                                    timesCountBigger
                            )
                    );

            // ******
            // Add All Then Remove All Sequentially
            // ******
            totalDuration_TreeMultiset_AddAllThenRemoveAllSequentially =
                    totalDuration_TreeMultiset_AddAllThenRemoveAllSequentially.plus(
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAllAddedThenAllRemovedSequentially(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingTreeMultiSet(),
                                    timesCountSmaller
                            )
                    );

            totalDuration_List_AddAllThenRemoveAllSequentially =
                    totalDuration_List_AddAllThenRemoveAllSequentially.plus(
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAllAddedThenAllRemovedSequentially(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingArrayList(),
                                    timesCountSmaller
                            )
                    );
            totalDuration_MinMaxPriorityQueue_AddAllThenRemoveAllSequentially =
                    totalDuration_MinMaxPriorityQueue_AddAllThenRemoveAllSequentially.plus(
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAllAddedThenAllRemovedSequentially(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingMinMaxPriorityQueue(),
                                    timesCountSmaller
                            )
                    );

            // ******
            // Just Add
            // ******
            totalDuration_TreeMultiSet_JustAdd =
                    totalDuration_TreeMultiSet_JustAdd.plus(
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesJustAdd(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingTreeMultiSet(),
                                    timesCountBigger
                            )
                    );

            totalDuration_List_JustAdd =
                    totalDuration_List_JustAdd.plus(
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesJustAdd(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingArrayList(),
                                    timesCountBigger
                            )
                    );

            totalDuration_MinMaxPriorityQueue_JustAdd =
                    totalDuration_MinMaxPriorityQueue_JustAdd.plus(
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesJustAdd(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingMinMaxPriorityQueue(),
                                    timesCountBigger
                            )
                    );

            // ******
            // Add All Then Remove All Randomly
            // ******
            totalDuration_TreeMultiset_AddAllThenRemoveAllRandomly =
                    totalDuration_TreeMultiset_AddAllThenRemoveAllRandomly.plus(
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAllAddedThenAllRemovedRandomly(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingTreeMultiSet(),
                                    timesCountSmaller
                            )
                    );

            totalDuration_List_AddAllThenRemoveAllRandomly =
                    totalDuration_List_AddAllThenRemoveAllRandomly.plus(
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAllAddedThenAllRemovedRandomly(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingArrayList(),
                                    timesCountSmaller
                            )
                    );

            totalDuration_MinMaxPriorityQueue_AddAllThenRemoveAllRandomly =
                    totalDuration_MinMaxPriorityQueue_AddAllThenRemoveAllRandomly.plus(
                            performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAllAddedThenAllRemovedRandomly(
                                    LocalCompletionTimeStateManager.LocalInitiatedTimeTrackerImpl.createUsingMinMaxPriorityQueue(),
                                    timesCountSmaller
                            )
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

    public Duration performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAddedAndRemovedImmediately(
            LocalCompletionTimeStateManager.LocalInitiatedTimeTracker tracker,
            int timesCount) throws CompletionTimeException {
        TimeSource timeSource = new SystemTimeSource();
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));

        Iterator<Time> times = gf.limit(
                gf.constantIncrementTime(Time.fromMilli(0), Duration.fromMilli(1)),
                timesCount);

        Time startTime = timeSource.now();

        while (times.hasNext()) {
            Time time = times.next();
            tracker.addInitiatedTimeAndReturnLastKnownLowestTime(time);
            tracker.removeTimeAndReturnLastKnownLowestTime(time);
            tracker.uncompletedInitiatedTimes();
            tracker.highestInitiatedTime();
        }

        Time finishTime = timeSource.now();

        return finishTime.durationGreaterThan(startTime);
    }

    public Duration performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAllAddedThenAllRemovedSequentially(
            LocalCompletionTimeStateManager.LocalInitiatedTimeTracker tracker,
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
            tracker.addInitiatedTimeAndReturnLastKnownLowestTime(time);
            tracker.uncompletedInitiatedTimes();
            tracker.highestInitiatedTime();
        }

        // remove all times
        for (Time time : times) {
            tracker.removeTimeAndReturnLastKnownLowestTime(time);
            tracker.uncompletedInitiatedTimes();
            tracker.highestInitiatedTime();
        }

        Time finishTime = timeSource.now();

        return finishTime.durationGreaterThan(startTime);
    }

    public Duration performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesAllAddedThenAllRemovedRandomly(
            LocalCompletionTimeStateManager.LocalInitiatedTimeTracker tracker,
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
            tracker.addInitiatedTimeAndReturnLastKnownLowestTime(time);
            tracker.uncompletedInitiatedTimes();
            tracker.highestInitiatedTime();
        }

        assertThat(tracker.uncompletedInitiatedTimes(), is(timesCount));

        // remove all times
        while (timesToRemove.hasNext()) {
            Time time = timesToRemove.next();
            tracker.removeTimeAndReturnLastKnownLowestTime(time);
            tracker.uncompletedInitiatedTimes();
            tracker.highestInitiatedTime();
        }

        Time finishTime = timeSource.now();

        assertThat(tracker.uncompletedInitiatedTimes(), is(0));

        return finishTime.durationGreaterThan(startTime);
    }

    public Duration performanceOfLocalInitiatedTimeTrackerImplementationsWhenTimesJustAdd(
            LocalCompletionTimeStateManager.LocalInitiatedTimeTracker tracker,
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
            tracker.addInitiatedTimeAndReturnLastKnownLowestTime(time);
            tracker.uncompletedInitiatedTimes();
            tracker.highestInitiatedTime();
        }

        Time finishTime = timeSource.now();

        return finishTime.durationGreaterThan(startTime);
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
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(0)), equalTo(Time.fromMilli(0)));

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
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(0)), equalTo(Time.fromMilli(0)));

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
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(1)), equalTo(Time.fromMilli(0)));
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(1)), equalTo(Time.fromMilli(0)));
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(4)), equalTo(Time.fromMilli(0)));
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(5)), equalTo(Time.fromMilli(0)));

        assertThat(tracker.highestInitiatedTime(), is(Time.fromMilli(5)));
        assertThat(tracker.uncompletedInitiatedTimes(), is(6));

        // [0,0,1,1,4,5,7,9,10,15]
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(7)), equalTo(Time.fromMilli(0)));
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(9)), equalTo(Time.fromMilli(0)));
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(10)), equalTo(Time.fromMilli(0)));
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(15)), equalTo(Time.fromMilli(0)));

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
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(15)), equalTo(Time.fromMilli(1)));
        assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(15)), equalTo(Time.fromMilli(1)));

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

        for (int i = 16; i < 10000; i++) {
            assertThat(tracker.addInitiatedTimeAndReturnLastKnownLowestTime(Time.fromMilli(i)), equalTo(Time.fromMilli(16)));
        }
        for (int i = 16; i < 9999; i++) {
            assertThat(tracker.removeTimeAndReturnLastKnownLowestTime(Time.fromMilli(i)), is(Time.fromMilli(i + 1)));
        }
        assertThat(tracker.removeTimeAndReturnLastKnownLowestTime(Time.fromMilli(9999)), is(Time.fromMilli(9999)));
    }
}
