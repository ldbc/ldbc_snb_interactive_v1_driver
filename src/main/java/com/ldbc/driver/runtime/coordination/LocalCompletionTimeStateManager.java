package com.ldbc.driver.runtime.coordination;

import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.TreeMultiset;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.Function1;
import com.ldbc.driver.util.Function2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Completion time is the point in time AT which there are no uncompleted events.
 * It is not possible that there are uncompleted events AT that time.
 * <p/>
 * Approximately --> Completion Time = min( min(Initiated Events), max(Completed Events) )
 * <p/>
 * But not exactly, as Completion Time is ALWAYS lower than min(Initiated Events).
 * <p/>
 * This class performs the logic of tracking completion time. It is NOT thread-safe.
 */
public class LocalCompletionTimeStateManager implements LocalCompletionTimeReaderWriter {
    private Time localCompletionTime = null;
    private final LocalInitiatedTimeTracker localInitiatedTimeTracker = LocalInitiatedTimeTrackerImpl.createUsingTreeMultiSet();
    private final LocalCompletedTimeTracker localCompletedTimeTracker = LocalCompletedTimeTrackerImpl.createUsingTreeMultiSet();
    private Time lastKnownLowestInitiatedTime = null;

    LocalCompletionTimeStateManager() {
    }

    @Override
    public Time lastKnownLowestInitiatedTimeAsMilli() throws CompletionTimeException {
        return lastKnownLowestInitiatedTime;
    }

    @Override
    public Time localCompletionTimeAsMilli() {
        return localCompletionTime;
    }

    /**
     * Logs the new initiated time and updates completion time accordingly.
     * NOTE, initiated times MUST be applied in ascending order!
     *
     * @param timeAsMilli
     */
    @Override
    public void submitLocalInitiatedTime(Time timeAsMilli) throws CompletionTimeException {
        if (null == timeAsMilli) throw new CompletionTimeException("Submitted initiated time may not be null");
        lastKnownLowestInitiatedTime = localInitiatedTimeTracker.addInitiatedTimeAndReturnLastKnownLowestTime(timeAsMilli);
        updateCompletionTime();
    }

    /**
     * Logs the new completed time and updates completion time accordingly.
     *
     * @param timeAsMilli
     * @throws com.ldbc.driver.runtime.coordination.CompletionTimeException
     */
    @Override
    public void submitLocalCompletedTime(Time timeAsMilli) throws CompletionTimeException {
        if (null == timeAsMilli) throw new CompletionTimeException("Submitted completed time may not be null");
        lastKnownLowestInitiatedTime = localInitiatedTimeTracker.removeTimeAndReturnLastKnownLowestTime(timeAsMilli);
        localCompletedTimeTracker.addCompletedTime(timeAsMilli);
        updateCompletionTime();
    }

    private void updateCompletionTime() {
        Time highestSafeCompletedTime = localCompletedTimeTracker.removeTimesLowerThanAndReturnHighestRemoved(lastKnownLowestInitiatedTime);
        if (null != highestSafeCompletedTime)
            localCompletionTime = highestSafeCompletedTime;
    }

    static interface LocalCompletedTimeTracker {
        void addCompletedTime(Time completedTime);

        Time removeTimesLowerThanAndReturnHighestRemoved(Time time);
    }

    public static interface LocalInitiatedTimeTracker {
        Time addInitiatedTimeAndReturnLastKnownLowestTime(Time initiatedTime) throws CompletionTimeException;

        Time removeTimeAndReturnLastKnownLowestTime(Time time) throws CompletionTimeException;

        Time highestInitiatedTime();

        int uncompletedInitiatedTimes();
    }

    static class LocalCompletedTimeTrackerImpl<INITIATED_TIMES_CONTAINER_TYPE extends Collection<Time>> implements LocalCompletedTimeTracker {
        private final INITIATED_TIMES_CONTAINER_TYPE completedTimes;
        private final Function2<INITIATED_TIMES_CONTAINER_TYPE, Time, Time> removeTimesLowerThanAndReturnHighestRemovedFun;

        static LocalCompletedTimeTrackerImpl createUsingTreeMultiSet() {
            Function2<TreeMultiset<Time>, Time, Time> removeTimesLowerThanAndReturnHighestRemovedFun = new Function2<TreeMultiset<Time>, Time, Time>() {
                @Override
                public Time apply(TreeMultiset<Time> completedTimes, Time time) {
                    Time highestRemoved = null;
                    for (Time completedTime : completedTimes) {
                        if (completedTime.lt(time)) {
                            completedTimes.remove(completedTime);
                            highestRemoved = completedTime;
                        } else {
                            break;
                        }
                    }
                    return highestRemoved;
                }
            };
            return new LocalCompletedTimeTrackerImpl(TreeMultiset.<Time>create(), removeTimesLowerThanAndReturnHighestRemovedFun);
        }

        static LocalCompletedTimeTrackerImpl createUsingArrayList() {
            Function2<ArrayList<Time>, Time, Time> removeTimesLowerThanAndReturnHighestRemovedFun = new Function2<ArrayList<Time>, Time, Time>() {
                @Override
                public Time apply(ArrayList<Time> completedTimes, Time time) {
                    Time highestRemoved = null;
                    Iterator<Time> completedTimesIterator = completedTimes.iterator();
                    while (completedTimesIterator.hasNext()) {
                        Time completedTime = completedTimesIterator.next();
                        if (completedTime.lt(time)) {
                            completedTimesIterator.remove();
                            if (null == highestRemoved || completedTime.gt(highestRemoved)) {
                                highestRemoved = completedTime;
                            }
                        }
                    }
                    return highestRemoved;
                }
            };
            return new LocalCompletedTimeTrackerImpl(Lists.<Time>newArrayList(), removeTimesLowerThanAndReturnHighestRemovedFun);
        }

        private LocalCompletedTimeTrackerImpl(INITIATED_TIMES_CONTAINER_TYPE completedTimes,
                                              Function2<INITIATED_TIMES_CONTAINER_TYPE, Time, Time> removeTimesLowerThanAndReturnHighestRemovedFun) {
            this.completedTimes = completedTimes;
            this.removeTimesLowerThanAndReturnHighestRemovedFun = removeTimesLowerThanAndReturnHighestRemovedFun;
        }

        @Override
        public void addCompletedTime(Time time) {
            completedTimes.add(time);
        }

        @Override
        public Time removeTimesLowerThanAndReturnHighestRemoved(Time time) {
            return removeTimesLowerThanAndReturnHighestRemovedFun.apply(completedTimes, time);
        }

        @Override
        public String toString() {
            return "TreeMultisetLocalCompletedTimeTracker{" +
                    "completedTimes=" + completedTimes +
                    '}';
        }
    }

    static class LocalInitiatedTimeTrackerImpl<INITIATED_TIMES_CONTAINER_TYPE extends Collection<Time>> implements LocalInitiatedTimeTracker {
        private final INITIATED_TIMES_CONTAINER_TYPE initiatedTimes;
        private final Function1<INITIATED_TIMES_CONTAINER_TYPE, Time> getLastKnownLowestInitiatedTimeFun;
        private Time lastKnownLowestInitiatedTime = null;
        private Time highestInitiatedTime = null;
        private int uncompletedInitiatedTimes = 0;

        static LocalInitiatedTimeTrackerImpl createUsingTreeMultiSet() {
            Function1<TreeMultiset<Time>, Time> getLastKnownLowestInitiatedTimeFun = new Function1<TreeMultiset<Time>, Time>() {
                @Override
                public Time apply(TreeMultiset<Time> initiatedTimes) {
                    return initiatedTimes.firstEntry().getElement();
                }
            };
            return new LocalInitiatedTimeTrackerImpl(TreeMultiset.<Time>create(), getLastKnownLowestInitiatedTimeFun);
        }

        static LocalInitiatedTimeTrackerImpl createUsingArrayList() {
            Function1<List<Time>, Time> getLastKnownLowestInitiatedTimeFun = new Function1<List<Time>, Time>() {
                @Override
                public Time apply(List<Time> initiatedTimes) {
                    return initiatedTimes.get(0);
                }
            };
            return new LocalInitiatedTimeTrackerImpl(Lists.<Time>newArrayList(), getLastKnownLowestInitiatedTimeFun);
        }

        static LocalInitiatedTimeTrackerImpl createUsingMinMaxPriorityQueue() {
            // TODO this seems to have a bug for larger values, don't use it
            System.err.println(String.format("LocalInitiatedTimeTrackerImpl.createUsingMinMaxPriorityQueue() is buggy. When collection size is very large values seem to get dropped. DO NOT USE!"));
            MinMaxPriorityQueue.<Time>create();
            Function1<MinMaxPriorityQueue<Time>, Time> getLastKnownLowestInitiatedTimeFun = new Function1<MinMaxPriorityQueue<Time>, Time>() {
                @Override
                public Time apply(MinMaxPriorityQueue<Time> initiatedTimes) {
                    return initiatedTimes.peekFirst();
                }
            };
            return new LocalInitiatedTimeTrackerImpl(MinMaxPriorityQueue.<Time>create(), getLastKnownLowestInitiatedTimeFun);
        }

        private LocalInitiatedTimeTrackerImpl(INITIATED_TIMES_CONTAINER_TYPE initiatedTimes,
                                              Function1<INITIATED_TIMES_CONTAINER_TYPE, Time> getLastKnownLowestInitiatedTimeFun) {
            this.initiatedTimes = initiatedTimes;
            this.getLastKnownLowestInitiatedTimeFun = getLastKnownLowestInitiatedTimeFun;
        }

        @Override
        public Time addInitiatedTimeAndReturnLastKnownLowestTime(Time time) throws CompletionTimeException {
            if (null != highestInitiatedTime && time.lt(highestInitiatedTime)) {
                String errMsg = String.format("Submitted initiated time is lower than previously submitted initiated time\n"
                                + "  Submitted: %s (%s ns)\n"
                                + "  Previous: %s (%s ns)",
                        time, time.asNano(),
                        highestInitiatedTime, highestInitiatedTime.asNano()
                );
                throw new CompletionTimeException(errMsg);
            }
            highestInitiatedTime = time;

            if (0 == uncompletedInitiatedTimes)
                lastKnownLowestInitiatedTime = time;
            initiatedTimes.add(time);
            uncompletedInitiatedTimes++;
            return lastKnownLowestInitiatedTime;
        }

        @Override
        public Time removeTimeAndReturnLastKnownLowestTime(Time time) throws CompletionTimeException {
            if (initiatedTimes.remove(time)) {
                uncompletedInitiatedTimes--;
                if (0 == uncompletedInitiatedTimes)
                    lastKnownLowestInitiatedTime = highestInitiatedTime;
                else
                    lastKnownLowestInitiatedTime = getLastKnownLowestInitiatedTimeFun.apply(initiatedTimes);
                return lastKnownLowestInitiatedTime;
            } else {
                throw new CompletionTimeException(String.format("Initiated time [%s] of completed event does not map to any uncompleted operation", time));
            }
        }

        @Override
        public Time highestInitiatedTime() {
            return highestInitiatedTime;
        }

        @Override
        public int uncompletedInitiatedTimes() {
            return uncompletedInitiatedTimes;
        }

        @Override
        public String toString() {
            return "TreeMultisetLocalInitiatedTimeTracker{" +
                    "initiatedTimes=" + initiatedTimes +
                    ", lastKnownLowestInitiatedTime=" + lastKnownLowestInitiatedTime +
                    ", highestInitiatedTime=" + highestInitiatedTime +
                    ", uncompletedInitiatedTimes=" + uncompletedInitiatedTimes +
                    '}';
        }
    }
}
