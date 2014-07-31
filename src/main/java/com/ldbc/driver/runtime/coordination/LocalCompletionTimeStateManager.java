package com.ldbc.driver.runtime.coordination;

import com.google.common.collect.Lists;
import com.google.common.collect.TreeMultiset;
import com.ldbc.driver.temporal.Time;

import java.util.Collection;

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
    private final LocalInitiatedTimeTracker localInitiatedTimeTracker = new TreeMultisetLocalInitiatedTimeTracker();
    private final LocalCompletedTimeTracker localCompletedTimeTracker = CollectionLocalCompletedTimeTracker.createUsingTreeMultiSet();
    private Time lastKnownLowestInitiatedTime = null;

    LocalCompletionTimeStateManager() {
    }

    @Override
    public Time lastKnownLowestInitiatedTime() throws CompletionTimeException {
        return lastKnownLowestInitiatedTime;
    }

    @Override
    public Time localCompletionTime() {
        return localCompletionTime;
    }

    /**
     * Logs the new initiated time and updates completion time accordingly.
     * NOTE, initiated times MUST be applied in ascending order!
     *
     * @param scheduledStartTime
     */
    @Override
    public void submitLocalInitiatedTime(Time scheduledStartTime) throws CompletionTimeException {
        if (null == scheduledStartTime) throw new CompletionTimeException("Submitted initiated time may not be null");
        lastKnownLowestInitiatedTime = localInitiatedTimeTracker.addInitiatedTimeAndReturnLastKnownLowestTime(scheduledStartTime);
        updateCompletionTime();
    }

    /**
     * Logs the new completed time and updates completion time accordingly.
     *
     * @param scheduledStartTime
     * @throws com.ldbc.driver.runtime.coordination.CompletionTimeException
     */
    @Override
    public void submitLocalCompletedTime(Time scheduledStartTime) throws CompletionTimeException {
        if (null == scheduledStartTime) throw new CompletionTimeException("Submitted completed time may not be null");
        lastKnownLowestInitiatedTime = localInitiatedTimeTracker.removeTimeAndReturnLastKnownLowestTime(scheduledStartTime);
        localCompletedTimeTracker.addCompletedTime(scheduledStartTime);
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

    static class CollectionLocalCompletedTimeTracker implements LocalCompletedTimeTracker {
        private final Collection<Time> completedTimes;

        static CollectionLocalCompletedTimeTracker createUsingTreeMultiSet() {
            return new CollectionLocalCompletedTimeTracker(TreeMultiset.<Time>create());
        }

        static CollectionLocalCompletedTimeTracker createUsingArrayList() {
            return new CollectionLocalCompletedTimeTracker(Lists.<Time>newArrayList());
        }

        private CollectionLocalCompletedTimeTracker(Collection<Time> completedTimes) {
            this.completedTimes = completedTimes;
        }

        @Override
        public void addCompletedTime(Time time) {
            completedTimes.add(time);
        }

        @Override
        public Time removeTimesLowerThanAndReturnHighestRemoved(Time time) {
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

        @Override
        public String toString() {
            return "TreeMultisetLocalCompletedTimeTracker{" +
                    "completedTimes=" + completedTimes +
                    '}';
        }
    }

    static class TreeMultisetLocalInitiatedTimeTracker implements LocalInitiatedTimeTracker {
        // TODO use List
        private final TreeMultiset<Time> initiatedTimes = TreeMultiset.create();
        private Time lastKnownLowestInitiatedTime = null;
        private Time highestInitiatedTime = null;
        private int uncompletedInitiatedTimes = 0;

        @Override
        public Time addInitiatedTimeAndReturnLastKnownLowestTime(Time time) throws CompletionTimeException {
            if (null != highestInitiatedTime && time.lt(highestInitiatedTime)) {
                String errMsg = String.format("Submitted initiated time is lower than previously submitted initiated time\n"
                                + "  Submitted: %s\n"
                                + "  Previous: %s",
                        time,
                        highestInitiatedTime
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
                    lastKnownLowestInitiatedTime = initiatedTimes.firstEntry().getElement();
                return lastKnownLowestInitiatedTime;
            } else {
                throw new CompletionTimeException("Initiated time of completed event does not map to any uncompleted operation");
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
