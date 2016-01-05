package com.ldbc.driver.runtime.coordination;

import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.TreeMultiset;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.util.Function1;
import com.ldbc.driver.util.Function2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static java.lang.String.format;

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
public class LocalCompletionTimeStateManager implements LocalCompletionTimeReaderWriter
{
    private long localCompletionTimeAsMilli = -1;
    private final LocalInitiatedTimeTracker localInitiatedTimeTracker =
            LocalInitiatedTimeTrackerImpl.createUsingTreeMultiSet();
    private final LocalCompletedTimeTracker localCompletedTimeTracker =
            LocalCompletedTimeTrackerImpl.createUsingTreeMultiSet();
    private long lastKnownLowestInitiatedTimeAsMilli = -1;

    LocalCompletionTimeStateManager()
    {
    }

    @Override
    public long lastKnownLowestInitiatedTimeAsMilli() throws CompletionTimeException
    {
        return lastKnownLowestInitiatedTimeAsMilli;
    }

    @Override
    public long localCompletionTimeAsMilli()
    {
        return localCompletionTimeAsMilli;
    }

    /**
     * Logs the new initiated time and updates completion time accordingly.
     * NOTE, initiated times MUST be applied in ascending order!
     *
     * @param timeAsMilli
     */
    @Override
    public void submitLocalInitiatedTime( long timeAsMilli ) throws CompletionTimeException
    {
        lastKnownLowestInitiatedTimeAsMilli =
                localInitiatedTimeTracker.addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli( timeAsMilli );
        updateCompletionTime();
    }

    /**
     * Logs the new completed time and updates completion time accordingly.
     *
     * @param timeAsMilli
     * @throws com.ldbc.driver.runtime.coordination.CompletionTimeException
     */
    @Override
    public void submitLocalCompletedTime( long timeAsMilli ) throws CompletionTimeException
    {
        lastKnownLowestInitiatedTimeAsMilli =
                localInitiatedTimeTracker.removeTimeAndReturnLastKnownLowestTimeAsMilli( timeAsMilli );
        localCompletedTimeTracker.addCompletedTimeAsMilli( timeAsMilli );
        updateCompletionTime();
    }

    private void updateCompletionTime()
    {
        long highestSafeCompletedTimeAsMilli = localCompletedTimeTracker
                .removeTimesLowerThanAndReturnHighestRemoved( lastKnownLowestInitiatedTimeAsMilli );
        if ( -1 != highestSafeCompletedTimeAsMilli )
        { localCompletionTimeAsMilli = highestSafeCompletedTimeAsMilli; }
    }

    interface LocalCompletedTimeTracker
    {
        void addCompletedTimeAsMilli( long completedTimeAsMilli );

        long removeTimesLowerThanAndReturnHighestRemoved( long timeAsMilli );
    }

    public interface LocalInitiatedTimeTracker
    {
        long addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli( long initiatedTimeAsMilli )
                throws CompletionTimeException;

        long removeTimeAndReturnLastKnownLowestTimeAsMilli( long timeAsMilli ) throws CompletionTimeException;

        long highestInitiatedTimeAsMilli();

        int uncompletedInitiatedTimes();
    }

    static class LocalCompletedTimeTrackerImpl<INITIATED_TIMES_CONTAINER_TYPE extends Collection<Long>>
            implements LocalCompletedTimeTracker
    {
        private final INITIATED_TIMES_CONTAINER_TYPE completedTimesAsMilli;
        private final Function2<INITIATED_TIMES_CONTAINER_TYPE,Long,Long,RuntimeException>
                removeTimesLowerThanAndReturnHighestRemovedFun;

        static LocalCompletedTimeTrackerImpl createUsingTreeMultiSet()
        {
            Function2<TreeMultiset<Long>,Long,Long,RuntimeException> removeTimesLowerThanAndReturnHighestRemovedFun =
                    new Function2<TreeMultiset<Long>,Long,Long,RuntimeException>()
                    {
                        @Override
                        public Long apply( TreeMultiset<Long> completedTimesAsMilli, Long timeAsMilli )
                        {
                            long highestRemovedAsMilli = -1;
                            for ( long completedTimeAsMilli : completedTimesAsMilli )
                            {
                                if ( completedTimeAsMilli < timeAsMilli )
                                {
                                    completedTimesAsMilli.remove( completedTimeAsMilli );
                                    highestRemovedAsMilli = completedTimeAsMilli;
                                }
                                else
                                {
                                    break;
                                }
                            }
                            return highestRemovedAsMilli;
                        }
                    };
            return new LocalCompletedTimeTrackerImpl(
                    TreeMultiset.<Long>create(),
                    removeTimesLowerThanAndReturnHighestRemovedFun
            );
        }

        static LocalCompletedTimeTrackerImpl createUsingArrayList()
        {
            Function2<ArrayList<Long>,Long,Long,RuntimeException> removeTimesLowerThanAndReturnHighestRemovedFun =
                    new Function2<ArrayList<Long>,Long,Long,RuntimeException>()
                    {
                        @Override
                        public Long apply( ArrayList<Long> completedTimes, Long timeAsMilli )
                        {
                            long highestRemovedAsMilli = -1;
                            Iterator<Long> completedTimesIterator = completedTimes.iterator();
                            while ( completedTimesIterator.hasNext() )
                            {
                                long completedTimeAsMilli = completedTimesIterator.next();
                                if ( completedTimeAsMilli < timeAsMilli )
                                {
                                    completedTimesIterator.remove();
                                    if ( -1 == highestRemovedAsMilli || completedTimeAsMilli > highestRemovedAsMilli )
                                    {
                                        highestRemovedAsMilli = completedTimeAsMilli;
                                    }
                                }
                            }
                            return highestRemovedAsMilli;
                        }
                    };
            return new LocalCompletedTimeTrackerImpl(
                    Lists.<Long>newArrayList(),
                    removeTimesLowerThanAndReturnHighestRemovedFun
            );
        }

        private LocalCompletedTimeTrackerImpl(
                INITIATED_TIMES_CONTAINER_TYPE completedTimesAsMilli,
                Function2<INITIATED_TIMES_CONTAINER_TYPE,Long,Long,RuntimeException>
                        removeTimesLowerThanAndReturnHighestRemovedFun )
        {
            this.completedTimesAsMilli = completedTimesAsMilli;
            this.removeTimesLowerThanAndReturnHighestRemovedFun = removeTimesLowerThanAndReturnHighestRemovedFun;
        }

        @Override
        public void addCompletedTimeAsMilli( long completedTimeAsMilli )
        {
            completedTimesAsMilli.add( completedTimeAsMilli );
        }

        @Override
        public long removeTimesLowerThanAndReturnHighestRemoved( long timeAsMilli )
        {
            return removeTimesLowerThanAndReturnHighestRemovedFun.apply( completedTimesAsMilli, timeAsMilli );
        }

        @Override
        public String toString()
        {
            return "LocalCompletedTimeTrackerImpl{" +
                   "completedTimesAsMilli=" + completedTimesAsMilli.toString() +
                   '}';
        }
    }

    static class LocalInitiatedTimeTrackerImpl<INITIATED_TIMES_CONTAINER_TYPE extends Collection<Long>>
            implements LocalInitiatedTimeTracker
    {
        private final TemporalUtil temporalUtil = new TemporalUtil();
        private final INITIATED_TIMES_CONTAINER_TYPE initiatedTimesAsMilli;
        private final Function1<INITIATED_TIMES_CONTAINER_TYPE,Long,RuntimeException>
                getLastKnownLowestInitiatedTimeFun;
        private long lastKnownLowestInitiatedTimeAsMilli = -1;
        private long highestInitiatedTimeAsMilli = -1;
        private int uncompletedInitiatedTimes = 0;

        static LocalInitiatedTimeTrackerImpl createUsingTreeMultiSet()
        {
            Function1<TreeMultiset<Long>,Long,RuntimeException> getLastKnownLowestInitiatedTimeFun =
                    new Function1<TreeMultiset<Long>,Long,RuntimeException>()
                    {
                        @Override
                        public Long apply( TreeMultiset<Long> initiatedTimesAsMilli )
                        {
                            return initiatedTimesAsMilli.firstEntry().getElement();
                        }
                    };
            return new LocalInitiatedTimeTrackerImpl(
                    TreeMultiset.<Long>create(),
                    getLastKnownLowestInitiatedTimeFun
            );
        }

        static LocalInitiatedTimeTrackerImpl createUsingArrayList()
        {
            Function1<List<Long>,Long,RuntimeException> getLastKnownLowestInitiatedTimeFun =
                    new Function1<List<Long>,Long,RuntimeException>()
                    {
                        @Override
                        public Long apply( List<Long> initiatedTimesAsMilli )
                        {
                            return initiatedTimesAsMilli.get( 0 );
                        }
                    };
            return new LocalInitiatedTimeTrackerImpl(
                    Lists.<Long>newArrayList(),
                    getLastKnownLowestInitiatedTimeFun
            );
        }

        static LocalInitiatedTimeTrackerImpl createUsingMinMaxPriorityQueue()
        {
            // TODO this seems to have a bug for larger values, don't use it
            System.err.println( format(
                    "LocalInitiatedTimeTrackerImpl.createUsingMinMaxPriorityQueue() is buggy. When collection size is" +
                    " very large values seem to get dropped. DO NOT USE!" ) );
            Function1<MinMaxPriorityQueue<Long>,Long,RuntimeException> getLastKnownLowestInitiatedTimeFun =
                    new Function1<MinMaxPriorityQueue<Long>,Long,RuntimeException>()
                    {
                        @Override
                        public Long apply( MinMaxPriorityQueue<Long> initiatedTimesAsMilli )
                        {
                            return initiatedTimesAsMilli.peekFirst();
                        }
                    };
            return new LocalInitiatedTimeTrackerImpl(
                    MinMaxPriorityQueue.create(),
                    getLastKnownLowestInitiatedTimeFun
            );
        }

        private LocalInitiatedTimeTrackerImpl(
                INITIATED_TIMES_CONTAINER_TYPE initiatedTimesAsMilli,
                Function1<INITIATED_TIMES_CONTAINER_TYPE,Long,RuntimeException> getLastKnownLowestInitiatedTimeFun )
        {
            this.initiatedTimesAsMilli = initiatedTimesAsMilli;
            this.getLastKnownLowestInitiatedTimeFun = getLastKnownLowestInitiatedTimeFun;
        }

        @Override
        public long addInitiatedTimeAndReturnLastKnownLowestTimeAsMilli( long initiatedTimeAsMilli )
                throws CompletionTimeException
        {
            if ( -1 != highestInitiatedTimeAsMilli && initiatedTimeAsMilli < highestInitiatedTimeAsMilli )
            {
                String errMsg = format( "Submitted initiated time is lower than previously submitted initiated time\n"
                                        + "  Submitted: %s (%s ms)\n"
                                        + "  Previous: %s (%s ms)",
                        temporalUtil.milliTimeToDateTimeString( initiatedTimeAsMilli ), initiatedTimeAsMilli,
                        temporalUtil.milliTimeToDateTimeString( highestInitiatedTimeAsMilli ),
                        highestInitiatedTimeAsMilli
                );
                throw new CompletionTimeException( errMsg );
            }
            highestInitiatedTimeAsMilli = initiatedTimeAsMilli;

            if ( 0 == uncompletedInitiatedTimes )
            { lastKnownLowestInitiatedTimeAsMilli = initiatedTimeAsMilli; }
            initiatedTimesAsMilli.add( initiatedTimeAsMilli );
            uncompletedInitiatedTimes++;
            return lastKnownLowestInitiatedTimeAsMilli;
        }

        @Override
        public long removeTimeAndReturnLastKnownLowestTimeAsMilli( long timeAsMilli ) throws CompletionTimeException
        {
            if ( initiatedTimesAsMilli.remove( timeAsMilli ) )
            {
                uncompletedInitiatedTimes--;
                if ( 0 == uncompletedInitiatedTimes )
                { lastKnownLowestInitiatedTimeAsMilli = highestInitiatedTimeAsMilli; }
                else
                {
                    lastKnownLowestInitiatedTimeAsMilli =
                            getLastKnownLowestInitiatedTimeFun.apply( initiatedTimesAsMilli );
                }
                return lastKnownLowestInitiatedTimeAsMilli;
            }
            else
            {
                throw new CompletionTimeException( format(
                        "Initiated time [%s] of completed event does not map to any uncompleted operation",
                        timeAsMilli ) );
            }
        }

        @Override
        public long highestInitiatedTimeAsMilli()
        {
            return highestInitiatedTimeAsMilli;
        }

        @Override
        public int uncompletedInitiatedTimes()
        {
            return uncompletedInitiatedTimes;
        }

        @Override
        public String toString()
        {
            return "LocalInitiatedTimeTrackerImpl{" +
                   "initiatedTimesAsMilli=" + initiatedTimesAsMilli.toString() +
                   ", lastKnownLowestInitiatedTimeAsMilli=" + lastKnownLowestInitiatedTimeAsMilli +
                   ", lastKnownLowestInitiatedTimeAsMilli=" +
                   temporalUtil.milliTimeToDateTimeString( lastKnownLowestInitiatedTimeAsMilli ) +
                   ", highestInitiatedTimeAsMilli=" + highestInitiatedTimeAsMilli +
                   ", highestInitiatedTimeAsMilli=" +
                   temporalUtil.milliTimeToDateTimeString( highestInitiatedTimeAsMilli ) +
                   ", uncompletedInitiatedTimes=" + uncompletedInitiatedTimes +
                   '}';
        }
    }
}
