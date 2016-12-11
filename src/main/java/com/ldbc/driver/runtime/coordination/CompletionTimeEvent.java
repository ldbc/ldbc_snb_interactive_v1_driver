package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.coordination.ThreadedQueuedCompletionTimeService.CompletionTimeFuture;
import com.ldbc.driver.runtime.coordination.ThreadedQueuedCompletionTimeService.CompletionTimeWriterFuture;
import com.ldbc.driver.temporal.TemporalUtil;

abstract class CompletionTimeEvent
{
    public enum CompletionTimeEventType
    {
        // Operation started executing
        WRITE_INITIATED_TIME,
        // Operation completed scheduling
        WRITE_COMPLETED_TIME,
        // Instruction to terminate when all results have arrived
        TERMINATE_SERVICE,
        // Request for future to CT value (value will only be available once event is processed)
        READ_CT_FUTURE,
        // Request a new completion time writer
        NEW_COMPLETION_TIME_WRITER
    }

    static InitiatedTimeEvent writeInitiatedTime( int completionTimeWriterId, long timeAsMilli )
    {
        return new InitiatedTimeEvent( completionTimeWriterId, timeAsMilli );
    }

    static CompletedTimeEvent writeCompletedTime( int completionTimeWriterId, long timeAsMilli )
    {
        return new CompletedTimeEvent( completionTimeWriterId, timeAsMilli );
    }

    static TerminationServiceEvent terminateService( long expectedEventCount )
    {
        return new TerminationServiceEvent( expectedEventCount );
    }

    static CompletionTimeFutureEvent completionTimeFuture( CompletionTimeFuture future )
    {
        return new CompletionTimeFutureEvent( future );
    }

    static NewCompletionTimeWriterEvent newCompletionTimeWriter( CompletionTimeWriterFuture future )
    {
        return new NewCompletionTimeWriterEvent( future );
    }

    abstract CompletionTimeEventType type();

    static class InitiatedTimeEvent extends CompletionTimeEvent
    {
        private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
        private final int completionTimeWriterId;
        private final long timeAsMilli;

        private InitiatedTimeEvent( int completionTimeWriterId, long timeAsMilli )
        {
            this.completionTimeWriterId = completionTimeWriterId;
            this.timeAsMilli = timeAsMilli;
        }

        @Override
        CompletionTimeEventType type()
        {
            return CompletionTimeEventType.WRITE_INITIATED_TIME;
        }

        int completionTimeWriterId()
        {
            return completionTimeWriterId;
        }

        long timeAsMilli()
        {
            return timeAsMilli;
        }

        @Override
        public String toString()
        {
            return "InitiatedEvent{" +
                   "completionTimeWriterId=" + completionTimeWriterId +
                   ", timeAsMilli=" + timeAsMilli +
                   ", time=" + TEMPORAL_UTIL.milliTimeToTimeString( timeAsMilli ) +
                   '}';
        }
    }

    static class CompletedTimeEvent extends CompletionTimeEvent
    {
        private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
        private final int completionTimeWriterId;
        private final long timeAsMilli;

        private CompletedTimeEvent( int completionTimeWriterId, long timeAsMilli )
        {
            this.completionTimeWriterId = completionTimeWriterId;
            this.timeAsMilli = timeAsMilli;
        }

        @Override
        CompletionTimeEventType type()
        {
            return CompletionTimeEventType.WRITE_COMPLETED_TIME;
        }

        int completionTimeWriterId()
        {
            return completionTimeWriterId;
        }

        long timeAsMilli()
        {
            return timeAsMilli;
        }

        @Override
        public String toString()
        {
            return "InitiatedEvent{" +
                   "completionTimeWriterId=" + completionTimeWriterId +
                   ", timeAsMilli=" + timeAsMilli +
                   ", time=" + TEMPORAL_UTIL.milliTimeToTimeString( timeAsMilli ) +
                   '}';
        }
    }

    static class TerminationServiceEvent extends CompletionTimeEvent
    {
        private final long expectedEventCount;

        private TerminationServiceEvent( long expectedEventCount )
        {
            this.expectedEventCount = expectedEventCount;
        }

        @Override
        CompletionTimeEventType type()
        {
            return CompletionTimeEventType.TERMINATE_SERVICE;
        }

        long expectedEventCount()
        {
            return expectedEventCount;
        }

        @Override
        public String toString()
        {
            return "TerminationServiceEvent{" +
                   "expectedEventCount=" + expectedEventCount +
                   '}';
        }
    }

    static class CompletionTimeFutureEvent extends CompletionTimeEvent
    {
        private final CompletionTimeFuture future;

        private CompletionTimeFutureEvent( CompletionTimeFuture future )
        {
            this.future = future;
        }

        @Override
        CompletionTimeEventType type()
        {
            return CompletionTimeEventType.READ_CT_FUTURE;
        }

        CompletionTimeFuture future()
        {
            return future;
        }

        @Override
        public String toString()
        {
            return "FutureEvent{" +
                   "future=" + future +
                   '}';
        }
    }

    static class NewCompletionTimeWriterEvent extends CompletionTimeEvent
    {
        private final CompletionTimeWriterFuture future;

        private NewCompletionTimeWriterEvent( CompletionTimeWriterFuture future )
        {
            this.future = future;
        }

        @Override
        CompletionTimeEventType type()
        {
            return CompletionTimeEventType.NEW_COMPLETION_TIME_WRITER;
        }

        CompletionTimeWriterFuture future()
        {
            return future;
        }

        @Override
        public String toString()
        {
            return "NewCompletionTimeWriterEvent{" +
                   "future=" + future +
                   '}';
        }
    }
}
