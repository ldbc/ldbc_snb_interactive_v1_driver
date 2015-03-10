package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.temporal.TemporalUtil;

abstract class CompletionTimeEvent {

    public enum CompletionTimeEventType {
        // Operation started executing
        WRITE_LOCAL_INITIATED_TIME,
        // Operation completed scheduling
        WRITE_LOCAL_COMPLETED_TIME,
        // GCT came in from other process
        WRITE_EXTERNAL_COMPLETION_TIME,
        // Instruction to terminate when all results have arrived
        TERMINATE_SERVICE,
        // Request for future to GCT value (value will only be available once event is processed)
        READ_GCT_FUTURE,
        // Request a new local completion time writer
        NEW_LOCAL_COMPLETION_TIME_WRITER
    }

    public static LocalInitiatedTimeEvent writeLocalInitiatedTime(int localCompletionTimeWriterId, long timeAsMilli) {
        return new LocalInitiatedTimeEvent(localCompletionTimeWriterId, timeAsMilli);
    }

    public static LocalCompletedTimeEvent writeLocalCompletedTime(int localCompletionTimeWriterId, long timeAsMilli) {
        return new LocalCompletedTimeEvent(localCompletionTimeWriterId, timeAsMilli);
    }

    public static ExternalCompletionTimeEvent writeExternalCompletionTime(String peerId, long timeAsMilli) {
        return new ExternalCompletionTimeEvent(peerId, timeAsMilli);
    }

    public static TerminationServiceEvent terminateService(long expectedEventCount) {
        return new TerminationServiceEvent(expectedEventCount);
    }

    public static GlobalCompletionTimeFutureEvent globalCompletionTimeFuture(ThreadedQueuedCompletionTimeService.GlobalCompletionTimeFuture future) {
        return new GlobalCompletionTimeFutureEvent(future);
    }

    public static NewLocalCompletionTimeWriterEvent newLocalCompletionTimeWriter(ThreadedQueuedCompletionTimeService.LocalCompletionTimeWriterFuture future) {
        return new NewLocalCompletionTimeWriterEvent(future);
    }

    abstract CompletionTimeEventType type();

    static class LocalInitiatedTimeEvent extends CompletionTimeEvent {
        private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
        private final int localCompletionTimeWriterId;
        private final long timeAsMilli;

        private LocalInitiatedTimeEvent(int localCompletionTimeWriterId, long timeAsMilli) {
            this.localCompletionTimeWriterId = localCompletionTimeWriterId;
            this.timeAsMilli = timeAsMilli;
        }

        @Override
        CompletionTimeEventType type() {
            return CompletionTimeEventType.WRITE_LOCAL_INITIATED_TIME;
        }

        int localCompletionTimeWriterId() {
            return localCompletionTimeWriterId;
        }

        long timeAsMilli() {
            return timeAsMilli;
        }

        @Override
        public String toString() {
            return "InitiatedEvent{" +
                    "localCompletionTimeWriterId=" + localCompletionTimeWriterId +
                    ", timeAsMilli=" + timeAsMilli +
                    ", time=" + TEMPORAL_UTIL.milliTimeToTimeString(timeAsMilli) +
                    '}';
        }
    }

    static class LocalCompletedTimeEvent extends CompletionTimeEvent {
        private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
        private final int localCompletionTimeWriterId;
        private final long timeAsMilli;

        private LocalCompletedTimeEvent(int localCompletionTimeWriterId, long timeAsMilli) {
            this.localCompletionTimeWriterId = localCompletionTimeWriterId;
            this.timeAsMilli = timeAsMilli;
        }

        @Override
        CompletionTimeEventType type() {
            return CompletionTimeEventType.WRITE_LOCAL_COMPLETED_TIME;
        }

        int localCompletionTimeWriterId() {
            return localCompletionTimeWriterId;
        }

        long timeAsMilli() {
            return timeAsMilli;
        }

        @Override
        public String toString() {
            return "InitiatedEvent{" +
                    "localCompletionTimeWriterId=" + localCompletionTimeWriterId +
                    ", timeAsMilli=" + timeAsMilli +
                    ", time=" + TEMPORAL_UTIL.milliTimeToTimeString(timeAsMilli) +
                    '}';
        }
    }

    static class ExternalCompletionTimeEvent extends CompletionTimeEvent {
        private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
        private final long timeAsMilli;
        private final String peerId;

        private ExternalCompletionTimeEvent(String peerId, long timeAsMilli) {
            this.timeAsMilli = timeAsMilli;
            this.peerId = peerId;
        }

        @Override
        CompletionTimeEventType type() {
            return CompletionTimeEventType.WRITE_EXTERNAL_COMPLETION_TIME;
        }

        long timeAsMilli() {
            return timeAsMilli;
        }

        String peerId() {
            return peerId;
        }

        @Override
        public String toString() {
            return "ExternalCompletionTimeEvent{" +
                    "timeAsMilli=" + timeAsMilli +
                    ", time=" + TEMPORAL_UTIL.milliTimeToTimeString(timeAsMilli) +
                    ", peerId='" + peerId + '\'' +
                    '}';
        }
    }

    static class TerminationServiceEvent extends CompletionTimeEvent {
        private final long expectedEventCount;

        private TerminationServiceEvent(long expectedEventCount) {
            this.expectedEventCount = expectedEventCount;
        }

        @Override
        CompletionTimeEventType type() {
            return CompletionTimeEventType.TERMINATE_SERVICE;
        }

        long expectedEventCount() {
            return expectedEventCount;
        }

        @Override
        public String toString() {
            return "TerminationServiceEvent{" +
                    "expectedEventCount=" + expectedEventCount +
                    '}';
        }
    }

    static class GlobalCompletionTimeFutureEvent extends CompletionTimeEvent {
        private final ThreadedQueuedCompletionTimeService.GlobalCompletionTimeFuture future;

        private GlobalCompletionTimeFutureEvent(ThreadedQueuedCompletionTimeService.GlobalCompletionTimeFuture future) {
            this.future = future;
        }

        @Override
        CompletionTimeEventType type() {
            return CompletionTimeEventType.READ_GCT_FUTURE;
        }

        ThreadedQueuedCompletionTimeService.GlobalCompletionTimeFuture future() {
            return future;
        }

        @Override
        public String toString() {
            return "FutureEvent{" +
                    "future=" + future +
                    '}';
        }
    }

    static class NewLocalCompletionTimeWriterEvent extends CompletionTimeEvent {
        private final ThreadedQueuedCompletionTimeService.LocalCompletionTimeWriterFuture future;

        private NewLocalCompletionTimeWriterEvent(ThreadedQueuedCompletionTimeService.LocalCompletionTimeWriterFuture future) {
            this.future = future;
        }

        @Override
        CompletionTimeEventType type() {
            return CompletionTimeEventType.NEW_LOCAL_COMPLETION_TIME_WRITER;
        }

        ThreadedQueuedCompletionTimeService.LocalCompletionTimeWriterFuture future() {
            return future;
        }

        @Override
        public String toString() {
            return "NewLocalCompletionTimeWriterEvent{" +
                    "future=" + future +
                    '}';
        }
    }
}