package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.temporal.Time;

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

    public static LocalInitiatedTimeEvent writeLocalInitiatedTime(int localCompletionTimeWriterId, Time time) {
        return new LocalInitiatedTimeEvent(localCompletionTimeWriterId, time);
    }

    public static LocalCompletedTimeEvent writeLocalCompletedTime(int localCompletionTimeWriterId, Time time) {
        return new LocalCompletedTimeEvent(localCompletionTimeWriterId, time);
    }

    public static ExternalCompletionTimeEvent writeExternalCompletionTime(String peerId, Time time) {
        return new ExternalCompletionTimeEvent(peerId, time);
    }

    public static TerminationServiceEvent terminateService(long expectedEventCount) {
        return new TerminationServiceEvent(expectedEventCount);
    }

    public static GlobalCompletionTimeFutureEvent globalCompletionTimeFuture(ThreadedQueuedConcurrentCompletionTimeService.GlobalCompletionTimeFuture future) {
        return new GlobalCompletionTimeFutureEvent(future);
    }

    public static NewLocalCompletionTimeWriterEvent newLocalCompletionTimeWriter(ThreadedQueuedConcurrentCompletionTimeService.LocalCompletionTimeWriterFuture future) {
        return new NewLocalCompletionTimeWriterEvent(future);
    }

    abstract CompletionTimeEventType type();

    static class LocalInitiatedTimeEvent extends CompletionTimeEvent {
        private final int localCompletionTimeWriterId;
        private final Time time;

        private LocalInitiatedTimeEvent(int localCompletionTimeWriterId, Time time) {
            this.localCompletionTimeWriterId = localCompletionTimeWriterId;
            this.time = time;
        }

        @Override
        CompletionTimeEventType type() {
            return CompletionTimeEventType.WRITE_LOCAL_INITIATED_TIME;
        }

        int localCompletionTimeWriterId() {
            return localCompletionTimeWriterId;
        }

        Time time() {
            return time;
        }

        @Override
        public String toString() {
            return "InitiatedEvent{" +
                    "localCompletionTimeWriterId=" + localCompletionTimeWriterId +
                    ", time=" + time +
                    '}';
        }
    }

    static class LocalCompletedTimeEvent extends CompletionTimeEvent {
        private final int localCompletionTimeWriterId;
        private final Time time;

        private LocalCompletedTimeEvent(int localCompletionTimeWriterId, Time time) {
            this.localCompletionTimeWriterId = localCompletionTimeWriterId;
            this.time = time;
        }

        @Override
        CompletionTimeEventType type() {
            return CompletionTimeEventType.WRITE_LOCAL_COMPLETED_TIME;
        }

        int localCompletionTimeWriterId() {
            return localCompletionTimeWriterId;
        }

        Time time() {
            return time;
        }

        @Override
        public String toString() {
            return "CompletedEvent{" +
                    "localCompletionTimeWriterId=" + localCompletionTimeWriterId +
                    ", time=" + time +
                    '}';
        }
    }

    static class ExternalCompletionTimeEvent extends CompletionTimeEvent {
        private final Time time;
        private final String peerId;

        private ExternalCompletionTimeEvent(String peerId, Time time) {
            this.time = time;
            this.peerId = peerId;
        }

        @Override
        CompletionTimeEventType type() {
            return CompletionTimeEventType.WRITE_EXTERNAL_COMPLETION_TIME;
        }

        Time time() {
            return time;
        }

        String peerId() {
            return peerId;
        }

        @Override
        public String toString() {
            return "ExternalEvent{" +
                    "time=" + time +
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
            return "TerminationEvent{" +
                    "expectedEventCount=" + expectedEventCount +
                    '}';
        }
    }

    static class GlobalCompletionTimeFutureEvent extends CompletionTimeEvent {
        private final ThreadedQueuedConcurrentCompletionTimeService.GlobalCompletionTimeFuture future;

        private GlobalCompletionTimeFutureEvent(ThreadedQueuedConcurrentCompletionTimeService.GlobalCompletionTimeFuture future) {
            this.future = future;
        }

        @Override
        CompletionTimeEventType type() {
            return CompletionTimeEventType.READ_GCT_FUTURE;
        }

        ThreadedQueuedConcurrentCompletionTimeService.GlobalCompletionTimeFuture future() {
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
        private final ThreadedQueuedConcurrentCompletionTimeService.LocalCompletionTimeWriterFuture future;

        private NewLocalCompletionTimeWriterEvent(ThreadedQueuedConcurrentCompletionTimeService.LocalCompletionTimeWriterFuture future) {
            this.future = future;
        }

        @Override
        CompletionTimeEventType type() {
            return CompletionTimeEventType.NEW_LOCAL_COMPLETION_TIME_WRITER;
        }

        ThreadedQueuedConcurrentCompletionTimeService.LocalCompletionTimeWriterFuture future() {
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