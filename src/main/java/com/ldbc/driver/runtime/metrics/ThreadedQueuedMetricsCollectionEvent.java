package com.ldbc.driver.runtime.metrics;

abstract class ThreadedQueuedMetricsCollectionEvent {

    public static enum MetricsEventType {
        // Submit operation result for its metrics to be collected
        SUBMIT_RESULT,
        // Request metrics summary
        WORKLOAD_STATUS,
        // Request complete workload results
        WORKLOAD_RESULT,
        // Terminate when all results metrics have been collected
        SHUTDOWN_SERVICE
    }

    public abstract MetricsEventType type();

    static public class SubmitOperationResult extends ThreadedQueuedMetricsCollectionEvent {
        private final int operationType;
        private final long scheduledStartTimeAsMilli;
        private final long actualStartTimeAsMilli;
        private final long runDurationAsNano;
        private final int resultCode;

        public SubmitOperationResult(
                int operationType,
                long scheduledStartTimeAsMilli,
                long actualStartTimeAsMilli,
                long runDurationAsNano,
                int resultCode) {
            this.operationType = operationType;
            this.scheduledStartTimeAsMilli = scheduledStartTimeAsMilli;
            this.actualStartTimeAsMilli = actualStartTimeAsMilli;
            this.runDurationAsNano = runDurationAsNano;
            this.resultCode = resultCode;
        }

        public int operationType() {
            return operationType;
        }

        public long scheduledStartTimeAsMilli() {
            return scheduledStartTimeAsMilli;
        }

        public long actualStartTimeAsMilli() {
            return actualStartTimeAsMilli;
        }

        public long runDurationAsNano() {
            return runDurationAsNano;
        }

        public int resultCode() {
            return resultCode;
        }

        @Override
        public MetricsEventType type() {
            return MetricsEventType.SUBMIT_RESULT;
        }
    }

    static public class Status extends ThreadedQueuedMetricsCollectionEvent {
        private final ThreadedQueuedConcurrentMetricsService.MetricsStatusFuture statusFuture;

        public Status(ThreadedQueuedConcurrentMetricsService.MetricsStatusFuture statusFuture) {
            this.statusFuture = statusFuture;
        }

        public ThreadedQueuedConcurrentMetricsService.MetricsStatusFuture statusFuture() {
            return statusFuture;
        }

        @Override
        public MetricsEventType type() {
            return MetricsEventType.WORKLOAD_STATUS;
        }
    }

    static public class GetWorkloadResults extends ThreadedQueuedMetricsCollectionEvent {
        private final ThreadedQueuedConcurrentMetricsService.MetricsWorkloadResultFuture workloadResultFuture;

        public GetWorkloadResults(ThreadedQueuedConcurrentMetricsService.MetricsWorkloadResultFuture workloadResultFuture) {
            this.workloadResultFuture = workloadResultFuture;
        }

        public ThreadedQueuedConcurrentMetricsService.MetricsWorkloadResultFuture workloadResultFuture() {
            return workloadResultFuture;
        }

        @Override
        public MetricsEventType type() {
            return MetricsEventType.WORKLOAD_RESULT;
        }
    }

    static public class Shutdown extends ThreadedQueuedMetricsCollectionEvent {
        private final long initiatedEvents;

        public Shutdown(long initiatedEvents) {
            this.initiatedEvents = initiatedEvents;
        }

        public long initiatedEvents() {
            return initiatedEvents;
        }

        @Override
        public MetricsEventType type() {
            return MetricsEventType.SHUTDOWN_SERVICE;
        }
    }
}