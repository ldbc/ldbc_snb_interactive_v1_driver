package com.ldbc.driver.runtime.metrics;

abstract class ThreadedQueuedMetricsEvent {

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

    static public class SubmitOperationResult extends ThreadedQueuedMetricsEvent {
        private final int operationType;
        private final long scheduledStartTimeAsMilli;
        private final long actualStartTimeAsMilli;
        private final long runDurationAsNano;
        private final int resultCode;
        private final long originalStartTime;

        public SubmitOperationResult(
                int operationType,
                long scheduledStartTimeAsMilli,
                long actualStartTimeAsMilli,
                long runDurationAsNano,
                int resultCode,
                long originalStartTime) {
            this.operationType = operationType;
            this.scheduledStartTimeAsMilli = scheduledStartTimeAsMilli;
            this.actualStartTimeAsMilli = actualStartTimeAsMilli;
            this.runDurationAsNano = runDurationAsNano;
            this.resultCode = resultCode;
            this.originalStartTime = originalStartTime;
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
        
        public long originalStartTime() {
        	return originalStartTime;
        }

        @Override
        public MetricsEventType type() {
            return MetricsEventType.SUBMIT_RESULT;
        }
    }

    static public class Status extends ThreadedQueuedMetricsEvent {
        private final ThreadedQueuedMetricsService.MetricsStatusFuture statusFuture;

        public Status(ThreadedQueuedMetricsService.MetricsStatusFuture statusFuture) {
            this.statusFuture = statusFuture;
        }

        public ThreadedQueuedMetricsService.MetricsStatusFuture statusFuture() {
            return statusFuture;
        }

        @Override
        public MetricsEventType type() {
            return MetricsEventType.WORKLOAD_STATUS;
        }
    }

    static public class GetWorkloadResults extends ThreadedQueuedMetricsEvent {
        private final ThreadedQueuedMetricsService.MetricsWorkloadResultFuture workloadResultFuture;

        public GetWorkloadResults(ThreadedQueuedMetricsService.MetricsWorkloadResultFuture workloadResultFuture) {
            this.workloadResultFuture = workloadResultFuture;
        }

        public ThreadedQueuedMetricsService.MetricsWorkloadResultFuture workloadResultFuture() {
            return workloadResultFuture;
        }

        @Override
        public MetricsEventType type() {
            return MetricsEventType.WORKLOAD_RESULT;
        }
    }

    static public class Shutdown extends ThreadedQueuedMetricsEvent {
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