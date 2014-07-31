package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResultReport;

abstract class MetricsCollectionEvent {

    public enum MetricsEventType {
        // Submit operation result for its metrics to be collected
        SUBMIT_RESULT,
        // Request metrics summary
        WORKLOAD_STATUS,
        // Request complete workload results
        WORKLOAD_RESULT,
        // Terminate when all results metrics have been collected
        TERMINATE_SERVICE
    }

    public static SubmitResultEvent submitResult(OperationResultReport result) {
        return new SubmitResultEvent(result);
    }

    public static StatusEvent status(ThreadedQueuedConcurrentMetricsService.MetricsStatusFuture future) {
        return new StatusEvent(future);
    }

    public static WorkloadResultEvent workloadResult(ThreadedQueuedConcurrentMetricsService.MetricsWorkloadResultFuture future) {
        return new WorkloadResultEvent(future);
    }

    public static TerminationEvent terminate(long expectedEventCount) {
        return new TerminationEvent(expectedEventCount);
    }

    abstract MetricsEventType type();

    static class SubmitResultEvent extends MetricsCollectionEvent {
        private final OperationResultReport result;

        private SubmitResultEvent(OperationResultReport result) {
            this.result = result;
        }

        @Override
        MetricsEventType type() {
            return MetricsEventType.SUBMIT_RESULT;
        }

        OperationResultReport result() {
            return result;
        }

        @Override
        public String toString() {
            return "SubmitResultEvent{" +
                    "result=" + result +
                    '}';
        }
    }

    static class StatusEvent extends MetricsCollectionEvent {
        private final ThreadedQueuedConcurrentMetricsService.MetricsStatusFuture future;

        private StatusEvent(ThreadedQueuedConcurrentMetricsService.MetricsStatusFuture future) {
            this.future = future;
        }

        @Override
        MetricsEventType type() {
            return MetricsEventType.WORKLOAD_STATUS;
        }

        ThreadedQueuedConcurrentMetricsService.MetricsStatusFuture future() {
            return future;
        }

        @Override
        public String toString() {
            return "StatusEvent{" +
                    "future=" + future +
                    '}';
        }
    }

    static class WorkloadResultEvent extends MetricsCollectionEvent {
        private final ThreadedQueuedConcurrentMetricsService.MetricsWorkloadResultFuture future;

        private WorkloadResultEvent(ThreadedQueuedConcurrentMetricsService.MetricsWorkloadResultFuture future) {
            this.future = future;
        }

        @Override
        MetricsEventType type() {
            return MetricsEventType.WORKLOAD_RESULT;
        }

        ThreadedQueuedConcurrentMetricsService.MetricsWorkloadResultFuture future() {
            return future;
        }

        @Override
        public String toString() {
            return "WorkloadResultEvent{" +
                    "future=" + future +
                    '}';
        }
    }

    static class TerminationEvent extends MetricsCollectionEvent {
        private final long expectedEventCount;

        private TerminationEvent(long expectedEventCount) {
            this.expectedEventCount = expectedEventCount;
        }

        @Override
        MetricsEventType type() {
            return MetricsEventType.TERMINATE_SERVICE;
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
}