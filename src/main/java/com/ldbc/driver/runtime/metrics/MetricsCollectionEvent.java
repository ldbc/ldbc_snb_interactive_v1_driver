package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResultReport;

abstract class MetricsCollectionEvent {

    public enum MetricsEventType {
        // Submit operation result for its metrics to be collected
        SUBMIT_RESULT,
        // TODO make this a class that is returned, rather than a string
        // Request metrics summary
        WORKLOAD_STATUS,
        // Request complete workload results
        WORKLOAD_RESULT,
        // Terminate when all results metrics have been collected
        TERMINATE
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
    }

    static class TerminationEvent extends MetricsCollectionEvent {
        private final long expectedEventCount;

        private TerminationEvent(long expectedEventCount) {
            this.expectedEventCount = expectedEventCount;
        }

        @Override
        MetricsEventType type() {
            return MetricsEventType.TERMINATE;
        }

        long expectedEventCount() {
            return expectedEventCount;
        }
    }
}