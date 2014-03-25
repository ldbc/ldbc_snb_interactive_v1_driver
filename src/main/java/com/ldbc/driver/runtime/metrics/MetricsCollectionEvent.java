package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResult;

abstract class MetricsCollectionEvent {

    public enum MetricsEventType {
        // Collects metrics for an operation result
        RESULT,
        // Export collected metrics
        EXPORT,
        // Request metrics summary
        STATUS,
        // Terminate when all results metrics have been collected
        TERMINATE
    }

    public static ResultEvent result(OperationResult result) {
        return new ResultEvent(result);
    }

    public static ExportEvent export(ThreadedQueuedConcurrentMetricsService.MetricsExportFuture future) {
        return new ExportEvent(future);
    }

    public static StatusEvent status(ThreadedQueuedConcurrentMetricsService.MetricsStatusFuture future) {
        return new StatusEvent(future);
    }

    public static TerminationEvent terminate(long expectedEventCount) {
        return new TerminationEvent(expectedEventCount);
    }

    abstract MetricsEventType type();

    static class ResultEvent extends MetricsCollectionEvent {
        private final OperationResult result;

        private ResultEvent(OperationResult result) {
            this.result = result;
        }

        @Override
        MetricsEventType type() {
            return MetricsEventType.RESULT;
        }

        OperationResult result() {
            return result;
        }
    }

    static class ExportEvent extends MetricsCollectionEvent {
        private final ThreadedQueuedConcurrentMetricsService.MetricsExportFuture future;

        private ExportEvent(ThreadedQueuedConcurrentMetricsService.MetricsExportFuture future) {
            this.future = future;
        }

        @Override
        MetricsEventType type() {
            return MetricsEventType.EXPORT;
        }

        ThreadedQueuedConcurrentMetricsService.MetricsExportFuture future() {
            return future;
        }
    }

    static class StatusEvent extends MetricsCollectionEvent {
        private final ThreadedQueuedConcurrentMetricsService.MetricsStatusFuture future;

        private StatusEvent(ThreadedQueuedConcurrentMetricsService.MetricsStatusFuture future) {
            this.future = future;
        }

        @Override
        MetricsEventType type() {
            return MetricsEventType.STATUS;
        }

        ThreadedQueuedConcurrentMetricsService.MetricsStatusFuture future() {
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