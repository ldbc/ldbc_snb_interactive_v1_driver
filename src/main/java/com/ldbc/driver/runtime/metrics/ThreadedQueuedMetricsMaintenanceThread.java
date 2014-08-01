package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;

public class ThreadedQueuedMetricsMaintenanceThread extends Thread {
    private final MetricsManager metricsManager;
    private final ConcurrentErrorReporter errorReporter;
    private final QueueEventFetcher queueEventFetcher;
    private Long processedEventCount = 0l;
    private Long expectedEventCount = null;

    public ThreadedQueuedMetricsMaintenanceThread(ConcurrentErrorReporter errorReporter,
                                                  Queue<MetricsCollectionEvent> metricsEventsQueue,
                                                  MetricsManager metricsManager) {
        this(
                errorReporter,
                (BlockingQueue.class.isAssignableFrom(metricsEventsQueue.getClass()))
                        ? new BlockingQueueEventFetcher((BlockingQueue) metricsEventsQueue)
                        : new NonBlockingQueueEventFetcher(metricsEventsQueue),
                metricsManager);
    }

    private ThreadedQueuedMetricsMaintenanceThread(ConcurrentErrorReporter errorReporter,
                                                   QueueEventFetcher queueEventFetcher,
                                                   MetricsManager metricsManager) {
        super(ThreadedQueuedMetricsMaintenanceThread.class.getSimpleName() + "-" + System.currentTimeMillis());
        this.errorReporter = errorReporter;
        this.metricsManager = metricsManager;
        this.queueEventFetcher = queueEventFetcher;
    }

    @Override
    public void run() {
        while (null == expectedEventCount || processedEventCount < expectedEventCount) {
            try {
                MetricsCollectionEvent event = null;
                while (event == null) {
                    event = queueEventFetcher.fetchNextEvent();
                }
                switch (event.type()) {
                    case SUBMIT_RESULT:
                        OperationResultReport result = ((MetricsCollectionEvent.SubmitResultEvent) event).result();
                        try {
                            collectResultMetrics(result);
                        } catch (MetricsCollectionException e) {
                            errorReporter.reportError(
                                    this,
                                    String.format("Encountered error while collecting metrics for result: %s\n%s",
                                            result.toString(),
                                            ConcurrentErrorReporter.stackTraceToString(e)));
                        }
                        processedEventCount++;
                        break;
                    case WORKLOAD_STATUS:
                        ThreadedQueuedConcurrentMetricsService.MetricsStatusFuture statusFuture = ((MetricsCollectionEvent.StatusEvent) event).future();
                        statusFuture.set(metricsManager.status());
                        break;
                    case WORKLOAD_RESULT:
                        ThreadedQueuedConcurrentMetricsService.MetricsWorkloadResultFuture workloadResultFuture = ((MetricsCollectionEvent.WorkloadResultEvent) event).future();
                        workloadResultFuture.set(metricsManager.snapshot());
                        break;
                    case TERMINATE_SERVICE:
                        if (expectedEventCount == null) {
                            expectedEventCount = ((MetricsCollectionEvent.TerminationEvent) event).expectedEventCount();
                        } else {
                            // this is not the first termination event that the thread has received
                            errorReporter.reportError(
                                    this,
                                    String.format("Encountered multiple %s events. First expectedEventCount[%s]. Second expectedEventCount[%s]",
                                            MetricsCollectionEvent.MetricsEventType.TERMINATE_SERVICE.name(),
                                            expectedEventCount,
                                            ((MetricsCollectionEvent.TerminationEvent) event).expectedEventCount()));
                        }
                        break;
                    default:
                        errorReporter.reportError(
                                this,
                                String.format("Encountered unexpected event type: %s", event.type().name()));
                        return;
                }
            } catch (Exception e) {
                errorReporter.reportError(
                        this,
                        String.format("Encountered unexpected exception\n%s", ConcurrentErrorReporter.stackTraceToString(e)));
                return;
            }
        }
    }

    private void collectResultMetrics(OperationResultReport operationResultReport) throws MetricsCollectionException {
        try {
            metricsManager.measure(operationResultReport);
        } catch (Exception e) {
            String errMsg = String.format("Error encountered while logging result:\n\t%s", operationResultReport);
            throw new MetricsCollectionException(errMsg, e);
        }
    }

    private static class NonBlockingQueueEventFetcher implements QueueEventFetcher {
        private final Queue<MetricsCollectionEvent> queue;

        private NonBlockingQueueEventFetcher(Queue<MetricsCollectionEvent> queue) {
            this.queue = queue;
        }

        @Override
        public MetricsCollectionEvent fetchNextEvent() throws InterruptedException {
            MetricsCollectionEvent event = null;
            while (event == null) {
                event = queue.poll();
            }
            return event;
        }
    }

    private static class BlockingQueueEventFetcher implements QueueEventFetcher {
        private final BlockingQueue<MetricsCollectionEvent> queue;

        private BlockingQueueEventFetcher(BlockingQueue<MetricsCollectionEvent> queue) {
            this.queue = queue;
        }

        @Override
        public MetricsCollectionEvent fetchNextEvent() throws InterruptedException {
            return queue.take();
        }
    }

    private static interface QueueEventFetcher {
        MetricsCollectionEvent fetchNextEvent() throws InterruptedException;
    }
}
