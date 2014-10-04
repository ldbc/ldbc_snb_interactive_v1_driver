package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.QueueEventFetcher;

import java.util.Queue;

public class ThreadedQueuedConcurrentMetricsServiceThread extends Thread {
    private final MetricsManager metricsManager;
    private final ConcurrentErrorReporter errorReporter;
    private final QueueEventFetcher<MetricsCollectionEvent> queueEventFetcher;
    private Long processedEventCount = 0l;
    private Long expectedEventCount = null;

    public ThreadedQueuedConcurrentMetricsServiceThread(ConcurrentErrorReporter errorReporter,
                                                        Queue<MetricsCollectionEvent> metricsEventsQueue,
                                                        MetricsManager metricsManager) {
        this(errorReporter, QueueEventFetcher.queueEventFetcherFor(metricsEventsQueue), metricsManager);
    }

    private ThreadedQueuedConcurrentMetricsServiceThread(ConcurrentErrorReporter errorReporter,
                                                         QueueEventFetcher<MetricsCollectionEvent> queueEventFetcher,
                                                         MetricsManager metricsManager) {
        super(ThreadedQueuedConcurrentMetricsServiceThread.class.getSimpleName() + "-" + System.currentTimeMillis());
        this.errorReporter = errorReporter;
        this.metricsManager = metricsManager;
        this.queueEventFetcher = queueEventFetcher;
    }

    @Override
    public void run() {
        while (null == expectedEventCount || processedEventCount < expectedEventCount) {
            try {
                MetricsCollectionEvent event = queueEventFetcher.fetchNextEvent();
                switch (event.type()) {
                    case SUBMIT_RESULT:
                        OperationResultReport result = ((MetricsCollectionEvent.SubmitResultEvent) event).result();

                        try {
                            metricsManager.measure(result);
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
}
