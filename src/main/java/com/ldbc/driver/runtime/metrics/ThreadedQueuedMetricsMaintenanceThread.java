package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResult;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;

import java.util.Queue;

public class ThreadedQueuedMetricsMaintenanceThread extends Thread {
    private final MetricsManager metricsManager;
    private final ConcurrentErrorReporter errorReporter;
    private final Queue<MetricsCollectionEvent> metricsEventsQueue;
    private Long processedEventCount = 0l;
    private Long expectedEventCount = null;

    public ThreadedQueuedMetricsMaintenanceThread(ConcurrentErrorReporter errorReporter,
                                                  Queue<MetricsCollectionEvent> metricsEventsQueue,
                                                  MetricsManager metricsManager) {
        super(ThreadedQueuedMetricsMaintenanceThread.class.getSimpleName());
        this.errorReporter = errorReporter;
        this.metricsEventsQueue = metricsEventsQueue;
        this.metricsManager = metricsManager;
    }

    @Override
    public void run() {
        while (null == expectedEventCount || processedEventCount < expectedEventCount) {
            try {
                MetricsCollectionEvent event = null;
                while (event == null) {
                    event = metricsEventsQueue.poll();
                }
                switch (event.type()) {
                    case SUBMIT_RESULT:
                        OperationResult result = ((MetricsCollectionEvent.SubmitResultEvent) event).result();
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
                    case TERMINATE:
                        if (expectedEventCount == null) {
                            expectedEventCount = ((MetricsCollectionEvent.TerminationEvent) event).expectedEventCount();
                        } else {
                            // this is not the first TERMINATE event thread has received
                            errorReporter.reportError(
                                    this,
                                    String.format("Encountered multiple TERMINATE events. First expectedEventCount[%s]. Second expectedEventCount[%s]",
                                            expectedEventCount, ((MetricsCollectionEvent.TerminationEvent) event).expectedEventCount()));
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

    private void collectResultMetrics(OperationResult operationResult) throws MetricsCollectionException {
        try {
            metricsManager.measure(operationResult);
        } catch (Exception e) {
            String errMsg = String.format("Error encountered while logging result:\n\t%s", operationResult);
            throw new MetricsCollectionException(errMsg, e);
        }
    }
}
