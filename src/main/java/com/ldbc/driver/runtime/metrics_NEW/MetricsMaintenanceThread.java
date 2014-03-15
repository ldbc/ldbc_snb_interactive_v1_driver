package com.ldbc.driver.runtime.metrics_NEW;

import com.ldbc.driver.OperationResult;
import com.ldbc.driver.runtime.error.ConcurrentErrorReporter;

import java.util.Queue;

// TODO test
public class MetricsMaintenanceThread extends Thread {
    private final WorkloadMetricsManager metricsManager;
    private final ConcurrentErrorReporter errorReporter;
    private final Queue<MetricsCollectionEvent> metricsEventsQueue;
    private Long processedEventCount = 0l;
    private Long expectedEventCount = null;

    public MetricsMaintenanceThread(ConcurrentErrorReporter errorReporter,
                                    Queue<MetricsCollectionEvent> metricsEventsQueue,
                                    WorkloadMetricsManager metricsManager) {
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
                    case RESULT:
                        OperationResult result = ((MetricsCollectionEvent.ResultEvent) event).result();
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
                    case EXPORT:
                        ThreadedQueuedConcurrentMetricsService.MetricsExportFuture exportFuture = ((MetricsCollectionEvent.ExportEvent) event).future();
                        exportFuture.export(metricsManager);
                        break;
                    case STATUS:
                        ThreadedQueuedConcurrentMetricsService.MetricsStatusFuture statusFuture = ((MetricsCollectionEvent.StatusEvent) event).future();
                        statusFuture.set(metricsManager.getStatusString());
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
            throw new MetricsCollectionException(errMsg, e.getCause());
        }
    }
}
