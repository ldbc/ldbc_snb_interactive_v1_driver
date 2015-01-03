package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.QueueEventFetcher;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedMetricsCollectionEvent.GetWorkloadResults;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedMetricsCollectionEvent.Shutdown;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedMetricsCollectionEvent.Status;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedMetricsCollectionEvent.SubmitOperationResult;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.csv.SimpleCsvFileWriter;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class ThreadedQueuedConcurrentMetricsServiceThread extends Thread {
    private final MetricsManager metricsManager;
    private final ConcurrentErrorReporter errorReporter;
    private final QueueEventFetcher<ThreadedQueuedMetricsCollectionEvent> queueEventFetcher;
    private final SimpleCsvFileWriter csvResultsLogWriter;
    private final TimeUnit unit;
    private Long processedEventCount = 0l;
    private Long expectedEventCount = null;
    private final String[] operationNames;

    public ThreadedQueuedConcurrentMetricsServiceThread(ConcurrentErrorReporter errorReporter,
                                                        Queue<ThreadedQueuedMetricsCollectionEvent> metricsEventsQueue,
                                                        SimpleCsvFileWriter csvResultsLogWriter,
                                                        TimeSource timeSource,
                                                        TimeUnit unit,
                                                        long maxRuntimeDurationAsNano,
                                                        Map<Integer, Class<? extends Operation<?>>> operationTypeToClassMapping) throws MetricsCollectionException {
        this(errorReporter,
                QueueEventFetcher.queueEventFetcherFor(metricsEventsQueue),
                csvResultsLogWriter,
                timeSource,
                unit,
                maxRuntimeDurationAsNano,
                operationTypeToClassMapping);
    }

    private ThreadedQueuedConcurrentMetricsServiceThread(ConcurrentErrorReporter errorReporter,
                                                         QueueEventFetcher<ThreadedQueuedMetricsCollectionEvent> queueEventFetcher,
                                                         SimpleCsvFileWriter csvResultsLogWriter,
                                                         TimeSource timeSource,
                                                         TimeUnit unit,
                                                         long maxRuntimeDurationAsNano,
                                                         Map<Integer, Class<? extends Operation<?>>> operationTypeToClassMapping) throws MetricsCollectionException {
        super(ThreadedQueuedConcurrentMetricsServiceThread.class.getSimpleName() + "-" + System.currentTimeMillis());
        this.errorReporter = errorReporter;
        this.queueEventFetcher = queueEventFetcher;
        this.csvResultsLogWriter = csvResultsLogWriter;
        this.unit = unit;
        this.metricsManager = new MetricsManager(
                timeSource,
                unit,
                maxRuntimeDurationAsNano,
                operationTypeToClassMapping);
        operationNames = MetricsManager.toOperationNameArray(operationTypeToClassMapping);
    }

    @Override
    public void run() {
        while (null == expectedEventCount || processedEventCount < expectedEventCount) {
            try {
                ThreadedQueuedMetricsCollectionEvent event = queueEventFetcher.fetchNextEvent();
                onEvent(event);
            } catch (Throwable e) {
                errorReporter.reportError(
                        this,
                        String.format("Encountered unexpected exception\n%s", ConcurrentErrorReporter.stackTraceToString(e)));
                return;
            }
        }
    }

    public void onEvent(ThreadedQueuedMetricsCollectionEvent event) throws IOException, MetricsCollectionException {
        switch (event.type()) {
            case SUBMIT_RESULT:
                SubmitOperationResult submitOperationResultEvent = (SubmitOperationResult) event;
                if (null != csvResultsLogWriter) {
                    csvResultsLogWriter.writeRow(
                            operationNames[submitOperationResultEvent.operationType()],
                            Long.toString(submitOperationResultEvent.scheduledStartTimeAsMilli()),
                            Long.toString(submitOperationResultEvent.actualStartTimeAsMilli()),
                            Long.toString(unit.convert(submitOperationResultEvent.runDurationAsNano(), TimeUnit.NANOSECONDS)),
                            Integer.toString(submitOperationResultEvent.resultCode())
                    );
                }

                try {
                    metricsManager.measure(
                            submitOperationResultEvent.actualStartTimeAsMilli(),
                            submitOperationResultEvent.runDurationAsNano(),
                            submitOperationResultEvent.operationType()
                    );
                } catch (MetricsCollectionException e) {
                    errorReporter.reportError(
                            this,
                            String.format(
                                    "Encountered error while collecting metrics for result\n"
                                            + "Operation Type: %s\n"
                                            + "Scheduled Start Time Ms: %s\n"
                                            + "Actual Start Time Ms: %s\n"
                                            + "Duration Ns: %s\n"
                                            + "Result Code: %s\n"
                                    ,
                                    submitOperationResultEvent.operationType(),
                                    submitOperationResultEvent.scheduledStartTimeAsMilli(),
                                    submitOperationResultEvent.actualStartTimeAsMilli(),
                                    submitOperationResultEvent.runDurationAsNano(),
                                    submitOperationResultEvent.resultCode(),
                                    ConcurrentErrorReporter.stackTraceToString(e)
                            )
                    );
                }

                processedEventCount++;
                break;
            case WORKLOAD_STATUS:
                ThreadedQueuedConcurrentMetricsService.MetricsStatusFuture statusFuture = ((Status) event).statusFuture();
                statusFuture.set(metricsManager.status());
                break;
            case WORKLOAD_RESULT:
                ThreadedQueuedConcurrentMetricsService.MetricsWorkloadResultFuture workloadResultFuture = ((GetWorkloadResults) event).workloadResultFuture();
                WorkloadResultsSnapshot resultsSnapshot = metricsManager.snapshot();
                workloadResultFuture.set(resultsSnapshot);
                break;
            case SHUTDOWN_SERVICE:
                if (null == expectedEventCount) {
                    expectedEventCount = ((Shutdown) event).initiatedEvents();
                } else {
                    // this is not the first termination event that the thread has received
                    errorReporter.reportError(
                            this,
                            String.format("Encountered multiple %s events. First expectedEventCount[%s]. Second expectedEventCount[%s]",
                                    ThreadedQueuedMetricsCollectionEvent.MetricsEventType.SHUTDOWN_SERVICE.name(),
                                    expectedEventCount,
                                    ((Shutdown) event).initiatedEvents()));
                }
                break;
            default:
                errorReporter.reportError(
                        this,
                        String.format("Encountered unexpected event type: %s", event.type().name()));
                return;
        }
    }
}
