package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.QueueEventFetcher;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.csv.SimpleCsvFileWriter;

import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class ThreadedQueuedConcurrentMetricsServiceThread extends Thread {
    private final TemporalUtil temporalUtil = new TemporalUtil();
    private final MetricsManager metricsManager;
    private final ConcurrentErrorReporter errorReporter;
    private final QueueEventFetcher<MetricsCollectionEvent> queueEventFetcher;
    private final SimpleCsvFileWriter csvResultsLogWriter;
    private final TimeUnit unit;
    private Long processedEventCount = 0l;
    private Long expectedEventCount = null;

    public ThreadedQueuedConcurrentMetricsServiceThread(ConcurrentErrorReporter errorReporter,
                                                        Queue<MetricsCollectionEvent> metricsEventsQueue,
                                                        SimpleCsvFileWriter csvResultsLogWriter,
                                                        TimeSource timeSource,
                                                        TimeUnit unit,
                                                        long maxRuntimeDurationAsNano) {
        this(errorReporter,
                QueueEventFetcher.queueEventFetcherFor(metricsEventsQueue),
                csvResultsLogWriter,
                timeSource,
                unit,
                maxRuntimeDurationAsNano);
    }

    private ThreadedQueuedConcurrentMetricsServiceThread(ConcurrentErrorReporter errorReporter,
                                                         QueueEventFetcher<MetricsCollectionEvent> queueEventFetcher,
                                                         SimpleCsvFileWriter csvResultsLogWriter,
                                                         TimeSource timeSource,
                                                         TimeUnit unit,
                                                         long maxRuntimeDurationAsNano) {
        super(ThreadedQueuedConcurrentMetricsServiceThread.class.getSimpleName() + "-" + System.currentTimeMillis());
        this.errorReporter = errorReporter;
        this.queueEventFetcher = queueEventFetcher;
        this.csvResultsLogWriter = csvResultsLogWriter;
        this.unit = unit;
        this.metricsManager = new MetricsManager(
                timeSource,
                unit,
                maxRuntimeDurationAsNano);
    }

    @Override
    public void run() {
        while (null == expectedEventCount || processedEventCount < expectedEventCount) {
            try {
                MetricsCollectionEvent event = queueEventFetcher.fetchNextEvent();
                switch (event.type()) {
                    case SUBMIT_RESULT:
                        OperationResultReport result = (OperationResultReport) event.value();
                        event.release();

                        if (null != csvResultsLogWriter) {
                            csvResultsLogWriter.writeRow(
                                    result.operation().getClass().getSimpleName(),
                                    Long.toString(result.operation().scheduledStartTimeAsMilli()),
                                    Long.toString(result.actualStartTimeAsMilli()),
                                    Long.toString(temporalUtil.convert(result.runDurationAsNano(), TimeUnit.NANOSECONDS, unit)),
                                    Integer.toString(result.resultCode())
                            );
                        }

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
                        ThreadedQueuedConcurrentMetricsService.MetricsStatusFuture statusFuture = (ThreadedQueuedConcurrentMetricsService.MetricsStatusFuture) event.value();
                        event.release();
                        statusFuture.set(metricsManager.status());
                        break;
                    case WORKLOAD_RESULT:
                        ThreadedQueuedConcurrentMetricsService.MetricsWorkloadResultFuture workloadResultFuture = (ThreadedQueuedConcurrentMetricsService.MetricsWorkloadResultFuture) event.value();
                        event.release();
                        WorkloadResultsSnapshot resultsSnapshot = metricsManager.snapshot();
                        workloadResultFuture.set(resultsSnapshot);
                        break;
                    case TERMINATE_SERVICE:
                        long eventExpectedEventCount = (long) event.value();
                        event.release();
                        if (null == expectedEventCount) {
                            expectedEventCount = eventExpectedEventCount;
                        } else {
                            // this is not the first termination event that the thread has received
                            errorReporter.reportError(
                                    this,
                                    String.format("Encountered multiple %s events. First expectedEventCount[%s]. Second expectedEventCount[%s]",
                                            MetricsCollectionEvent.MetricsEventType.TERMINATE_SERVICE.name(),
                                            expectedEventCount,
                                            eventExpectedEventCount));
                        }
                        break;
                    default:
                        errorReporter.reportError(
                                this,
                                String.format("Encountered unexpected event type: %s", event.type().name()));
                        event.release();
                        return;
                }
            } catch (Throwable e) {
                errorReporter.reportError(
                        this,
                        String.format("Encountered unexpected exception\n%s", ConcurrentErrorReporter.stackTraceToString(e)));
                return;
            }
        }
    }
}
