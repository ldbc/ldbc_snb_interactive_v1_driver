package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.QueueEventFetcher;
import com.ldbc.driver.runtime.scheduling.ExecutionDelayPolicy;
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
    private final ExecutionDelayPolicy executionDelayPolicy;
    private final boolean shouldRecordStartTimeDelayLatencies;
    private final SimpleCsvFileWriter csvResultsLogWriter;
    private final TimeUnit unit;
    private Long processedEventCount = 0l;
    private Long expectedEventCount = null;

    public ThreadedQueuedConcurrentMetricsServiceThread(ConcurrentErrorReporter errorReporter,
                                                        Queue<MetricsCollectionEvent> metricsEventsQueue,
                                                        boolean shouldRecordStartTimeDelayLatencies,
                                                        ExecutionDelayPolicy executionDelayPolicy,
                                                        SimpleCsvFileWriter csvResultsLogWriter,
                                                        TimeSource timeSource,
                                                        TimeUnit unit,
                                                        long maxRuntimeDurationAsNano) {
        this(errorReporter,
                QueueEventFetcher.queueEventFetcherFor(metricsEventsQueue),
                shouldRecordStartTimeDelayLatencies,
                executionDelayPolicy,
                csvResultsLogWriter,
                timeSource,
                unit,
                maxRuntimeDurationAsNano);
    }

    private ThreadedQueuedConcurrentMetricsServiceThread(ConcurrentErrorReporter errorReporter,
                                                         QueueEventFetcher<MetricsCollectionEvent> queueEventFetcher,
                                                         boolean shouldRecordStartTimeDelayLatencies,
                                                         ExecutionDelayPolicy executionDelayPolicy,
                                                         SimpleCsvFileWriter csvResultsLogWriter,
                                                         TimeSource timeSource,
                                                         TimeUnit unit,
                                                         long maxRuntimeDurationAsNano) {
        super(ThreadedQueuedConcurrentMetricsServiceThread.class.getSimpleName() + "-" + System.currentTimeMillis());
        this.errorReporter = errorReporter;
        this.queueEventFetcher = queueEventFetcher;
        this.shouldRecordStartTimeDelayLatencies = shouldRecordStartTimeDelayLatencies;
        this.executionDelayPolicy = executionDelayPolicy;
        this.csvResultsLogWriter = csvResultsLogWriter;
        this.unit = unit;
        this.metricsManager = new MetricsManager(
                timeSource,
                unit,
                maxRuntimeDurationAsNano,
                executionDelayPolicy.toleratedDelayAsMilli(),
                shouldRecordStartTimeDelayLatencies);
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
                                    Long.toString(result.runDurationAsNano()),
                                    Integer.toString(result.resultCode())
                            );
                        }

                        boolean shouldRecordResultMetricsForThisOperation = true;
                        if (shouldRecordStartTimeDelayLatencies) {
                            // TODO if operation is blocked in spinner because something like GCT_CHECK never returns there needs to be a way to detect and terminate
                            // TODO this may not be triggered by maximum runtime check, as execution does not begin until spinner returns
                            // TODO perhaps it can be done in the same/similar way though, by somehow getting metrics service to occasionally check for "progress"? look into further
                            // TOO EARLY = <---(now)--(scheduled)[<---delay--->]------> <=(Time Line)
                            // GOOD      = <-----(scheduled)[<-(now)--delay--->]------> <=(Time Line)
                            // TOO LATE  = <-----(scheduled)[<---delay--->]--(now)----> <=(Time Line)
                            if (result.operation().scheduledStartTimeAsMilli() + executionDelayPolicy.toleratedDelayAsMilli() < result.actualStartTimeAsMilli()) {
                                shouldRecordResultMetricsForThisOperation = executionDelayPolicy.handleExcessiveDelay(result.operation());
                            }
                        }

                        if (shouldRecordResultMetricsForThisOperation) {
                            try {
                                metricsManager.measure(result);
                            } catch (MetricsCollectionException e) {
                                errorReporter.reportError(
                                        this,
                                        String.format("Encountered error while collecting metrics for result: %s\n%s",
                                                result.toString(),
                                                ConcurrentErrorReporter.stackTraceToString(e)));
                            }
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
