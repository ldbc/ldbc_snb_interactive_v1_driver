package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.metrics.sbe.MetricsEvent;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.csv.SimpleCsvFileWriter;
import com.lmax.disruptor.EventHandler;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicStampedReference;

class DisruptorMetricsEventHandler_NEW implements EventHandler<DirectBuffer> {
    private final AtomicStampedReference<WorkloadStatusSnapshot> statusSnapshotReference = new AtomicStampedReference<>(null, 0);
    private final AtomicStampedReference<WorkloadResultsSnapshot> resultsSnapshotReference = new AtomicStampedReference<>(null, 0);

    private final MetricsManager metricsManager;
    private final ConcurrentErrorReporter errorReporter;
    private final SimpleCsvFileWriter csvResultsLogWriter;
    private final TimeUnit unit;
    private long processedEventCount = 0l;
    private final String[] operationNames;

    private static final MetricsEvent METRICS_EVENT = new MetricsEvent();

    private final int actingBlockLength;
    private final int actingVersion;
    private final int messageHeaderSize;

    DisruptorMetricsEventHandler_NEW(ConcurrentErrorReporter errorReporter,
                                     SimpleCsvFileWriter csvResultsLogWriter,
                                     TimeUnit unit,
                                     TimeSource timeSource,
                                     long maxRuntimeDurationAsNano,
                                     Map<Integer, Class<? extends Operation<?>>> operationTypeToClassMapping,
                                     int actingBlockLength,
                                     int actingVersion,
                                     int messageHeaderSize) throws MetricsCollectionException {
        this.errorReporter = errorReporter;
        this.csvResultsLogWriter = csvResultsLogWriter;
        this.unit = unit;
        this.metricsManager = new MetricsManager(
                timeSource,
                unit,
                maxRuntimeDurationAsNano,
                operationTypeToClassMapping);
        operationNames = MetricsManager.toOperationNameArray(operationTypeToClassMapping);
        this.actingBlockLength = actingBlockLength;
        this.actingVersion = actingVersion;
        this.messageHeaderSize = messageHeaderSize;
    }

    AtomicStampedReference<WorkloadStatusSnapshot> statusSnapshot() {
        return statusSnapshotReference;
    }

    AtomicStampedReference<WorkloadResultsSnapshot> resultsSnapshot() {
        return resultsSnapshotReference;
    }

    long processedEventCount() {
        return processedEventCount;
    }

    @Override
    public void onEvent(DirectBuffer event, long l, boolean b) throws Exception {
        METRICS_EVENT.wrapForDecode(event, 0, actingBlockLength, actingVersion);

        switch (METRICS_EVENT.eventType()) {
            case DisruptorMetricsCollectionEvent.SUBMIT_RESULT: {
                int operationType = METRICS_EVENT.operationType();
                long scheduledStartTimeAsMilli = METRICS_EVENT.scheduledStartTimeAsMilli();
                long actualStartTimeAsMilli = METRICS_EVENT.actualStartTimeAsMilli();
                long runDurationAsNano = METRICS_EVENT.runDurationAsNano();
                int resultCode = METRICS_EVENT.resultCode();

                if (null != csvResultsLogWriter) {
                    csvResultsLogWriter.writeRow(
                            operationNames[operationType],
                            Long.toString(scheduledStartTimeAsMilli),
                            Long.toString(actualStartTimeAsMilli),
                            Long.toString(unit.convert(runDurationAsNano, TimeUnit.NANOSECONDS)),
                            Integer.toString(resultCode)
                    );
                }
                metricsManager.measure(actualStartTimeAsMilli, runDurationAsNano, operationType);
                processedEventCount++;
                break;
            }
            case DisruptorMetricsCollectionEvent.WORKLOAD_STATUS: {
                WorkloadStatusSnapshot newStatus = metricsManager.status();
                WorkloadStatusSnapshot oldStatus;
                int oldStamp;
                do {
                    oldStatus = statusSnapshotReference.getReference();
                    oldStamp = statusSnapshotReference.getStamp();
                }
                while (false == statusSnapshotReference.compareAndSet(oldStatus, newStatus, oldStamp, oldStamp + 1));
                break;
            }
            case DisruptorMetricsCollectionEvent.WORKLOAD_RESULT: {
                WorkloadResultsSnapshot newResults = metricsManager.snapshot();
                WorkloadResultsSnapshot oldResults;
                int oldStamp;
                do {
                    oldResults = resultsSnapshotReference.getReference();
                    oldStamp = resultsSnapshotReference.getStamp();
                }
                while (false == resultsSnapshotReference.compareAndSet(oldResults, newResults, oldStamp, oldStamp + 1));
                break;
            }
            default: {
                errorReporter.reportError(
                        this,
                        String.format("Encountered unexpected event: %s", event.toString()));
                break;
            }
        }
    }
}