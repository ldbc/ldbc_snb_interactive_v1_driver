package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.Operation;
import com.ldbc.driver.control.LoggingServiceFactory;
import com.ldbc.driver.csv.simple.SimpleCsvFileWriter;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.TimeSource;
import com.lmax.disruptor.EventHandler;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicStampedReference;

import static java.lang.String.format;

class DisruptorJavolutionMetricsEventHandler implements EventHandler<DisruptorJavolutionMetricsEvent>
{
    private final AtomicStampedReference<WorkloadStatusSnapshot> statusSnapshotReference =
            new AtomicStampedReference<>( null, 0 );
    private final AtomicStampedReference<WorkloadResultsSnapshot> resultsSnapshotReference =
            new AtomicStampedReference<>( null, 0 );

    private final MetricsManager metricsManager;
    private final ConcurrentErrorReporter errorReporter;
    private final SimpleCsvFileWriter csvResultsLogWriter;
    private final TimeUnit unit;
    private long processedEventCount = 0l;
    private final String[] operationNames;

    DisruptorJavolutionMetricsEventHandler(
            ConcurrentErrorReporter errorReporter,
            SimpleCsvFileWriter csvResultsLogWriter,
            TimeUnit unit,
            TimeSource timeSource,
            long maxRuntimeDurationAsNano,
            Map<Integer,Class<? extends Operation>> operationTypeToClassMapping,
            LoggingServiceFactory loggingServiceFactory ) throws MetricsCollectionException
    {
        this.errorReporter = errorReporter;
        this.csvResultsLogWriter = csvResultsLogWriter;
        this.unit = unit;
        this.metricsManager = new MetricsManager(
                timeSource,
                unit,
                maxRuntimeDurationAsNano,
                operationTypeToClassMapping,
                loggingServiceFactory
        );
        operationNames = MetricsManager.toOperationNameArray( operationTypeToClassMapping );
    }

    AtomicStampedReference<WorkloadStatusSnapshot> statusSnapshot()
    {
        return statusSnapshotReference;
    }

    AtomicStampedReference<WorkloadResultsSnapshot> resultsSnapshot()
    {
        return resultsSnapshotReference;
    }

    long processedEventCount()
    {
        return processedEventCount;
    }

    @Override
    public void onEvent( DisruptorJavolutionMetricsEvent event, long l, boolean b ) throws Exception
    {
        switch ( event.eventType() )
        {
        case DisruptorJavolutionMetricsEvent.SUBMIT_RESULT:
        {
            if ( null != csvResultsLogWriter )
            {
                csvResultsLogWriter.writeRow(
                        operationNames[event.operationType()],
                        Long.toString( event.scheduledStartTimeAsMilli() ),
                        Long.toString( event.actualStartTimeAsMilli() ),
                        Long.toString( unit.convert( event.runDurationAsNano(), TimeUnit.NANOSECONDS ) ),
                        Integer.toString( event.resultCode() )
                );
            }
            metricsManager.measure( event.actualStartTimeAsMilli(), event.runDurationAsNano(), event.operationType() );
            processedEventCount++;
            break;
        }
        case DisruptorJavolutionMetricsEvent.WORKLOAD_STATUS:
        {
            WorkloadStatusSnapshot newStatus = metricsManager.status();
            WorkloadStatusSnapshot oldStatus;
            int oldStamp;
            do
            {
                oldStatus = statusSnapshotReference.getReference();
                oldStamp = statusSnapshotReference.getStamp();
            }
            while ( false == statusSnapshotReference.compareAndSet( oldStatus, newStatus, oldStamp, oldStamp + 1 ) );
            break;
        }
        case DisruptorJavolutionMetricsEvent.WORKLOAD_RESULT:
        {
            WorkloadResultsSnapshot newResults = metricsManager.snapshot();
            WorkloadResultsSnapshot oldResults;
            int oldStamp;
            do
            {
                oldResults = resultsSnapshotReference.getReference();
                oldStamp = resultsSnapshotReference.getStamp();
            }
            while ( false == resultsSnapshotReference.compareAndSet( oldResults, newResults, oldStamp, oldStamp + 1 ) );
            break;
        }
        default:
        {
            errorReporter.reportError( this, format( "Encountered unexpected event: %s", event.toString() ) );
            break;
        }
        }
    }
}