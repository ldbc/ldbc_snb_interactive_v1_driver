package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.Operation;
import com.ldbc.driver.control.LoggingServiceFactory;
import com.ldbc.driver.csv.simple.SimpleCsvFileWriter;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.QueueEventFetcher;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedMetricsEvent.GetWorkloadResults;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedMetricsEvent.Shutdown;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedMetricsEvent.Status;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedMetricsEvent.SubmitOperationResult;
import com.ldbc.driver.temporal.TimeSource;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class ThreadedQueuedMetricsServiceThread extends Thread
{
    private final MetricsManager metricsManager;
    private final ConcurrentErrorReporter errorReporter;
    private final QueueEventFetcher<ThreadedQueuedMetricsEvent> queueEventFetcher;
    private final SimpleCsvFileWriter csvResultsLogWriter;
    private final TimeUnit unit;
    private Long processedEventCount = 0l;
    private Long expectedEventCount = null;
    private final String[] operationNames;

    public ThreadedQueuedMetricsServiceThread( ConcurrentErrorReporter errorReporter,
            Queue<ThreadedQueuedMetricsEvent> metricsEventsQueue,
            SimpleCsvFileWriter csvResultsLogWriter,
            TimeSource timeSource,
            TimeUnit unit,
            long maxRuntimeDurationAsNano,
            Map<Integer,Class<? extends Operation>> operationTypeToClassMapping,
            LoggingServiceFactory loggingServiceFactory ) throws MetricsCollectionException
    {
        this( errorReporter,
                QueueEventFetcher.queueEventFetcherFor( metricsEventsQueue ),
                csvResultsLogWriter,
                timeSource,
                unit,
                maxRuntimeDurationAsNano,
                operationTypeToClassMapping,
                loggingServiceFactory
        );
    }

    private ThreadedQueuedMetricsServiceThread(
            ConcurrentErrorReporter errorReporter,
            QueueEventFetcher<ThreadedQueuedMetricsEvent> queueEventFetcher,
            SimpleCsvFileWriter csvResultsLogWriter,
            TimeSource timeSource,
            TimeUnit unit,
            long maxRuntimeDurationAsNano,
            Map<Integer,Class<? extends Operation>> operationTypeToClassMapping,
            LoggingServiceFactory loggingServiceFactory ) throws MetricsCollectionException
    {
        super( ThreadedQueuedMetricsServiceThread.class.getSimpleName() + "-" + System.currentTimeMillis() );
        this.errorReporter = errorReporter;
        this.queueEventFetcher = queueEventFetcher;
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

    @Override
    public void run()
    {
        while ( null == expectedEventCount || processedEventCount < expectedEventCount )
        {
            try
            {
                ThreadedQueuedMetricsEvent event = queueEventFetcher.fetchNextEvent();
                onEvent( event );
            }
            catch ( Throwable e )
            {
                errorReporter.reportError(
                        this,
                        format( "Encountered unexpected exception\n%s",
                                ConcurrentErrorReporter.stackTraceToString( e ) ) );
                return;
            }
        }
    }

    public void onEvent( ThreadedQueuedMetricsEvent event ) throws IOException, MetricsCollectionException
    {
        switch ( event.type() )
        {
        case SUBMIT_RESULT:
            SubmitOperationResult submitOperationResultEvent = (SubmitOperationResult) event;
            if ( null != csvResultsLogWriter )
            {
                csvResultsLogWriter.writeRow(
                        operationNames[submitOperationResultEvent.operationType()],
                        Long.toString( submitOperationResultEvent.scheduledStartTimeAsMilli() ),
                        Long.toString( submitOperationResultEvent.actualStartTimeAsMilli() ),
                        Long.toString(
                                unit.convert( submitOperationResultEvent.runDurationAsNano(), TimeUnit.NANOSECONDS ) ),
                        Integer.toString( submitOperationResultEvent.resultCode() ),
                        Long.toString( submitOperationResultEvent.originalStartTime() )
                );
            }

            try
            {
                metricsManager.measure(
                        submitOperationResultEvent.actualStartTimeAsMilli(),
                        submitOperationResultEvent.runDurationAsNano(),
                        submitOperationResultEvent.operationType()
                );
            }
            catch ( MetricsCollectionException e )
            {
                errorReporter.reportError(
                        this,
                        format(
                                "Encountered error while collecting metrics for result\n"
                                + "Operation Type: %s\n"
                                + "Scheduled Start Time Ms: %s\n"
                                + "Actual Start Time Ms: %s\n"
                                + "Duration Ns: %s\n"
                                + "Result Code: %s\n"
                                + "Original start time: %s\n"
                                ,
                                submitOperationResultEvent.operationType(),
                                submitOperationResultEvent.scheduledStartTimeAsMilli(),
                                submitOperationResultEvent.actualStartTimeAsMilli(),
                                submitOperationResultEvent.runDurationAsNano(),
                                submitOperationResultEvent.resultCode(),
                                submitOperationResultEvent.originalStartTime(),
                                ConcurrentErrorReporter.stackTraceToString( e )
                        )
                );
            }

            processedEventCount++;
            break;
        case WORKLOAD_STATUS:
            ThreadedQueuedMetricsService.MetricsStatusFuture statusFuture = ((Status) event).statusFuture();
            statusFuture.set( metricsManager.status() );
            break;
        case WORKLOAD_RESULT:
            ThreadedQueuedMetricsService.MetricsWorkloadResultFuture workloadResultFuture =
                    ((GetWorkloadResults) event).workloadResultFuture();
            WorkloadResultsSnapshot resultsSnapshot = metricsManager.snapshot();
            workloadResultFuture.set( resultsSnapshot );
            break;
        case SHUTDOWN_SERVICE:
            if ( null == expectedEventCount )
            {
                expectedEventCount = ((Shutdown) event).initiatedEvents();
            }
            else
            {
                // this is not the first termination event that the thread has received
                errorReporter.reportError(
                        this,
                        format(
                                "Encountered multiple %s events. First expectedEventCount[%s]. Second " +
                                "expectedEventCount[%s]",
                                ThreadedQueuedMetricsEvent.MetricsEventType.SHUTDOWN_SERVICE.name(),
                                expectedEventCount,
                                ((Shutdown) event).initiatedEvents() ) );
            }
            break;
        default:
            errorReporter.reportError(
                    this,
                    format( "Encountered unexpected event type: %s", event.type().name() ) );
            return;
        }
    }
}
