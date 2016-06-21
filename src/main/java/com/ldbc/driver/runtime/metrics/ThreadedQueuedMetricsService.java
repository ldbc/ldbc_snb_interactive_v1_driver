package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.Operation;
import com.ldbc.driver.control.LoggingServiceFactory;
import com.ldbc.driver.csv.simple.SimpleCsvFileWriter;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.DefaultQueues;
import com.ldbc.driver.runtime.QueueEventSubmitter;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TimeSource;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;

public class ThreadedQueuedMetricsService implements MetricsService
{
    private static final long SHUTDOWN_WAIT_TIMEOUT_AS_MILLI = TimeUnit.MINUTES.toMillis( 1 );
    private static final long FUTURE_GET_TIMEOUT_AS_MILLI = TimeUnit.MINUTES.toMillis( 30 );

    // TODO this could come from config, if we had a max_runtime parameter. for now, it can default to something
    public static final long DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO = TimeUnit.MINUTES.toNanos( 90 );

    private final TimeSource timeSource;
    private final QueueEventSubmitter<ThreadedQueuedMetricsEvent> queueEventSubmitter;
    private final AtomicLong initiatedEvents;
    private final ThreadedQueuedMetricsServiceThread threadedQueuedMetricsServiceThread;
    private final AtomicBoolean shutdown = new AtomicBoolean( false );
    private final ConcurrentLinkedQueue<ThreadedQueuedMetricsServiceWriter> metricsServiceWriters;

    public static ThreadedQueuedMetricsService newInstanceUsingNonBlockingBoundedQueue(
            TimeSource timeSource,
            ConcurrentErrorReporter errorReporter,
            TimeUnit unit,
            long maxRuntimeDurationAsNano,
            SimpleCsvFileWriter csvResultsLogWriter,
            Map<Integer,Class<? extends Operation>> operationTypeToClassMapping,
            LoggingServiceFactory loggingServiceFactory ) throws MetricsCollectionException
    {
        Queue<ThreadedQueuedMetricsEvent> queue = DefaultQueues.newBlockingBounded( 10_000 );
        return new ThreadedQueuedMetricsService(
                timeSource,
                errorReporter,
                unit,
                maxRuntimeDurationAsNano,
                queue,
                csvResultsLogWriter,
                operationTypeToClassMapping,
                loggingServiceFactory
        );
    }

    public static ThreadedQueuedMetricsService newInstanceUsingBlockingBoundedQueue(
            TimeSource timeSource,
            ConcurrentErrorReporter errorReporter,
            TimeUnit unit,
            long maxRuntimeDurationAsNano,
            SimpleCsvFileWriter csvResultsLogWriter,
            Map<Integer,Class<? extends Operation>> operationTypeToClassMapping,
            LoggingServiceFactory loggingServiceFactory ) throws MetricsCollectionException
    {
        Queue<ThreadedQueuedMetricsEvent> queue = DefaultQueues.newBlockingBounded( 10000 );
        return new ThreadedQueuedMetricsService(
                timeSource,
                errorReporter,
                unit,
                maxRuntimeDurationAsNano,
                queue,
                csvResultsLogWriter,
                operationTypeToClassMapping,
                loggingServiceFactory
        );
    }

    private ThreadedQueuedMetricsService(
            TimeSource timeSource,
            ConcurrentErrorReporter errorReporter,
            TimeUnit unit,
            long maxRuntimeDurationAsNano,
            Queue<ThreadedQueuedMetricsEvent> queue,
            SimpleCsvFileWriter csvResultsLogWriter,
            Map<Integer,Class<? extends Operation>> operationTypeToClassMapping,
            LoggingServiceFactory loggingServiceFactory ) throws MetricsCollectionException
    {
        this.timeSource = timeSource;
        queueEventSubmitter = QueueEventSubmitter.queueEventSubmitterFor( queue );
        initiatedEvents = new AtomicLong( 0 );
        metricsServiceWriters = new ConcurrentLinkedQueue<>();
        threadedQueuedMetricsServiceThread = new ThreadedQueuedMetricsServiceThread(
                errorReporter,
                queue,
                csvResultsLogWriter,
                timeSource,
                unit,
                maxRuntimeDurationAsNano,
                operationTypeToClassMapping,
                loggingServiceFactory
        );
        threadedQueuedMetricsServiceThread.start();
    }

    @Override
    synchronized public void shutdown() throws MetricsCollectionException
    {
        if ( shutdown.get() )
        {
            throw new MetricsCollectionException( "Metrics service has already been shutdown" );
        }
        try
        {
            ThreadedQueuedMetricsEvent event = new ThreadedQueuedMetricsEvent.Shutdown( initiatedEvents.get() );
            queueEventSubmitter.submitEventToQueue( event );
            threadedQueuedMetricsServiceThread.join( SHUTDOWN_WAIT_TIMEOUT_AS_MILLI );
        }
        catch ( InterruptedException e )
        {
            String errMsg = format( "Thread was interrupted while waiting for %s to complete",
                    threadedQueuedMetricsServiceThread.getClass().getSimpleName() );
            throw new MetricsCollectionException( errMsg, e );
        }
        AlreadyShutdownPolicy alreadyShutdownPolicy = new AlreadyShutdownPolicy();
        for ( ThreadedQueuedMetricsServiceWriter metricsServiceWriter : metricsServiceWriters )
        {
            metricsServiceWriter.setAlreadyShutdownPolicy( alreadyShutdownPolicy );
        }
        shutdown.set( true );
    }

    @Override
    public MetricsServiceWriter getWriter() throws MetricsCollectionException
    {
        if ( shutdown.get() )
        {
            throw new MetricsCollectionException( "Metrics service has already been shutdown" );
        }
        ThreadedQueuedMetricsServiceWriter metricsServiceWriter =
                new ThreadedQueuedMetricsServiceWriter( initiatedEvents, queueEventSubmitter, timeSource );
        metricsServiceWriters.add( metricsServiceWriter );
        return metricsServiceWriter;
    }

    private static class ThreadedQueuedMetricsServiceWriter implements MetricsServiceWriter
    {
        private final AtomicLong initiatedEvents;
        private final QueueEventSubmitter<ThreadedQueuedMetricsEvent> queueEventSubmitter;
        private final TimeSource timeSource;

        private AlreadyShutdownPolicy alreadyShutdownPolicy = null;

        private ThreadedQueuedMetricsServiceWriter( AtomicLong initiatedEvents,
                QueueEventSubmitter<ThreadedQueuedMetricsEvent> queueEventSubmitter,
                TimeSource timeSource )
        {
            this.initiatedEvents = initiatedEvents;
            this.queueEventSubmitter = queueEventSubmitter;
            this.timeSource = timeSource;
        }

        private void setAlreadyShutdownPolicy( AlreadyShutdownPolicy alreadyShutdownPolicy )
        {
            this.alreadyShutdownPolicy = alreadyShutdownPolicy;
        }

        @Override
        public void submitOperationResult( int operationType, long scheduledStartTimeAsMilli,
                long actualStartTimeAsMilli, long runDurationAsNano, int resultCode, long originalStartTime )
                throws MetricsCollectionException
        {
            if ( null != alreadyShutdownPolicy )
            {
                alreadyShutdownPolicy.apply();
            }
            try
            {
                initiatedEvents.incrementAndGet();
                ThreadedQueuedMetricsEvent event = new ThreadedQueuedMetricsEvent.SubmitOperationResult(
                        operationType,
                        scheduledStartTimeAsMilli,
                        actualStartTimeAsMilli,
                        runDurationAsNano,
                        resultCode,
                        originalStartTime
                );
                queueEventSubmitter.submitEventToQueue( event );
            }
            catch ( InterruptedException e )
            {
                String errMsg = format(
                        "Error submitting result\n"
                        + "Operation Type: %s\n"
                        + "Scheduled Start Time Ms: %s\n"
                        + "Actual Start Time Ms: %s\n"
                        + "Duration Ns: %s\n"
                        + "Result Code: %s\n"
                        + "Original start time: %s\n"
                        ,
                        operationType,
                        scheduledStartTimeAsMilli,
                        actualStartTimeAsMilli,
                        runDurationAsNano,
                        resultCode,
                        originalStartTime
                );
                throw new MetricsCollectionException( errMsg, e );
            }
        }

        @Override
        public WorkloadStatusSnapshot status() throws MetricsCollectionException
        {
            if ( null != alreadyShutdownPolicy )
            {
                alreadyShutdownPolicy.apply();
            }
            try
            {
                MetricsStatusFuture statusFuture = new MetricsStatusFuture( timeSource );
                ThreadedQueuedMetricsEvent event = new ThreadedQueuedMetricsEvent.Status( statusFuture );
                queueEventSubmitter.submitEventToQueue( event );
                return statusFuture.get( FUTURE_GET_TIMEOUT_AS_MILLI, TimeUnit.MILLISECONDS );
            }
            catch ( Exception e )
            {
                throw new MetricsCollectionException( "Error while submitting request for workload status", e );
            }
        }

        @Override
        public WorkloadResultsSnapshot results() throws MetricsCollectionException
        {
            if ( null != alreadyShutdownPolicy )
            {
                alreadyShutdownPolicy.apply();
            }
            try
            {
                MetricsWorkloadResultFuture workloadResultFuture = new MetricsWorkloadResultFuture( timeSource );
                ThreadedQueuedMetricsEvent event =
                        new ThreadedQueuedMetricsEvent.GetWorkloadResults( workloadResultFuture );
                queueEventSubmitter.submitEventToQueue( event );
                return workloadResultFuture.get( FUTURE_GET_TIMEOUT_AS_MILLI, TimeUnit.MILLISECONDS );
            }
            catch ( Exception e )
            {
                throw new MetricsCollectionException( "Error while submitting request for workload results", e );
            }
        }
    }

    static class MetricsWorkloadResultFuture implements Future<WorkloadResultsSnapshot>
    {
        private final TimeSource timeSource;
        private final AtomicBoolean done = new AtomicBoolean( false );
        private final AtomicReference<WorkloadResultsSnapshot> startTime = new AtomicReference<>( null );

        private MetricsWorkloadResultFuture( TimeSource timeSource )
        {
            this.timeSource = timeSource;
        }

        synchronized void set( WorkloadResultsSnapshot value ) throws MetricsCollectionException
        {
            if ( done.get() )
            {
                throw new MetricsCollectionException( "Value has already been set" );
            }
            startTime.set( value );
            done.set( true );
        }

        @Override
        public boolean cancel( boolean mayInterruptIfRunning )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCancelled()
        {
            return false;
        }

        @Override
        public boolean isDone()
        {
            return done.get();
        }

        @Override
        public WorkloadResultsSnapshot get()
        {
            while ( !done.get() )
            {
                // wait for value to be set
                Spinner.powerNap( 1 );
            }
            return startTime.get();
        }

        @Override
        public WorkloadResultsSnapshot get( long timeout, TimeUnit unit ) throws TimeoutException
        {
            long waitDurationMs = unit.toMillis( timeout );
            long startTimeMs = timeSource.nowAsMilli();
            while ( timeSource.nowAsMilli() - startTimeMs < waitDurationMs )
            {
                // wait for value to be set
                if ( done.get() )
                {
                    return startTime.get();
                }
            }
            throw new TimeoutException( "Could not complete future in time" );
        }
    }

    static class MetricsStatusFuture implements Future<WorkloadStatusSnapshot>
    {
        private final TimeSource timeSource;
        private final AtomicBoolean done = new AtomicBoolean( false );
        private final AtomicReference<WorkloadStatusSnapshot> status = new AtomicReference<>( null );

        private MetricsStatusFuture( TimeSource timeSource )
        {
            this.timeSource = timeSource;
        }

        synchronized void set( WorkloadStatusSnapshot value ) throws MetricsCollectionException
        {
            if ( done.get() )
            {
                throw new MetricsCollectionException( "Value has already been set" );
            }
            status.set( value );
            done.set( true );
        }

        @Override
        public boolean cancel( boolean mayInterruptIfRunning )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCancelled()
        {
            return false;
        }

        @Override
        public boolean isDone()
        {
            return done.get();
        }

        @Override
        public WorkloadStatusSnapshot get()
        {
            while ( !done.get() )
            {
                // wait for value to be set
                Spinner.powerNap( 1 );
            }
            return status.get();
        }

        @Override
        public WorkloadStatusSnapshot get( long timeout, TimeUnit unit ) throws TimeoutException
        {
            long waitDurationMs = unit.toMillis( timeout );
            long startTimeMs = timeSource.nowAsMilli();
            while ( timeSource.nowAsMilli() - startTimeMs < waitDurationMs )
            {
                // wait for value to be set
                if ( done.get() )
                {
                    return status.get();
                }
            }
            throw new TimeoutException( "Could not complete future in time" );
        }
    }

    private static class AlreadyShutdownPolicy
    {
        void apply() throws MetricsCollectionException
        {
            throw new MetricsCollectionException( "Metrics service has already been shutdown" );
        }
    }
}
