package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.DefaultQueues;
import com.ldbc.driver.runtime.QueueEventSubmitter;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TimeSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

public class ThreadedQueuedCompletionTimeService implements CompletionTimeService
{
    private static final long SHUTDOWN_WAIT_TIMEOUT_AS_MILLI = TimeUnit.SECONDS.toMillis( 10 );

    private final TimeSource timeSource;
    private final QueueEventSubmitter<CompletionTimeEvent> queueEventSubmitter;
    private final AtomicLong sharedCtReference;
    private final AtomicLong sharedWriteEventCountReference;
    private final ThreadedQueuedCompletionTimeServiceThread threadedQueuedCompletionTimeServiceThread;
    private final AtomicBoolean sharedIsShuttingDownReference = new AtomicBoolean( false );
    private final ConcurrentErrorReporter errorReporter;
    private final List<CompletionTimeWriter> writers = new ArrayList<>();

    ThreadedQueuedCompletionTimeService( TimeSource timeSource,
            ConcurrentErrorReporter errorReporter ) throws CompletionTimeException
    {
        this.timeSource = timeSource;
        this.errorReporter = errorReporter;
        Queue<CompletionTimeEvent> completionTimeEventQueue = DefaultQueues.newBlockingBounded( 10000 );
        this.queueEventSubmitter = QueueEventSubmitter.queueEventSubmitterFor( completionTimeEventQueue );
        this.sharedCtReference = new AtomicLong( -1 );
        this.sharedWriteEventCountReference = new AtomicLong( 0 );
        threadedQueuedCompletionTimeServiceThread = new ThreadedQueuedCompletionTimeServiceThread(
                completionTimeEventQueue,
                errorReporter,
                sharedCtReference );
        threadedQueuedCompletionTimeServiceThread.start();
    }

    @Override
    public long completionTimeAsMilli()
    {
        return sharedCtReference.get();
    }

    @Override
    public CompletionTimeWriter newCompletionTimeWriter() throws CompletionTimeException
    {
        long futureTimeoutDurationAsMilli = TimeUnit.MINUTES.toMillis( 1 );
        try
        {
            CompletionTimeWriterFuture future = new CompletionTimeWriterFuture( timeSource );
            queueEventSubmitter.submitEventToQueue( CompletionTimeEvent.newCompletionTimeWriter( future ) );
            int writerId;
            try
            {
                writerId = future.get( futureTimeoutDurationAsMilli, TimeUnit.MILLISECONDS );
                CompletionTimeWriter writer = new ThreadedQueuedCompletionTimeWriter(
                        writerId,
                        sharedIsShuttingDownReference,
                        sharedWriteEventCountReference,
                        queueEventSubmitter );
                writers.add( writer );
                return writer;
            }
            catch ( TimeoutException e )
            {
                // do nothing
                throw new CompletionTimeException( "Timeout while waiting for creation of completion time writer" );
            }
        }
        catch ( Exception e )
        {
            throw new CompletionTimeException( "Error requesting new completion time writer", e );
        }
    }

    @Override
    synchronized public Future<Long> completionTimeAsMilliFuture() throws CompletionTimeException
    {
        try
        {
            CompletionTimeFuture future = new CompletionTimeFuture( timeSource );
            queueEventSubmitter.submitEventToQueue( CompletionTimeEvent.completionTimeFuture( future ) );
            return future;
        }
        catch ( Exception e )
        {
            throw new CompletionTimeException( "Error requesting CT future", e );
        }
    }

    @Override
    public List<CompletionTimeWriter> getAllWriters() throws CompletionTimeException
    {
        return writers;
    }

    @Override
    // TODO remove from interface
    public long lastKnownLowestInitiatedTimeAsMilli() throws CompletionTimeException
    {
        throw new UnsupportedOperationException( "Method not supported" );
    }

    @Override
    synchronized public void shutdown() throws CompletionTimeException
    {
        if ( sharedIsShuttingDownReference.get() )
        {
            return;
        }
        sharedIsShuttingDownReference.set( true );

        long pollingIntervalAsMilli = 100;
        long shutdownTimeoutTimeAsMilli = timeSource.nowAsMilli() + SHUTDOWN_WAIT_TIMEOUT_AS_MILLI;
        try
        {
            queueEventSubmitter
                    .submitEventToQueue( CompletionTimeEvent.terminateService( sharedWriteEventCountReference.get() ) );
        }
        catch ( InterruptedException e )
        {
            throw new CompletionTimeException( "Encountered error while writing TERMINATE event to queue" );
        }
        while ( timeSource.nowAsMilli() < shutdownTimeoutTimeAsMilli )
        {
            if ( threadedQueuedCompletionTimeServiceThread.shutdownComplete() )
            {
                return;
            }
            if ( errorReporter.errorEncountered() )
            {
                errorReporter.reportError( this, "Error encountered while shutting down" );
                throw new CompletionTimeException( "Error encountered while shutting down" );
            }
            Spinner.powerNap( pollingIntervalAsMilli );
        }
        throw new CompletionTimeException( "Service took too long to shutdown" );
    }

    public static class ThreadedQueuedCompletionTimeWriter implements CompletionTimeWriter
    {
        private final int writerId;
        private final AtomicBoolean sharedIsShuttingDownReference;
        private final AtomicLong sharedWriteEventCountReference;
        private final QueueEventSubmitter<CompletionTimeEvent> queueEventSubmitter;

        ThreadedQueuedCompletionTimeWriter( int writerId,
                AtomicBoolean sharedIsShuttingDownReference,
                AtomicLong sharedWriteEventCountReference,
                QueueEventSubmitter<CompletionTimeEvent> queueEventSubmitter )
        {
            this.writerId = writerId;
            this.sharedIsShuttingDownReference = sharedIsShuttingDownReference;
            this.sharedWriteEventCountReference = sharedWriteEventCountReference;
            this.queueEventSubmitter = queueEventSubmitter;
        }

        @Override
        public void submitInitiatedTime( long timeAsMilli ) throws CompletionTimeException
        {
            if ( sharedIsShuttingDownReference.get() )
            {
                throw new CompletionTimeException( "Can not submit initiated time after calling shutdown" );
            }
            try
            {
                sharedWriteEventCountReference.incrementAndGet();
                queueEventSubmitter.submitEventToQueue(
                        CompletionTimeEvent.writeInitiatedTime( writerId, timeAsMilli ) );
            }
            catch ( Exception e )
            {
                String errMsg = format( "Error submitting initiated time for Time[%s]", timeAsMilli );
                throw new CompletionTimeException( errMsg, e );
            }
        }

        @Override
        public void submitCompletedTime( long timeAsMilli ) throws CompletionTimeException
        {
            try
            {
                sharedWriteEventCountReference.incrementAndGet();
                queueEventSubmitter.submitEventToQueue(
                        CompletionTimeEvent.writeCompletedTime( writerId, timeAsMilli ) );
            }
            catch ( Exception e )
            {
                String errMsg = format( "Error submitting completed time for Time[%s]", timeAsMilli );
                throw new CompletionTimeException( errMsg, e );
            }
        }

        @Override
        public String toString()
        {
            return "ThreadedQueuedCompletionTimeWriter{" + "writerId=" + writerId + '}';
        }
    }

    public static class CompletionTimeFuture implements Future<Long>
    {
        private final TimeSource timeSource;
        private final AtomicBoolean done = new AtomicBoolean( false );
        private final AtomicLong completionTimeReference = new AtomicLong( -1 );

        private CompletionTimeFuture( TimeSource timeSource )
        {
            this.timeSource = timeSource;
        }

        synchronized void set( long timeAsMilli ) throws CompletionTimeException
        {
            if ( done.get() )
            {
                throw new CompletionTimeException( "Value has already been set" );
            }
            this.completionTimeReference.set( timeAsMilli );
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
        public Long get()
        {
            while ( !done.get() )
            {
                // wait for value to be set
                // TODO sleep?
            }
            return completionTimeReference.get();
        }

        @Override
        public Long get( long timeout, TimeUnit unit ) throws TimeoutException
        {
            long timeoutDurationAsMilli = unit.toMillis( timeout );
            long timeoutTimeAsMilli = timeSource.nowAsMilli() + timeoutDurationAsMilli;
            while ( timeSource.nowAsMilli() < timeoutTimeAsMilli )
            {
                // wait for value to be set
                if ( done.get() )
                {
                    return completionTimeReference.get();
                }
                // TODO sleep?
            }
            throw new TimeoutException( "Could not complete future in time" );
        }
    }

    public static class CompletionTimeWriterFuture implements Future<Integer>
    {
        private final TimeSource timeSource;
        private final AtomicBoolean done = new AtomicBoolean( false );
        private final AtomicInteger writerId = new AtomicInteger();

        private CompletionTimeWriterFuture( TimeSource timeSource )
        {
            this.timeSource = timeSource;
        }

        synchronized void set( int value ) throws CompletionTimeException
        {
            if ( done.get() )
            {
                throw new CompletionTimeException( "Value has already been set" );
            }
            this.writerId.set( value );
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
        public Integer get()
        {
            while ( !done.get() )
            {
                // wait for value to be set
                // TODO sleep?
            }
            return writerId.get();
        }

        @Override
        public Integer get( long timeout, TimeUnit unit ) throws TimeoutException
        {
            long timeoutDurationAsMilli = unit.toMillis( timeout );
            long timeoutTimeAsMilli = timeSource.nowAsMilli() + timeoutDurationAsMilli;
            while ( timeSource.nowAsMilli() < timeoutTimeAsMilli )
            {
                // wait for value to be set
                if ( done.get() )
                {
                    return writerId.get();
                }
                // TODO sleep?
            }
            throw new TimeoutException( "Could not complete future in time" );
        }
    }
}
