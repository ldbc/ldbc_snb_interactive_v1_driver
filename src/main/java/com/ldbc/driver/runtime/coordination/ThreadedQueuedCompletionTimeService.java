package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.DefaultQueues;
import com.ldbc.driver.runtime.QueueEventSubmitter;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.temporal.TimeSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
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
    private final AtomicLong sharedGctReference;
    private final AtomicLong sharedWriteEventCountReference;
    private final ThreadedQueuedConcurrentCompletionTimeServiceThread
            threadedQueuedConcurrentCompletionTimeServiceThread;
    private final AtomicBoolean sharedIsShuttingDownReference = new AtomicBoolean( false );
    private final ConcurrentErrorReporter errorReporter;
    private final List<LocalCompletionTimeWriter> writers = new ArrayList<>();

    ThreadedQueuedCompletionTimeService( TimeSource timeSource,
            Set<String> peerIds,
            ConcurrentErrorReporter errorReporter ) throws CompletionTimeException
    {
        this.timeSource = timeSource;
        this.errorReporter = errorReporter;
        Queue<CompletionTimeEvent> completionTimeEventQueue = DefaultQueues.newBlockingBounded( 10000 );
        this.queueEventSubmitter = QueueEventSubmitter.queueEventSubmitterFor( completionTimeEventQueue );

        this.sharedGctReference = new AtomicLong( -1 );
        this.sharedWriteEventCountReference = new AtomicLong( 0 );
        threadedQueuedConcurrentCompletionTimeServiceThread = new ThreadedQueuedConcurrentCompletionTimeServiceThread(
                completionTimeEventQueue,
                errorReporter,
                peerIds,
                sharedGctReference );
        threadedQueuedConcurrentCompletionTimeServiceThread.start();
    }

    @Override
    public long globalCompletionTimeAsMilli()
    {
        return sharedGctReference.get();
    }

    @Override
    public LocalCompletionTimeWriter newLocalCompletionTimeWriter() throws CompletionTimeException
    {
        long futureTimeoutDurationAsMilli = TimeUnit.MINUTES.toMillis( 1 );
        try
        {
            LocalCompletionTimeWriterFuture future = new LocalCompletionTimeWriterFuture( timeSource );
            queueEventSubmitter.submitEventToQueue( CompletionTimeEvent.newLocalCompletionTimeWriter( future ) );
            int writerId;
            try
            {
                writerId = future.get( futureTimeoutDurationAsMilli, TimeUnit.MILLISECONDS );
                LocalCompletionTimeWriter writer = new ThreadedQueuedLocalCompletionTimeWriter(
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
            String errMsg = format( "Error requesting new local completion time writer" );
            throw new CompletionTimeException( errMsg, e );
        }
    }

    @Override
    synchronized public Future<Long> globalCompletionTimeAsMilliFuture() throws CompletionTimeException
    {
        try
        {
            GlobalCompletionTimeFuture future = new GlobalCompletionTimeFuture( timeSource );
            queueEventSubmitter.submitEventToQueue( CompletionTimeEvent.globalCompletionTimeFuture( future ) );
            return future;
        }
        catch ( Exception e )
        {
            String errMsg = format( "Error requesting GCT future" );
            throw new CompletionTimeException( errMsg, e );
        }
    }

    @Override
    public List<LocalCompletionTimeWriter> getAllWriters() throws CompletionTimeException
    {
        return writers;
    }

    @Override
    synchronized public void submitPeerCompletionTime( String peerId, long timeAsMilli ) throws CompletionTimeException
    {
        try
        {
            sharedWriteEventCountReference.incrementAndGet();
            queueEventSubmitter
                    .submitEventToQueue( CompletionTimeEvent.writeExternalCompletionTime( peerId, timeAsMilli ) );
        }
        catch ( Exception e )
        {
            String errMsg = format( "Error submitting external completion time for PeerID[%s] Time[%s]", peerId,
                    timeAsMilli );
            throw new CompletionTimeException( errMsg, e );
        }
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
            if ( threadedQueuedConcurrentCompletionTimeServiceThread.shutdownComplete() )
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

    public static class ThreadedQueuedLocalCompletionTimeWriter implements LocalCompletionTimeWriter
    {
        private final int writerId;
        private final AtomicBoolean sharedIsShuttingDownReference;
        private final AtomicLong sharedWriteEventCountReference;
        private final QueueEventSubmitter<CompletionTimeEvent> queueEventSubmitter;

        ThreadedQueuedLocalCompletionTimeWriter( int writerId,
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
        public void submitLocalInitiatedTime( long timeAsMilli ) throws CompletionTimeException
        {
            if ( sharedIsShuttingDownReference.get() )
            {
                throw new CompletionTimeException( "Can not submit initiated time after calling shutdown" );
            }
            try
            {
                sharedWriteEventCountReference.incrementAndGet();
                queueEventSubmitter
                        .submitEventToQueue( CompletionTimeEvent.writeLocalInitiatedTime( writerId, timeAsMilli ) );
            }
            catch ( Exception e )
            {
                String errMsg = format( "Error submitting initiated time for Time[%s]", timeAsMilli );
                throw new CompletionTimeException( errMsg, e );
            }
        }

        @Override
        public void submitLocalCompletedTime( long timeAsMilli ) throws CompletionTimeException
        {
            try
            {
                sharedWriteEventCountReference.incrementAndGet();
                queueEventSubmitter
                        .submitEventToQueue( CompletionTimeEvent.writeLocalCompletedTime( writerId, timeAsMilli ) );
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
            return "ThreadedQueuedLocalCompletionTimeWriter{" +
                   "writerId=" + writerId +
                   '}';
        }
    }

    public static class GlobalCompletionTimeFuture implements Future<Long>
    {
        private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
        private final TimeSource timeSource;
        private final AtomicBoolean done = new AtomicBoolean( false );
        private final AtomicLong globalCompletionTimeReference = new AtomicLong( -1 );

        private GlobalCompletionTimeFuture( TimeSource timeSource )
        {
            this.timeSource = timeSource;
        }

        synchronized void set( long timeAsMilli ) throws CompletionTimeException
        {
            if ( done.get() )
            { throw new CompletionTimeException( "Value has already been set" ); }
            this.globalCompletionTimeReference.set( timeAsMilli );
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
            while ( done.get() == false )
            {
                // wait for value to be set
            }
            return globalCompletionTimeReference.get();
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
                { return globalCompletionTimeReference.get(); }
            }
            throw new TimeoutException( "Could not complete future in time" );
        }
    }

    public static class LocalCompletionTimeWriterFuture implements Future<Integer>
    {
        private final TimeSource timeSource;
        private final AtomicBoolean done = new AtomicBoolean( false );
        private final AtomicInteger writerId = new AtomicInteger();

        private LocalCompletionTimeWriterFuture( TimeSource timeSource )
        {
            this.timeSource = timeSource;
        }

        synchronized void set( int value ) throws CompletionTimeException
        {
            if ( done.get() )
            { throw new CompletionTimeException( "Value has already been set" ); }
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
            while ( done.get() == false )
            {
                // wait for value to be set
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
                { return writerId.get(); }
            }
            throw new TimeoutException( "Could not complete future in time" );
        }
    }
}
