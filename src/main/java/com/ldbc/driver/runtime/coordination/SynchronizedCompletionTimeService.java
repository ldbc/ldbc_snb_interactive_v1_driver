package com.ldbc.driver.runtime.coordination;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SynchronizedCompletionTimeService implements CompletionTimeService
{
    private final MultiWriterCompletionTimeStateManager completionTimeStateManager;
    private final List<CompletionTimeWriter> completionTimeWriters;

    private enum Event
    {
        READ_COMPLETION_TIME,
        READ_FUTURE_COMPLETION_TIME,
        CREATE_NEW_COMPLETION_TIME_WRITER,
        GET_ALL_WRITERS
    }

    SynchronizedCompletionTimeService() throws CompletionTimeException
    {
        // *** CT Reader ***
        // Completion Time will only get read from MultiWriterCompletionTimeStateManager,
        // and its internal Completion Time values will be written to by multiple instances of
        // MultiWriterCompletionTimeStateManagerWriter, retrieved via newCompletionTimeWriter()
        // *** CT Writer ***
        // it is not safe to write Completion Time directly through CompletionTimeStateManager,
        // because there are, potentially, many Completion Time writers.
        // every Completion Time writing thread must have its own CompletionTimeWriter,
        // to avoid race conditions where one thread tries to submit an Initiated Time,
        // another thread submits a higher Completed Time first, and then Completion Time advances,
        // which will result in an error when the lower Initiated Time is finally submitted.
        // MultiWriterCompletionTimeStateManagerWriter instances, via newCompletionTimeWriter(),
        // will perform the Completion Time writing
        this.completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        this.completionTimeWriters = new ArrayList<>();
    }

    @Override
    public Future<Long> completionTimeAsMilliFuture() throws CompletionTimeException
    {
        return (CompletionTimeAsMilliFuture) processEvent( Event.READ_FUTURE_COMPLETION_TIME );
    }

    @Override
    public List<CompletionTimeWriter> getAllWriters() throws CompletionTimeException
    {
        return (List<CompletionTimeWriter>) processEvent( Event.GET_ALL_WRITERS );
    }

    @Override
    // TODO remove from interface
    public long lastKnownLowestInitiatedTimeAsMilli() throws CompletionTimeException
    {
        throw new UnsupportedOperationException( "Method not supported" );
    }

    @Override
    public long completionTimeAsMilli() throws CompletionTimeException
    {
        return (long) processEvent( Event.READ_COMPLETION_TIME );
    }

    @Override
    public CompletionTimeWriter newCompletionTimeWriter() throws CompletionTimeException
    {
        return (CompletionTimeWriter) processEvent( Event.CREATE_NEW_COMPLETION_TIME_WRITER );
    }

    @Override
    public void shutdown() throws CompletionTimeException
    {
    }

    private Object processEvent( Event event ) throws CompletionTimeException
    {
        synchronized ( completionTimeStateManager )
        {
            switch ( event )
            {
            case READ_COMPLETION_TIME:
            {
                return completionTimeStateManager.completionTimeAsMilli();
            }
            case READ_FUTURE_COMPLETION_TIME:
            {
                return new CompletionTimeAsMilliFuture( completionTimeStateManager.completionTimeAsMilli() );
            }
            case CREATE_NEW_COMPLETION_TIME_WRITER:
            {
                CompletionTimeWriter completionTimeWriter = completionTimeStateManager.newCompletionTimeWriter();
                completionTimeWriters.add( completionTimeWriter );
                return completionTimeWriter;
            }
            case GET_ALL_WRITERS:
            {
                return completionTimeWriters;
            }
            default:
            {
                throw new CompletionTimeException( "Unrecognized event type: " + event.name() );
            }
            }
        }
    }

    private static class CompletionTimeAsMilliFuture implements Future<Long>
    {
        private final long completionTimeValueAsMilli;

        CompletionTimeAsMilliFuture( long completionTimeValueAsMilli )
        {
            this.completionTimeValueAsMilli = completionTimeValueAsMilli;
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
            return true;
        }

        @Override
        public Long get()
        {
            return completionTimeValueAsMilli;
        }

        @Override
        public Long get( long timeout, TimeUnit unit )
        {
            return completionTimeValueAsMilli;
        }
    }
}
