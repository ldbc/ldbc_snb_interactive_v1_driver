package com.ldbc.driver.runtime.coordination;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

/**
 * Completion time is the point in time AT which there are no uncompleted events.
 * It is not possible that there are uncompleted events AT that time.
 * <p/>
 * Approximately --> Completion Time = min( min(Initiated Events), max(Completed Events) )
 * <p/>
 * But not exactly, as Completion Time is ALWAYS lower than min(Initiated Events).
 * <p/>
 * This class performs the logic of tracking completion time.
 * It can be written to by multiple threads in a thread-safe manner.
 */
public class MultiWriterCompletionTimeStateManager implements CompletionTimeReader
{
    private enum Event
    {
        READ_LAST_KNOWN_IT,
        READ_CT,
        WRITE_IT,
        WRITE_CT,
        ADD_WRITER
    }

    private final List<CompletionTimeReaderWriter> completionTimeReaderWriters = new ArrayList<>();
    private long completionTimeAsMilli = -1;
    private long initiationTimeAsMilli = -1;

    MultiWriterCompletionTimeStateManager()
    {
    }

    @Override
    public long lastKnownLowestInitiatedTimeAsMilli() throws CompletionTimeException
    {
        return (long) processEvent( Event.READ_LAST_KNOWN_IT, -1, -1 );
    }

    @Override
    public long completionTimeAsMilli() throws CompletionTimeException
    {
        return (long) processEvent( Event.READ_CT, -1, -1 );
    }

    void submitInitiatedTime( int writerId, long scheduledStartTimeAsMilli ) throws CompletionTimeException
    {
        processEvent( Event.WRITE_IT, writerId, scheduledStartTimeAsMilli );
    }

    void submitCompletedTime( int writerId, long scheduledStartTimeAsMilli ) throws CompletionTimeException
    {
        processEvent( Event.WRITE_CT, writerId, scheduledStartTimeAsMilli );
    }

    /**
     * IMPORTANT: not safe to call after IT/CT times have been submitted, as it will likely put CT in invalid state
     *
     * @return new writer
     * @throws CompletionTimeException
     */
    CompletionTimeWriter newCompletionTimeWriter() throws CompletionTimeException
    {
        return (CompletionTimeWriter) processEvent( Event.ADD_WRITER, -1, -1 );
    }

    synchronized private Object processEvent( Event event, int writerId, long scheduledStartTimeAsMilli )
            throws CompletionTimeException
    {
        switch ( event )
        {
        case READ_LAST_KNOWN_IT:
        {
            return initiationTimeAsMilli;
        }
        case READ_CT:
        {
            return completionTimeAsMilli;
        }
        case WRITE_IT:
        {
            CompletionTimeWriter completionTimeWriter = completionTimeReaderWriters.get( writerId );
            if ( null == completionTimeWriter )
            { throw new CompletionTimeException( format( "Writer ID %s does not exist", writerId ) ); }
            completionTimeWriter.submitInitiatedTime( scheduledStartTimeAsMilli );
            updateCompletionTime();
            return null;
        }
        case WRITE_CT:
        {
            CompletionTimeWriter completionTimeWriter = completionTimeReaderWriters.get( writerId );
            if ( null == completionTimeWriter )
            { throw new CompletionTimeException( format( "Writer ID %s does not exist", writerId ) ); }
            completionTimeWriter.submitCompletedTime( scheduledStartTimeAsMilli );
            updateCompletionTime();
            return null;
        }
        case ADD_WRITER:
        {
            int nextWriterId = completionTimeReaderWriters.size();
            CompletionTimeReaderWriter completionTimeReaderWriter = new CompletionTimeStateManager();
            CompletionTimeWriter completionTimeWriter =
                    new MultiWriterCompletionTimeStateManagerWriter( nextWriterId, this );
            completionTimeReaderWriters.add( completionTimeReaderWriter );
            return completionTimeWriter;
        }
        default:
        {
            throw new CompletionTimeException( "This should never happen" );
        }
        }
    }

    private void updateCompletionTime() throws CompletionTimeException
    {
        long tempInitiationTimeAsMilli = -1;
        for ( CompletionTimeReader reader : completionTimeReaderWriters )
        {
            long readerInitiationTimeAsMilli = reader.lastKnownLowestInitiatedTimeAsMilli();
            if ( -1 == readerInitiationTimeAsMilli )
            {
                // if any initiation times are null, initiation time and completion time are undefined
                return;
            }
            else if ( -1 == tempInitiationTimeAsMilli ||
                      readerInitiationTimeAsMilli < tempInitiationTimeAsMilli )
            {
                tempInitiationTimeAsMilli = readerInitiationTimeAsMilli;
            }
            else
            {
                // reader has initiation time, but it is greater than minimum initiation time
            }
        }

        initiationTimeAsMilli = tempInitiationTimeAsMilli;

        long tempCompletionTimeAsMilli = completionTimeAsMilli;
        for ( CompletionTimeReader reader : completionTimeReaderWriters )
        {
            long readerCompletionTimeAsMilli = reader.completionTimeAsMilli();
            if ( -1 == readerCompletionTimeAsMilli )
            {
                // reader has non-null initiation time and null completion time
                // if at least one reader has non-null completion time it is still possible that completion
                // time is non-null
                // initiation time already tells us that no more times will arrive BELOW that time
                // continue checking completion times of other readers
            }
            else if ( readerCompletionTimeAsMilli < tempInitiationTimeAsMilli )
            {
                if ( -1 == tempCompletionTimeAsMilli ||
                     readerCompletionTimeAsMilli > tempCompletionTimeAsMilli )
                {
                    tempCompletionTimeAsMilli = readerCompletionTimeAsMilli;
                }
            }
            else
            {
                // completion time must be lower than initiation time
                // continue checking completion times of other readers
            }
        }
        completionTimeAsMilli = tempCompletionTimeAsMilli;
    }
}
