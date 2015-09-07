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
 * This class performs the logic of tracking local completion time.
 * It can be written to by multiple threads in a thread-safe manner.
 */
public class MultiWriterLocalCompletionTimeConcurrentStateManager implements LocalCompletionTimeReader
{

    private enum Event
    {
        READ_LAST_KNOWN_LIT,
        READ_LCT,
        WRITE_LIT,
        WRITE_LCT,
        ADD_WRITER
    }

    private final List<LocalCompletionTimeReaderWriter> localCompletionTimeReaderWriters = new ArrayList<>();
    private long localCompletionTimeAsMilli = -1;
    private long localInitiationTimeAsMilli = -1;

    MultiWriterLocalCompletionTimeConcurrentStateManager()
    {
    }

    @Override
    public long lastKnownLowestInitiatedTimeAsMilli() throws CompletionTimeException
    {
        return (long) processEvent( Event.READ_LAST_KNOWN_LIT, -1, -1 );
    }

    @Override
    public long localCompletionTimeAsMilli() throws CompletionTimeException
    {
        return (long) processEvent( Event.READ_LCT, -1, -1 );
    }

    void submitLocalInitiatedTime( int writerId, long scheduledStartTimeAsMilli ) throws CompletionTimeException
    {
        processEvent( Event.WRITE_LIT, writerId, scheduledStartTimeAsMilli );
    }

    void submitLocalCompletedTime( int writerId, long scheduledStartTimeAsMilli ) throws CompletionTimeException
    {
        processEvent( Event.WRITE_LCT, writerId, scheduledStartTimeAsMilli );
    }

    /**
     * IMPORTANT: not safe to call after LIT/LCT times have been submitted, as it will likely put LCT in invalid state
     *
     * @return new writer
     * @throws CompletionTimeException
     */
    LocalCompletionTimeWriter newLocalCompletionTimeWriter() throws CompletionTimeException
    {
        return (LocalCompletionTimeWriter) processEvent( Event.ADD_WRITER, -1, -1 );
    }

    synchronized private Object processEvent( Event event, int writerId, long scheduledStartTimeAsMilli )
            throws CompletionTimeException
    {
        switch ( event )
        {
        case READ_LAST_KNOWN_LIT:
        {
            return localInitiationTimeAsMilli;
        }
        case READ_LCT:
        {
            return localCompletionTimeAsMilli;
        }
        case WRITE_LIT:
        {
            LocalCompletionTimeWriter localCompletionTimeWriter = localCompletionTimeReaderWriters.get( writerId );
            if ( null == localCompletionTimeWriter )
            { throw new CompletionTimeException( format( "Writer ID %s does not exist", writerId ) ); }
            localCompletionTimeWriter.submitLocalInitiatedTime( scheduledStartTimeAsMilli );
            updateCompletionTime();
            return null;
        }
        case WRITE_LCT:
        {
            LocalCompletionTimeWriter localCompletionTimeWriter = localCompletionTimeReaderWriters.get( writerId );
            if ( null == localCompletionTimeWriter )
            { throw new CompletionTimeException( format( "Writer ID %s does not exist", writerId ) ); }
            localCompletionTimeWriter.submitLocalCompletedTime( scheduledStartTimeAsMilli );
            updateCompletionTime();
            return null;
        }
        case ADD_WRITER:
        {
            int nextWriterId = localCompletionTimeReaderWriters.size();
            LocalCompletionTimeReaderWriter localCompletionTimeReaderWriter = new LocalCompletionTimeStateManager();
            LocalCompletionTimeWriter localCompletionTimeWriter =
                    new MultiWriterLocalCompletionTimeConcurrentStateManagerWriter( nextWriterId, this );
            localCompletionTimeReaderWriters.add( localCompletionTimeReaderWriter );
            return localCompletionTimeWriter;
        }
        default:
        {
            throw new CompletionTimeException( "This should never happen" );
        }
        }
    }

    private void updateCompletionTime() throws CompletionTimeException
    {
        long tempLocalInitiationTimeAsMilli = -1;
        for ( int i = 0; i < localCompletionTimeReaderWriters.size(); i++ )
        {
            LocalCompletionTimeReader reader = localCompletionTimeReaderWriters.get( i );
            long readerLocalInitiationTimeAsMilli = reader.lastKnownLowestInitiatedTimeAsMilli();
            if ( -1 == readerLocalInitiationTimeAsMilli )
            {
                // if any initiation times are null, local initiation time and local completion time are undefined
                return;
            }
            else if ( -1 == tempLocalInitiationTimeAsMilli ||
                      readerLocalInitiationTimeAsMilli < tempLocalInitiationTimeAsMilli )
            {
                tempLocalInitiationTimeAsMilli = readerLocalInitiationTimeAsMilli;
            }
            else
            {
                // reader has initiation time, but it is greater than minimum initiation time
            }
        }

        localInitiationTimeAsMilli = tempLocalInitiationTimeAsMilli;

        long tempLocalCompletionTimeAsMilli = localCompletionTimeAsMilli;
        for ( int i = 0; i < localCompletionTimeReaderWriters.size(); i++ )
        {
            LocalCompletionTimeReader reader = localCompletionTimeReaderWriters.get( i );
            long readerLocalCompletionTimeAsMilli = reader.localCompletionTimeAsMilli();
            if ( -1 == readerLocalCompletionTimeAsMilli )
            {
                // reader has non-null initiation time and null completion time
                // if at least one reader has non-null completion time it is still possible that local completion
                // time is non-null
                // initiation time already tells us that no more times will arrive BELOW that time
                // continue checking completion times of other readers
            }
            else if ( readerLocalCompletionTimeAsMilli < tempLocalInitiationTimeAsMilli )
            {
                if ( -1 == tempLocalCompletionTimeAsMilli ||
                     readerLocalCompletionTimeAsMilli > tempLocalCompletionTimeAsMilli )
                {
                    tempLocalCompletionTimeAsMilli = readerLocalCompletionTimeAsMilli;
                }
            }
            else
            {
                // completion time must be lower than initiation time
                // continue checking completion times of other readers
            }
        }
        localCompletionTimeAsMilli = tempLocalCompletionTimeAsMilli;
    }
}
