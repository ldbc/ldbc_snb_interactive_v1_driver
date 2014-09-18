package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.temporal.Time;

import java.util.ArrayList;
import java.util.List;

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
public class MultiWriterLocalCompletionTimeConcurrentStateManager implements LocalCompletionTimeReader {

    private enum Event {
        READ_LAST_KNOWN_LIT,
        READ_LCT,
        WRITE_LIT,
        WRITE_LCT,
        ADD_WRITER
    }

    private final List<LocalCompletionTimeReaderWriter> localCompletionTimeReaderWriters = new ArrayList<>();
    private Time localCompletionTime = null;
    private Time localInitiationTime = null;

    MultiWriterLocalCompletionTimeConcurrentStateManager() {
    }

    @Override
    public Time lastKnownLowestInitiatedTime() throws CompletionTimeException {
        return (Time) processEvent(Event.READ_LAST_KNOWN_LIT, -1, null);
    }

    @Override
    public Time localCompletionTime() throws CompletionTimeException {
        return (Time) processEvent(Event.READ_LCT, -1, null);
    }

    void submitLocalInitiatedTime(int writerId, Time scheduledStartTime) throws CompletionTimeException {
        processEvent(Event.WRITE_LIT, writerId, scheduledStartTime);
    }

    void submitLocalCompletedTime(int writerId, Time scheduledStartTime) throws CompletionTimeException {
        processEvent(Event.WRITE_LCT, writerId, scheduledStartTime);
    }

    /**
     * IMPORTANT: not safe to call after LIT/LCT times have been submitted, as it will likely put LCT in invalid state
     *
     * @return new writer
     * @throws CompletionTimeException
     */
    LocalCompletionTimeWriter newLocalCompletionTimeWriter() throws CompletionTimeException {
        return (LocalCompletionTimeWriter) processEvent(Event.ADD_WRITER, -1, null);
    }

    synchronized private Object processEvent(Event event, int writerId, Time scheduledStartTime) throws CompletionTimeException {
        switch (event) {
            case READ_LAST_KNOWN_LIT: {
                return localInitiationTime;
            }
            case READ_LCT: {
                return localCompletionTime;
            }
            case WRITE_LIT: {
                LocalCompletionTimeWriter localCompletionTimeWriter = localCompletionTimeReaderWriters.get(writerId);
                if (null == localCompletionTimeWriter)
                    throw new CompletionTimeException(String.format("Writer ID %s does not exist", writerId));
                localCompletionTimeWriter.submitLocalInitiatedTime(scheduledStartTime);
                updateCompletionTime();
                return null;
            }
            case WRITE_LCT: {
                LocalCompletionTimeWriter localCompletionTimeWriter = localCompletionTimeReaderWriters.get(writerId);
                if (null == localCompletionTimeWriter)
                    throw new CompletionTimeException(String.format("Writer ID %s does not exist", writerId));
                localCompletionTimeWriter.submitLocalCompletedTime(scheduledStartTime);
                updateCompletionTime();
                return null;
            }
            case ADD_WRITER: {
                int nextWriterId = localCompletionTimeReaderWriters.size();
                LocalCompletionTimeReaderWriter localCompletionTimeReaderWriter = new LocalCompletionTimeStateManager();
                LocalCompletionTimeWriter localCompletionTimeWriter = new MultiWriterLocalCompletionTimeConcurrentStateManagerWriter(nextWriterId, this);
                localCompletionTimeReaderWriters.add(localCompletionTimeReaderWriter);
                return localCompletionTimeWriter;
            }
            default: {
                throw new CompletionTimeException("This should never happen");
            }
        }
    }

    private void updateCompletionTime() throws CompletionTimeException {
        Time tempLocalInitiationTime = null;
        for (int i = 0; i < localCompletionTimeReaderWriters.size(); i++) {
            LocalCompletionTimeReader reader = localCompletionTimeReaderWriters.get(i);
            Time readerLocalInitiationTime = reader.lastKnownLowestInitiatedTime();
            if (null == readerLocalInitiationTime) {
                // if any initiation times are null, local initiation time and local completion time are undefined
                return;
            } else if (null == tempLocalInitiationTime || readerLocalInitiationTime.lt(tempLocalInitiationTime)) {
                tempLocalInitiationTime = readerLocalInitiationTime;
            } else {
                // reader has initiation time, but it is greater than minimum initiation time
            }
        }

        localInitiationTime = tempLocalInitiationTime;

        Time tempLocalCompletionTime = null;
        for (int i = 0; i < localCompletionTimeReaderWriters.size(); i++) {
            LocalCompletionTimeReader reader = localCompletionTimeReaderWriters.get(i);
            Time readerLocalCompletionTime = reader.localCompletionTime();
            if (null == readerLocalCompletionTime) {
                // reader has non-null initiation time and null completion time
                // if at least one reader has non-null completion time it is still possible that local completion time is non-null
                // initiation time already tells us that no more times will arrive BELOW that time
                // continue checking completion times of other readers
                continue;
            } else if (readerLocalCompletionTime.lt(tempLocalInitiationTime)) {
                if (null == tempLocalCompletionTime || readerLocalCompletionTime.gt(tempLocalCompletionTime)) {
                    tempLocalCompletionTime = readerLocalCompletionTime;
                }
            } else {
                // completion time must be lower than initiation time
                // continue checking completion times of other readers
                continue;
            }
        }

        localCompletionTime = tempLocalCompletionTime;
    }
}
