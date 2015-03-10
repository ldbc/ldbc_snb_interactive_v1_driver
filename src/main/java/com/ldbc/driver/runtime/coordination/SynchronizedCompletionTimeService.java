package com.ldbc.driver.runtime.coordination;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SynchronizedCompletionTimeService implements CompletionTimeService {
    private final GlobalCompletionTimeStateManager globalCompletionTimeStateManager;
    private final MultiWriterLocalCompletionTimeConcurrentStateManager localCompletionTimeConcurrentStateManager;
    private final List<LocalCompletionTimeWriter> localCompletionTimeWriters;

    private enum Event {
        READ_GLOBAL_COMPLETION_TIME,
        READ_FUTURE_GLOBAL_COMPLETION_TIME,
        WRITE_EXTERNAL_COMPLETION_TIME,
        CREATE_NEW_LOCAL_COMPLETION_TIME_WRITER,
        GET_ALL_WRITERS
    }

    SynchronizedCompletionTimeService(Set<String> peerIds) throws CompletionTimeException {
        this.localCompletionTimeConcurrentStateManager = new MultiWriterLocalCompletionTimeConcurrentStateManager();
        this.localCompletionTimeWriters = new ArrayList<>();
        ExternalCompletionTimeStateManager externalCompletionTimeStateManager = new ExternalCompletionTimeStateManager(peerIds);
        ExternalCompletionTimeReader externalCompletionTimeReader =
                (peerIds.isEmpty())
                        // prevents GCT from blocking in the case when there are no peers (because ECT would not advance)
                        ? new LocalCompletionTimeReaderToExternalCompletionTimeReader(localCompletionTimeConcurrentStateManager)
                        : externalCompletionTimeStateManager;
        this.globalCompletionTimeStateManager = new GlobalCompletionTimeStateManager(
                // *** LCT Reader ***
                // Local Completion Time will only get read from MultiConsumerLocalCompletionTimeConcurrentStateManager,
                // and its internal Local Completion Time values will be written to by multiple instances of
                // MultiConsumerLocalCompletionTimeConcurrentStateManagerConsumer, retrieved via newLocalCompletionTimeWriter()
                localCompletionTimeConcurrentStateManager,
                // *** LCT Writer ***
                // it is not safe to write Local Completion Time directly through GlobalCompletionTimeStateManager,
                // because there are, potentially, many Local Completion Time writers.
                // every Local Completion Time writing thread must have its own LocalCompletionTimeWriter,
                // to avoid race conditions where one thread submits tries to submit an Initiated Time,
                // another thread submits a higher Completed Time first, and then Local Completion Time advances,
                // which will result in an error when the lower Initiated Time is finally submitted.
                // MultiConsumerLocalCompletionTimeConcurrentStateManagerConsumer instances,
                // via newLocalCompletionTimeWriter(), will perform the Local Completion Time writing.
                null,
                // *** ECT Reader ***
                externalCompletionTimeReader,
                // *** ECT Writer ***
                externalCompletionTimeStateManager
        );
    }

    @Override
    public Future<Long> globalCompletionTimeAsMilliFuture() throws CompletionTimeException {
        return (GlobalCompletionTimeAsMilliFuture) processEvent(Event.READ_FUTURE_GLOBAL_COMPLETION_TIME, null, -1);
    }

    @Override
    public List<LocalCompletionTimeWriter> getAllWriters() throws CompletionTimeException {
        return (List<LocalCompletionTimeWriter>) processEvent(Event.GET_ALL_WRITERS, null, -1);
    }

    @Override
    public long globalCompletionTimeAsMilli() throws CompletionTimeException {
        return (long) processEvent(Event.READ_GLOBAL_COMPLETION_TIME, null, -1);
    }

    @Override
    public LocalCompletionTimeWriter newLocalCompletionTimeWriter() throws CompletionTimeException {
        return (LocalCompletionTimeWriter) processEvent(Event.CREATE_NEW_LOCAL_COMPLETION_TIME_WRITER, null, -1);
    }

    @Override
    public void submitPeerCompletionTime(String peerId, long timeAsMilli) throws CompletionTimeException {
        processEvent(Event.WRITE_EXTERNAL_COMPLETION_TIME, peerId, timeAsMilli);
    }

    @Override
    public void shutdown() throws CompletionTimeException {
    }

    private Object processEvent(Event event, String peerId, long timeAsMilli) throws CompletionTimeException {
        synchronized (globalCompletionTimeStateManager) {
            switch (event) {
                case READ_GLOBAL_COMPLETION_TIME: {
                    return globalCompletionTimeStateManager.globalCompletionTimeAsMilli();
                }
                case READ_FUTURE_GLOBAL_COMPLETION_TIME: {
                    return new GlobalCompletionTimeAsMilliFuture(globalCompletionTimeStateManager.globalCompletionTimeAsMilli());
                }
                case WRITE_EXTERNAL_COMPLETION_TIME: {
                    globalCompletionTimeStateManager.submitPeerCompletionTime(peerId, timeAsMilli);
                    return null;
                }
                case CREATE_NEW_LOCAL_COMPLETION_TIME_WRITER: {
                    LocalCompletionTimeWriter localCompletionTimeWriter = localCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();
                    localCompletionTimeWriters.add(localCompletionTimeWriter);
                    return localCompletionTimeWriter;
                }
                case GET_ALL_WRITERS: {
                    return localCompletionTimeWriters;
                }
                default: {
                    throw new CompletionTimeException("Unrecognized event type: " + event.name());
                }
            }
        }
    }

    private static class GlobalCompletionTimeAsMilliFuture implements Future<Long> {
        private final long globalCompletionTimeValueAsMilli;

        public GlobalCompletionTimeAsMilliFuture(long globalCompletionTimeValueAsMilli) {
            this.globalCompletionTimeValueAsMilli = globalCompletionTimeValueAsMilli;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Long get() {
            return globalCompletionTimeValueAsMilli;
        }

        @Override
        public Long get(long timeout, TimeUnit unit) {
            return globalCompletionTimeValueAsMilli;
        }
    }
}
