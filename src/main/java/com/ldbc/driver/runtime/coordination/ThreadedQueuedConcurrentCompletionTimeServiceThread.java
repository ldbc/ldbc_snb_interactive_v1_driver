package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.QueueEventFetcher;
import com.ldbc.driver.temporal.Time;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ThreadedQueuedConcurrentCompletionTimeServiceThread extends Thread {

    /**
     * LocalCompletionTime: Completion Time of local instance, ignoring times received from peers
     * - WRITE here, RECEIVE from Workers, READ by PeerCommunicator
     * ExternalCompletionTime: Completion Time of peers instances, ignoring times from local workers
     * - RECEIVE from PeerCommunicator, WRITE here, READ here
     * GlobalCompletionTime: minimum of LocalCompletionTime & ExternalCompletionTime
     * - WRITE here, READ by Workers
     * Note:
     * - shared memory READS/WRITES can later be converted req/resp messages between actors
     */

    private final GlobalCompletionTimeStateManager globalCompletionTimeStateManager;
    private final MultiWriterLocalCompletionTimeConcurrentStateManager localCompletionTimeConcurrentStateManager;
    private final AtomicReference<Time> globalCompletionTimeSharedReference;
    private final QueueEventFetcher<CompletionTimeEvent> completionTimeEventQueueEventFetcher;
    private final ConcurrentErrorReporter errorReporter;
    private Long processedWriteEventCount = 0l;
    private Long expectedEventCount = null;
    private final Map<Integer, LocalCompletionTimeWriter> localCompletionTimeWriters = new HashMap<>();
    private final AtomicBoolean shutdownComplete = new AtomicBoolean(false);

    ThreadedQueuedConcurrentCompletionTimeServiceThread(Queue<CompletionTimeEvent> completionTimeQueue,
                                                        ConcurrentErrorReporter errorReporter,
                                                        Set<String> peerIds,
                                                        AtomicReference<Time> globalCompletionTimeSharedReference) throws CompletionTimeException {
        super(ThreadedQueuedConcurrentCompletionTimeServiceThread.class.getSimpleName() + "-" + System.currentTimeMillis());
        localCompletionTimeConcurrentStateManager = new MultiWriterLocalCompletionTimeConcurrentStateManager();
        ExternalCompletionTimeStateManager externalCompletionTimeStateManager = new ExternalCompletionTimeStateManager(peerIds);
        ExternalCompletionTimeReader externalCompletionTimeReader =
                (peerIds.isEmpty())
                        // prevents GCT from blocking in the case when there are no peers (because ECT would not advance)
                        ? new LocalCompletionTimeReaderToExternalCompletionTimeReader(localCompletionTimeConcurrentStateManager)
                        : externalCompletionTimeStateManager;
        globalCompletionTimeStateManager = new GlobalCompletionTimeStateManager(
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

        this.completionTimeEventQueueEventFetcher = QueueEventFetcher.queueEventFetcherFor(completionTimeQueue);
        this.errorReporter = errorReporter;
        this.globalCompletionTimeSharedReference = globalCompletionTimeSharedReference;
        this.globalCompletionTimeSharedReference.set(globalCompletionTimeStateManager.globalCompletionTime());
    }

    @Override
    public void run() {
        while (null == expectedEventCount || processedWriteEventCount < expectedEventCount) {
            try {
                CompletionTimeEvent event = completionTimeEventQueueEventFetcher.fetchNextEvent();
                switch (event.type()) {
                    case WRITE_LOCAL_INITIATED_TIME: {
                        CompletionTimeEvent.LocalInitiatedTimeEvent localInitiatedTimeEvent = (CompletionTimeEvent.LocalInitiatedTimeEvent) event;
                        Time initiatedTime = localInitiatedTimeEvent.time();
                        int writerId = localInitiatedTimeEvent.localCompletionTimeWriterId();
                        LocalCompletionTimeWriter writer = localCompletionTimeWriters.get(writerId);
                        writer.submitLocalInitiatedTime(initiatedTime);
                        updateGlobalCompletionTime();
                        processedWriteEventCount++;
                        break;
                    }
                    case WRITE_LOCAL_COMPLETED_TIME: {
                        CompletionTimeEvent.LocalCompletedTimeEvent localCompletedTimeEvent = (CompletionTimeEvent.LocalCompletedTimeEvent) event;
                        Time completedTime = localCompletedTimeEvent.time();
                        int writerId = localCompletedTimeEvent.localCompletionTimeWriterId();
                        LocalCompletionTimeWriter writer = localCompletionTimeWriters.get(writerId);
                        writer.submitLocalCompletedTime(completedTime);
                        updateGlobalCompletionTime();
                        processedWriteEventCount++;
                        break;
                    }
                    case WRITE_EXTERNAL_COMPLETION_TIME: {
                        String peerId = ((CompletionTimeEvent.ExternalCompletionTimeEvent) event).peerId();
                        Time peerCompletionTime = ((CompletionTimeEvent.ExternalCompletionTimeEvent) event).time();
                        globalCompletionTimeStateManager.submitPeerCompletionTime(peerId, peerCompletionTime);
                        updateGlobalCompletionTime();
                        processedWriteEventCount++;
                        break;
                    }
                    case READ_GCT_FUTURE: {
                        ThreadedQueuedConcurrentCompletionTimeService.GlobalCompletionTimeFuture future = ((CompletionTimeEvent.GlobalCompletionTimeFutureEvent) event).future();
                        future.set(globalCompletionTimeSharedReference.get());
                        break;
                    }
                    case NEW_LOCAL_COMPLETION_TIME_WRITER: {
                        ThreadedQueuedConcurrentCompletionTimeService.LocalCompletionTimeWriterFuture future = ((CompletionTimeEvent.NewLocalCompletionTimeWriterEvent) event).future();
                        MultiWriterLocalCompletionTimeConcurrentStateManagerWriter localCompletionTimeWriter =
                                (MultiWriterLocalCompletionTimeConcurrentStateManagerWriter) localCompletionTimeConcurrentStateManager.newLocalCompletionTimeWriter();
                        localCompletionTimeWriters.put(localCompletionTimeWriter.id(), localCompletionTimeWriter);
                        future.set(localCompletionTimeWriter.id());
                        break;
                    }
                    case TERMINATE_SERVICE: {
                        if (null == expectedEventCount) {
                            expectedEventCount = ((CompletionTimeEvent.TerminationServiceEvent) event).expectedEventCount();
                        } else {
                            errorReporter.reportError(
                                    this,
                                    String.format("Encountered multiple %s events. First expectedEventCount[%s]. Second expectedEventCount[%s]",
                                            CompletionTimeEvent.CompletionTimeEventType.TERMINATE_SERVICE.name(),
                                            expectedEventCount,
                                            ((CompletionTimeEvent.TerminationServiceEvent) event).expectedEventCount()));
                        }
                        break;
                    }
                    default: {
                        errorReporter.reportError(
                                this,
                                String.format("Encountered unexpected event type: %s", event.type().name()));
                        return;
                    }
                }
            } catch (CompletionTimeException e) {
                errorReporter.reportError(
                        this,
                        String.format("Encountered completion time related error\n%s", ConcurrentErrorReporter.stackTraceToString(e)));
                return;
            } catch (Throwable e) {
                errorReporter.reportError(
                        this,
                        String.format("Encountered unexpected exception\n%s", ConcurrentErrorReporter.stackTraceToString(e)));
                return;
            }
        }
        shutdownComplete.set(true);
    }

    boolean shutdownComplete() {
        return shutdownComplete.get();
    }

    private void updateGlobalCompletionTime() throws CompletionTimeException {
        Time newGlobalCompletionTime = globalCompletionTimeStateManager.globalCompletionTime();
        Time prevGlobalCompletionTime = globalCompletionTimeSharedReference.get();
        if (null == newGlobalCompletionTime) {
            // Either Completion Time has not been received from one or more peers, or no local Completion Time has been receive
            // Until both of the above have occurred there is no way of knowing what the lowest global time is
            return;
        }
        if (null != prevGlobalCompletionTime && newGlobalCompletionTime.lt(prevGlobalCompletionTime)) {
            errorReporter.reportError(
                    this,
                    String.format("New GCT %s smaller than previous GCT %s", newGlobalCompletionTime.toString(), prevGlobalCompletionTime.toString()));
        } else {
            globalCompletionTimeSharedReference.set(newGlobalCompletionTime);
        }
    }
}
