package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.error.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.Time;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;

public class ThreadedQueuedCompletionTimeMaintenanceThread extends Thread {

    // TODO add to confluence
    /**
     * LocalCompletionTime: Completion Time of local instance, ignoring times received from peers
     * - WRITE locally, RECEIVE from Workers, READ by PeerCommunicator
     * ExternalCompletionTime: Completion Time of peers instances, ignoring times from local workers
     * - RECEIVE from PeerCommunicator, WRITE here, READ here
     * GlobalCompletionTime: minimum of LocalCompletionTime & ExternalCompletionTime
     * - WRITE here, READ by Workers
     * Note:
     * - shared memory READS/WRITES can later be converted req/resp messages between actors
     */

    private final GlobalCompletionTime globalCompletionTime;
    private final AtomicReference<Time> sharedGctReference;
    private final Queue<CompletionTimeEvent> completionTimeQueue;
    private final ConcurrentErrorReporter errorReporter;
    private Long processedEventCount = 0l;
    private Long expectedEventCount = null;

    public ThreadedQueuedCompletionTimeMaintenanceThread(Queue<CompletionTimeEvent> completionTimeQueue,
                                                         ConcurrentErrorReporter errorReporter,
                                                         LocalCompletionTime localCompletionTime,
                                                         ExternalCompletionTime externalCompletionTime,
                                                         AtomicReference<Time> sharedGctReference) {
        this.completionTimeQueue = completionTimeQueue;
        this.errorReporter = errorReporter;
        this.globalCompletionTime = new GlobalCompletionTime(localCompletionTime, externalCompletionTime);
        this.sharedGctReference = sharedGctReference;
        this.sharedGctReference.set(globalCompletionTime.completionTime());
    }

    @Override
    public void run() {
        while (null == expectedEventCount || processedEventCount < expectedEventCount) {
            try {
                CompletionTimeEvent event = null;
                while (event == null) {
                    event = completionTimeQueue.poll();
                }
                switch (event.type()) {
                    case INITIATED:
                        Time initiatedTime = ((CompletionTimeEvent.InitiatedEvent) event).time();
                        globalCompletionTime.localCompletionTime().applyInitiatedTime(initiatedTime);
                        updateGlobalCompletionTime();
                        break;
                    case COMPLETED:
                        Time completedTime = ((CompletionTimeEvent.CompletedEvent) event).time();
                        globalCompletionTime.localCompletionTime().applyCompletedTime(completedTime);
                        updateGlobalCompletionTime();
                        processedEventCount++;
                        break;
                    case EXTERNAL:
                        String peerId = ((CompletionTimeEvent.ExternalEvent) event).peerId();
                        Time peerCompletionTime = ((CompletionTimeEvent.ExternalEvent) event).time();
                        globalCompletionTime.externalCompletionTime().applyPeerCompletionTime(peerId, peerCompletionTime);
                        updateGlobalCompletionTime();
                        break;
                    case FUTURE:
                        ThreadedQueuedConcurrentCompletionTimeService.GlobalCompletionTimeFuture future = ((CompletionTimeEvent.FutureEvent) event).future();
                        future.set(sharedGctReference.get());
                        break;
                    case TERMINATE:
                        if (expectedEventCount == null) {
                            expectedEventCount = ((CompletionTimeEvent.TerminationEvent) event).expectedEventCount();
                        } else {
                            errorReporter.reportError(
                                    this,
                                    String.format("Encountered multiple TERMINATE events. First expectedEventCount[%s]. Second expectedEventCount[%s]",
                                            expectedEventCount, ((CompletionTimeEvent.TerminationEvent) event).expectedEventCount()));
                        }
                        break;
                    default:
                        errorReporter.reportError(
                                this,
                                String.format("Encountered unexpected event type: %s", event.type().name()));
                        return;
                }
            } catch (CompletionTimeException e) {
                errorReporter.reportError(
                        this,
                        String.format("Encountered completion time related error\n%s", ConcurrentErrorReporter.stackTraceToString(e)));
                return;
            } catch (Exception e) {
                errorReporter.reportError(
                        this,
                        String.format("Encountered unexpected exception\n%s", ConcurrentErrorReporter.stackTraceToString(e)));
                return;
            }
        }
    }

    private void updateGlobalCompletionTime() {
        Time newGlobalCompletionTime = globalCompletionTime.completionTime();
        Time prevGlobalCompletionTime = sharedGctReference.get();
        if (null == newGlobalCompletionTime) {
            // TODO add this lesson into specification document on Confluence
            // Either Completion Time has not been received from one or more peers, or no local Completion Time has been receive
            // Until both of the above have occurred there is no way of knowing what the lowest global time is
            return;
        }
        if (null != prevGlobalCompletionTime && newGlobalCompletionTime.lt(prevGlobalCompletionTime)) {
            errorReporter.reportError(
                    this,
                    String.format("New GCT %s smaller than previous GCT %s", newGlobalCompletionTime.toString(), prevGlobalCompletionTime.toString()));
        } else {
            sharedGctReference.set(newGlobalCompletionTime);
        }
    }
}
