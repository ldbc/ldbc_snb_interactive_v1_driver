package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.QueueEventFetcher;
import com.ldbc.driver.temporal.TemporalUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

public class ThreadedQueuedConcurrentCompletionTimeServiceThread extends Thread
{

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

    private final TemporalUtil temporalUtil = new TemporalUtil();
    private final GlobalCompletionTimeStateManager globalCompletionTimeStateManager;
    private final MultiWriterLocalCompletionTimeConcurrentStateManager localCompletionTimeConcurrentStateManager;
    private final AtomicLong globalCompletionTimeSharedReference;
    private final QueueEventFetcher<CompletionTimeEvent> completionTimeEventQueueEventFetcher;
    private final ConcurrentErrorReporter errorReporter;
    private Long processedWriteEventCount = 0l;
    private Long expectedEventCount = null;
    private final Map<Integer,LocalCompletionTimeWriter> localCompletionTimeWriters;
    private final AtomicBoolean shutdownComplete = new AtomicBoolean( false );

    ThreadedQueuedConcurrentCompletionTimeServiceThread( Queue<CompletionTimeEvent> completionTimeQueue,
            ConcurrentErrorReporter errorReporter,
            Set<String> peerIds,
            AtomicLong globalCompletionTimeSharedReference ) throws CompletionTimeException
    {
        super( ThreadedQueuedConcurrentCompletionTimeServiceThread.class.getSimpleName() + "-" +
               System.currentTimeMillis() );
        localCompletionTimeConcurrentStateManager = new MultiWriterLocalCompletionTimeConcurrentStateManager();
        this.localCompletionTimeWriters = new HashMap<>();
        ExternalCompletionTimeStateManager externalCompletionTimeStateManager =
                new ExternalCompletionTimeStateManager( peerIds );
        ExternalCompletionTimeReader externalCompletionTimeReader =
                (peerIds.isEmpty())
                // prevents GCT from blocking in the case when there are no peers (because ECT would not advance)
                ? new LocalCompletionTimeReaderToExternalCompletionTimeReader(
                        localCompletionTimeConcurrentStateManager )
                : externalCompletionTimeStateManager;
        globalCompletionTimeStateManager = new GlobalCompletionTimeStateManager(
                // *** LCT Reader ***
                // Local Completion Time will only get read from MultiConsumerLocalCompletionTimeConcurrentStateManager,
                // and its internal Local Completion Time values will be written to by multiple instances of
                // MultiConsumerLocalCompletionTimeConcurrentStateManagerConsumer, retrieved via
                // newLocalCompletionTimeWriter()
                localCompletionTimeConcurrentStateManager,
                // *** LCT Writer ***
                // it is not safe to write Local Completion Time directly through GlobalCompletionTimeStateManager,
                // because there are, potentially, many Local Completion Time writers.
                // every Local Completion Time writing thread must have its own LocalCompletionTimeWriter,
                // to avoid race conditions where one thread tries to submit an Initiated Time,
                // another thread submits a higher Completed Time first, and then Local Completion Time advances,
                // which will result in an error when the lower Initiated Time is finally submitted.
                // MultiConsumerLocalCompletionTimeConcurrentStateManagerConsumer instances,
                // via newLocalCompletionTimeWriter(), will perform the Local Completion Time writing
                null,
                // *** ECT Reader ***
                externalCompletionTimeReader,
                // *** ECT Writer ***
                externalCompletionTimeStateManager
        );

        this.completionTimeEventQueueEventFetcher = QueueEventFetcher.queueEventFetcherFor( completionTimeQueue );
        this.errorReporter = errorReporter;
        this.globalCompletionTimeSharedReference = globalCompletionTimeSharedReference;
        this.globalCompletionTimeSharedReference.set( globalCompletionTimeStateManager.globalCompletionTimeAsMilli() );
    }

    @Override
    public void run()
    {
        while ( null == expectedEventCount || processedWriteEventCount < expectedEventCount )
        {
            try
            {
                CompletionTimeEvent event = completionTimeEventQueueEventFetcher.fetchNextEvent();
                switch ( event.type() )
                {
                case WRITE_LOCAL_INITIATED_TIME:
                {
                    CompletionTimeEvent.LocalInitiatedTimeEvent localInitiatedTimeEvent =
                            (CompletionTimeEvent.LocalInitiatedTimeEvent) event;
                    long initiatedTimeAsMilli = localInitiatedTimeEvent.timeAsMilli();
                    int writerId = localInitiatedTimeEvent.localCompletionTimeWriterId();
                    LocalCompletionTimeWriter writer = localCompletionTimeWriters.get( writerId );
                    writer.submitLocalInitiatedTime( initiatedTimeAsMilli );
                    updateGlobalCompletionTime();
                    processedWriteEventCount++;
                    break;
                }
                case WRITE_LOCAL_COMPLETED_TIME:
                {
                    CompletionTimeEvent.LocalCompletedTimeEvent localCompletedTimeEvent =
                            (CompletionTimeEvent.LocalCompletedTimeEvent) event;
                    long completedTimeAsMilli = localCompletedTimeEvent.timeAsMilli();
                    int writerId = localCompletedTimeEvent.localCompletionTimeWriterId();
                    LocalCompletionTimeWriter writer = localCompletionTimeWriters.get( writerId );
                    writer.submitLocalCompletedTime( completedTimeAsMilli );
                    updateGlobalCompletionTime();
                    processedWriteEventCount++;
                    break;
                }
                case WRITE_EXTERNAL_COMPLETION_TIME:
                {
                    String peerId = ((CompletionTimeEvent.ExternalCompletionTimeEvent) event).peerId();
                    long peerCompletionTimeAsMilli =
                            ((CompletionTimeEvent.ExternalCompletionTimeEvent) event).timeAsMilli();
                    globalCompletionTimeStateManager.submitPeerCompletionTime( peerId, peerCompletionTimeAsMilli );
                    updateGlobalCompletionTime();
                    processedWriteEventCount++;
                    break;
                }
                case READ_GCT_FUTURE:
                {
                    ThreadedQueuedCompletionTimeService.GlobalCompletionTimeFuture future =
                            ((CompletionTimeEvent.GlobalCompletionTimeFutureEvent) event).future();
                    future.set( globalCompletionTimeSharedReference.get() );
                    break;
                }
                case NEW_LOCAL_COMPLETION_TIME_WRITER:
                {
                    ThreadedQueuedCompletionTimeService.LocalCompletionTimeWriterFuture future =
                            ((CompletionTimeEvent.NewLocalCompletionTimeWriterEvent) event).future();
                    MultiWriterLocalCompletionTimeConcurrentStateManagerWriter localCompletionTimeWriter =
                            (MultiWriterLocalCompletionTimeConcurrentStateManagerWriter)
                                    localCompletionTimeConcurrentStateManager
                                    .newLocalCompletionTimeWriter();
                    localCompletionTimeWriters.put( localCompletionTimeWriter.id(), localCompletionTimeWriter );
                    future.set( localCompletionTimeWriter.id() );
                    break;
                }
                case TERMINATE_SERVICE:
                {
                    if ( null == expectedEventCount )
                    {
                        expectedEventCount = ((CompletionTimeEvent.TerminationServiceEvent) event).expectedEventCount();
                    }
                    else
                    {
                        errorReporter.reportError(
                                this,
                                format( "Encountered multiple %s events. First expectedEventCount[%s]. Second " +
                                        "expectedEventCount[%s]",
                                        CompletionTimeEvent.CompletionTimeEventType.TERMINATE_SERVICE.name(),
                                        expectedEventCount,
                                        ((CompletionTimeEvent.TerminationServiceEvent) event).expectedEventCount() ) );
                    }
                    break;
                }
                default:
                {
                    errorReporter.reportError(
                            this,
                            format( "Encountered unexpected event type: %s", event.type().name() ) );
                    return;
                }
                }
            }
            catch ( CompletionTimeException e )
            {
                errorReporter.reportError(
                        this,
                        format( "Encountered completion time related error\n%s",
                                ConcurrentErrorReporter.stackTraceToString( e ) ) );
                return;
            }
            catch ( Throwable e )
            {
                errorReporter.reportError(
                        this,
                        format( "Encountered unexpected exception\n%s",
                                ConcurrentErrorReporter.stackTraceToString( e ) ) );
                return;
            }
        }
        shutdownComplete.set( true );
    }

    boolean shutdownComplete()
    {
        return shutdownComplete.get();
    }

    private void updateGlobalCompletionTime() throws CompletionTimeException
    {
        long newGlobalCompletionTimeAsMilli = globalCompletionTimeStateManager.globalCompletionTimeAsMilli();
        if ( -1 == newGlobalCompletionTimeAsMilli )
        {
            // Either Completion Time has not been received from one or more peers, or no local Completion Time has
            // been receive
            // Until both of the above have occurred there is no way of knowing what the lowest global time is
            return;
        }
        long prevGlobalCompletionTimeAsMilli = globalCompletionTimeSharedReference.get();
        if ( -1 != prevGlobalCompletionTimeAsMilli && newGlobalCompletionTimeAsMilli < prevGlobalCompletionTimeAsMilli )
        {
            errorReporter.reportError(
                    this,
                    format( "New GCT %s / %s smaller than previous GCT %s / %s",
                            temporalUtil.milliTimeToDateTimeString( newGlobalCompletionTimeAsMilli ),
                            newGlobalCompletionTimeAsMilli,
                            temporalUtil.milliTimeToDateTimeString( prevGlobalCompletionTimeAsMilli ),
                            prevGlobalCompletionTimeAsMilli ) );
        }
        else
        {
            globalCompletionTimeSharedReference.set( newGlobalCompletionTimeAsMilli );
        }
    }
}
