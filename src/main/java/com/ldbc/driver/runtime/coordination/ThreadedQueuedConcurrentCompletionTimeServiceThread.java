package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.QueueEventFetcher;
import com.ldbc.driver.temporal.TemporalUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

public class ThreadedQueuedConcurrentCompletionTimeServiceThread extends Thread
{

    /**
     * LocalCompletionTime: Completion Time
     * - WRITE here, RECEIVE from Workers
     * GlobalCompletionTime: minimum of LocalCompletionTime
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
    private Long processedWriteEventCount = 0L;
    private Long expectedEventCount = null;
    private final Map<Integer,LocalCompletionTimeWriter> localCompletionTimeWriters;
    private final AtomicBoolean shutdownComplete = new AtomicBoolean( false );

    ThreadedQueuedConcurrentCompletionTimeServiceThread(
            Queue<CompletionTimeEvent> completionTimeQueue,
            ConcurrentErrorReporter errorReporter,
            AtomicLong globalCompletionTimeSharedReference ) throws CompletionTimeException
    {
        super( ThreadedQueuedConcurrentCompletionTimeServiceThread.class.getSimpleName() + "-" +
               System.currentTimeMillis() );
        localCompletionTimeConcurrentStateManager = new MultiWriterLocalCompletionTimeConcurrentStateManager();
        this.localCompletionTimeWriters = new HashMap<>();
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
                null
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
            // no Local Completion Time receive yet --> not yet possible to know what the lowest global time is
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
