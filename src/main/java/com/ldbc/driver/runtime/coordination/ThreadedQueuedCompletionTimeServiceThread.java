package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.QueueEventFetcher;
import com.ldbc.driver.runtime.coordination.CompletionTimeEvent.CompletedTimeEvent;
import com.ldbc.driver.runtime.coordination.CompletionTimeEvent.CompletionTimeFutureEvent;
import com.ldbc.driver.runtime.coordination.CompletionTimeEvent.InitiatedTimeEvent;
import com.ldbc.driver.runtime.coordination.CompletionTimeEvent.NewCompletionTimeWriterEvent;
import com.ldbc.driver.runtime.coordination.ThreadedQueuedCompletionTimeService.CompletionTimeFuture;
import com.ldbc.driver.runtime.coordination.ThreadedQueuedCompletionTimeService.CompletionTimeWriterFuture;
import com.ldbc.driver.temporal.TemporalUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

public class ThreadedQueuedCompletionTimeServiceThread extends Thread
{

    /**
     * CompletionTime: Completion Time
     * - WRITE here, RECEIVE from Workers
     * CompletionTime: minimum of CompletionTime
     * - WRITE here, READ by Workers
     * Note:
     * - shared memory READS/WRITES can later be converted req/resp messages between actors
     */

    private final TemporalUtil temporalUtil = new TemporalUtil();
    private final MultiWriterCompletionTimeStateManager completionTimeStateManager;
    private final AtomicLong completionTimeSharedReference;
    private final QueueEventFetcher<CompletionTimeEvent> completionTimeEventQueueEventFetcher;
    private final ConcurrentErrorReporter errorReporter;
    private Long processedWriteEventCount = 0L;
    private Long expectedEventCount = null;
    private final Map<Integer,CompletionTimeWriter> completionTimeWriters;
    private final AtomicBoolean shutdownComplete = new AtomicBoolean( false );

    ThreadedQueuedCompletionTimeServiceThread(
            Queue<CompletionTimeEvent> completionTimeQueue,
            ConcurrentErrorReporter errorReporter,
            AtomicLong completionTimeSharedReference ) throws CompletionTimeException
    {
        super( ThreadedQueuedCompletionTimeServiceThread.class.getSimpleName() + "-" +
               System.currentTimeMillis() );
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
        completionTimeStateManager = new MultiWriterCompletionTimeStateManager();
        this.completionTimeWriters = new HashMap<>();
        this.completionTimeEventQueueEventFetcher = QueueEventFetcher.queueEventFetcherFor( completionTimeQueue );
        this.errorReporter = errorReporter;
        this.completionTimeSharedReference = completionTimeSharedReference;
        this.completionTimeSharedReference.set( completionTimeStateManager.completionTimeAsMilli() );
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
                case WRITE_INITIATED_TIME:
                {
                    InitiatedTimeEvent initiatedTimeEvent = (InitiatedTimeEvent) event;
                    long initiatedTimeAsMilli = initiatedTimeEvent.timeAsMilli();
                    int writerId = initiatedTimeEvent.completionTimeWriterId();
                    CompletionTimeWriter writer = completionTimeWriters.get( writerId );
                    writer.submitInitiatedTime( initiatedTimeAsMilli );
                    updateCompletionTime();
                    processedWriteEventCount++;
                    break;
                }
                case WRITE_COMPLETED_TIME:
                {
                    CompletedTimeEvent completedTimeEvent = (CompletedTimeEvent) event;
                    long completedTimeAsMilli = completedTimeEvent.timeAsMilli();
                    int writerId = completedTimeEvent.completionTimeWriterId();
                    CompletionTimeWriter writer = completionTimeWriters.get( writerId );
                    writer.submitCompletedTime( completedTimeAsMilli );
                    updateCompletionTime();
                    processedWriteEventCount++;
                    break;
                }
                case READ_CT_FUTURE:
                {
                    CompletionTimeFuture future = ((CompletionTimeFutureEvent) event).future();
                    future.set( completionTimeSharedReference.get() );
                    break;
                }
                case NEW_COMPLETION_TIME_WRITER:
                {
                    CompletionTimeWriterFuture future = ((NewCompletionTimeWriterEvent) event).future();
                    MultiWriterCompletionTimeStateManagerWriter completionTimeWriter =
                            (MultiWriterCompletionTimeStateManagerWriter) completionTimeStateManager
                                    .newCompletionTimeWriter();
                    completionTimeWriters.put( completionTimeWriter.id(), completionTimeWriter );
                    future.set( completionTimeWriter.id() );
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

    private void updateCompletionTime() throws CompletionTimeException
    {
        long newCompletionTimeAsMilli = completionTimeStateManager.completionTimeAsMilli();
        if ( -1 == newCompletionTimeAsMilli )
        {
            // no Completion Time receive yet --> not yet possible to know what the lowest time is
            return;
        }
        long prevCompletionTimeAsMilli = completionTimeSharedReference.get();
        if ( -1 != prevCompletionTimeAsMilli && newCompletionTimeAsMilli < prevCompletionTimeAsMilli )
        {
            errorReporter.reportError(
                    this,
                    format( "New CT %s / %s smaller than previous CT %s / %s",
                            temporalUtil.milliTimeToDateTimeString( newCompletionTimeAsMilli ),
                            newCompletionTimeAsMilli,
                            temporalUtil.milliTimeToDateTimeString( prevCompletionTimeAsMilli ),
                            prevCompletionTimeAsMilli ) );
        }
        else
        {
            completionTimeSharedReference.set( newCompletionTimeAsMilli );
        }
    }
}
